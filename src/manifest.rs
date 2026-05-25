use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use crate::{
    error::{Error, Result},
    options::{
        CompressionProfile, FilterPolicy, IndexSearchPolicy, KeyspaceOptions, PrefixFilterPolicy,
    },
    prefix::PrefixExtractor,
    table::TableId,
    types::Sequence,
};

pub const MANIFEST_FILE_NAME: &str = "MANIFEST";
const MANIFEST_MAGIC: u32 = 0x5452_4d46;
const MANIFEST_VERSION: u16 = 1;
const HEADER_LEN: usize = 14;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestEdit {
    CreateKeyspace {
        name: String,
        options: KeyspaceOptions,
    },
    UpdateKeyspaceOptions {
        name: String,
        options: KeyspaceOptions,
    },
    AddTable {
        keyspace: String,
        table_id: TableId,
    },
    RemoveTable {
        keyspace: String,
        table_id: TableId,
    },
    UpdateWalReplayFloor {
        sequence: Sequence,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestState {
    wal_replay_floor: Sequence,
    keyspaces: BTreeMap<String, KeyspaceOptions>,
}

impl ManifestState {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            wal_replay_floor: Sequence::ZERO,
            keyspaces: BTreeMap::new(),
        }
    }

    #[must_use]
    pub const fn wal_replay_floor(&self) -> Sequence {
        self.wal_replay_floor
    }

    #[must_use]
    pub fn keyspaces(&self) -> &BTreeMap<String, KeyspaceOptions> {
        &self.keyspaces
    }
}

impl Default for ManifestState {
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Debug)]
pub struct ManifestStore {
    path: PathBuf,
    state: ManifestState,
}

impl ManifestStore {
    pub fn open_or_create(path: impl Into<PathBuf>, create_if_missing: bool) -> Result<Self> {
        let path = path.into();
        let state = if path.exists() {
            read_manifest(&path)?
        } else if create_if_missing {
            let state = ManifestState::empty();
            publish_manifest(&path, &state)?;
            state
        } else {
            ManifestState::empty()
        };

        Ok(Self { path, state })
    }

    #[must_use]
    pub const fn state(&self) -> &ManifestState {
        &self.state
    }

    pub fn create_keyspace(&mut self, name: String, options: KeyspaceOptions) -> Result<()> {
        if let Some(existing) = self.state.keyspaces.get(&name) {
            if existing == &options {
                return Ok(());
            }
            return Err(Error::invalid_options(
                "existing keyspace options do not match requested options",
            ));
        }

        self.state.keyspaces.insert(name, options);
        publish_manifest(&self.path, &self.state)
    }

    pub fn update_wal_replay_floor(&mut self, sequence: Sequence) -> Result<()> {
        self.state.wal_replay_floor = sequence;
        publish_manifest(&self.path, &self.state)
    }
}

#[must_use]
pub fn manifest_path(db_path: &Path) -> PathBuf {
    db_path.join(MANIFEST_FILE_NAME)
}

pub fn read_manifest(path: &Path) -> Result<ManifestState> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    decode_manifest(&bytes)
}

fn publish_manifest(path: &Path, state: &ManifestState) -> Result<()> {
    let payload = encode_state(state)?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| Error::invalid_options("manifest payload exceeds u32::MAX"))?;
    let payload_checksum = checksum(&payload);
    let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());

    bytes.extend_from_slice(&MANIFEST_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&payload_checksum.to_le_bytes());
    bytes.extend_from_slice(&payload);

    let tmp_path = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    fs::rename(tmp_path, path)?;

    Ok(())
}

fn encode_state(state: &ManifestState) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let keyspace_count = u32::try_from(state.keyspaces.len())
        .map_err(|_| Error::invalid_options("too many keyspaces for manifest"))?;

    put_u64(&mut bytes, state.wal_replay_floor.get());
    put_u32(&mut bytes, keyspace_count);
    for (name, options) in &state.keyspaces {
        put_bytes(&mut bytes, name.as_bytes())?;
        put_keyspace_options(&mut bytes, options)?;
    }

    Ok(bytes)
}

fn decode_manifest(bytes: &[u8]) -> Result<ManifestState> {
    if bytes.len() < HEADER_LEN {
        return Err(invalid_manifest("short header"));
    }

    let magic = read_u32_at(bytes, 0)?;
    let version = read_u16_at(bytes, 4)?;
    let payload_len = read_u32_at(bytes, 6)? as usize;
    let payload_checksum = read_u32_at(bytes, 10)?;
    if magic != MANIFEST_MAGIC {
        return Err(Error::Corruption {
            message: "manifest magic mismatch".to_owned(),
        });
    }
    if version != MANIFEST_VERSION {
        return Err(Error::UnsupportedFormat {
            message: format!("unsupported manifest version {version}"),
        });
    }
    if bytes.len() != HEADER_LEN + payload_len {
        return Err(Error::Corruption {
            message: "manifest length mismatch".to_owned(),
        });
    }

    let payload = &bytes[HEADER_LEN..];
    if checksum(payload) != payload_checksum {
        return Err(Error::Corruption {
            message: "manifest checksum mismatch".to_owned(),
        });
    }

    decode_state(payload)
}

fn decode_state(payload: &[u8]) -> Result<ManifestState> {
    let mut cursor = Cursor::new(payload);
    let wal_replay_floor = Sequence::new(cursor.read_u64()?);
    let keyspace_count = cursor.read_u32()? as usize;
    let mut keyspaces = BTreeMap::new();

    for _ in 0..keyspace_count {
        let name =
            String::from_utf8(cursor.read_bytes()?.to_vec()).map_err(|_| Error::InvalidFormat {
                message: "manifest keyspace name is not valid UTF-8".to_owned(),
            })?;
        let options = cursor.read_keyspace_options()?;
        keyspaces.insert(name, options);
    }

    if !cursor.is_finished() {
        return Err(invalid_manifest("trailing payload bytes"));
    }

    Ok(ManifestState {
        wal_replay_floor,
        keyspaces,
    })
}

fn put_keyspace_options(bytes: &mut Vec<u8>, options: &KeyspaceOptions) -> Result<()> {
    put_bool(bytes, options.allow_empty_keys);
    put_compression_profile(bytes, options.compression);
    put_usize(bytes, options.block_bytes)?;
    put_filter_policy(bytes, options.filter_policy);
    put_prefix_extractor(bytes, &options.prefix_extractor)?;
    put_prefix_filter_policy(bytes, options.prefix_filter_policy);
    put_index_search_policy(bytes, options.index_search_policy);
    put_usize(bytes, options.blob_threshold_bytes)?;
    Ok(())
}

fn put_bool(bytes: &mut Vec<u8>, value: bool) {
    put_u8(bytes, u8::from(value));
}

fn put_compression_profile(bytes: &mut Vec<u8>, value: CompressionProfile) {
    put_u8(
        bytes,
        match value {
            CompressionProfile::None => 0,
            CompressionProfile::Fast => 1,
            CompressionProfile::Compact => 2,
        },
    );
}

fn put_filter_policy(bytes: &mut Vec<u8>, value: FilterPolicy) {
    match value {
        FilterPolicy::Disabled => put_u8(bytes, 0),
        FilterPolicy::Bloom { bits_per_key } => {
            put_u8(bytes, 1);
            put_u8(bytes, bits_per_key);
        }
    }
}

fn put_prefix_extractor(bytes: &mut Vec<u8>, value: &PrefixExtractor) -> Result<()> {
    match value {
        PrefixExtractor::FixedLen(len) => {
            put_u8(bytes, 0);
            put_usize(bytes, *len)?;
        }
        PrefixExtractor::Separator(separator) => {
            put_u8(bytes, 1);
            put_u8(bytes, *separator);
        }
        PrefixExtractor::Custom(name) => {
            put_u8(bytes, 2);
            put_bytes(bytes, name.as_bytes())?;
        }
        PrefixExtractor::Disabled => put_u8(bytes, 3),
    }
    Ok(())
}

fn put_prefix_filter_policy(bytes: &mut Vec<u8>, value: PrefixFilterPolicy) {
    match value {
        PrefixFilterPolicy::Disabled => put_u8(bytes, 0),
        PrefixFilterPolicy::Bloom { bits_per_prefix } => {
            put_u8(bytes, 1);
            put_u8(bytes, bits_per_prefix);
        }
    }
}

fn put_index_search_policy(bytes: &mut Vec<u8>, value: IndexSearchPolicy) {
    put_u8(
        bytes,
        match value {
            IndexSearchPolicy::Linear => 0,
            IndexSearchPolicy::Binary => 1,
            IndexSearchPolicy::Eytzinger => 2,
            IndexSearchPolicy::GallopingWithHint => 3,
            IndexSearchPolicy::Auto => 4,
        },
    );
}

fn put_usize(bytes: &mut Vec<u8>, value: usize) -> Result<()> {
    let value = u64::try_from(value)
        .map_err(|_| Error::invalid_options("manifest usize field exceeds u64::MAX"))?;
    put_u64(bytes, value);
    Ok(())
}

fn put_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| Error::invalid_options("manifest byte field exceeds u32::MAX"))?;
    put_u32(bytes, len);
    bytes.extend_from_slice(value);
    Ok(())
}

fn read_u16_at(bytes: &[u8], offset: usize) -> Result<u16> {
    let value = bytes
        .get(offset..offset + 2)
        .ok_or_else(|| invalid_manifest("short u16"))?;
    Ok(u16::from_le_bytes([value[0], value[1]]))
}

fn read_u32_at(bytes: &[u8], offset: usize) -> Result<u32> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| invalid_manifest("short u32"))?;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

fn checksum(bytes: &[u8]) -> u32 {
    let mut hash = 0x811c_9dc5_u32;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

fn invalid_manifest(message: &'static str) -> Error {
    Error::InvalidFormat {
        message: format!("invalid manifest: {message}"),
    }
}

struct Cursor<'payload> {
    payload: &'payload [u8],
    offset: usize,
}

impl<'payload> Cursor<'payload> {
    const fn new(payload: &'payload [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        let value = *self
            .payload
            .get(self.offset)
            .ok_or_else(|| invalid_manifest("short u8"))?;
        self.offset += 1;
        Ok(value)
    }

    fn read_bool(&mut self) -> Result<bool> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(Error::InvalidFormat {
                message: format!("invalid manifest bool {value}"),
            }),
        }
    }

    fn read_u32(&mut self) -> Result<u32> {
        let value = read_u32_at(self.payload, self.offset)?;
        self.offset += 4;
        Ok(value)
    }

    fn read_u64(&mut self) -> Result<u64> {
        let value = self
            .payload
            .get(self.offset..self.offset + 8)
            .ok_or_else(|| invalid_manifest("short u64"))?;
        self.offset += 8;
        Ok(u64::from_le_bytes([
            value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
        ]))
    }

    fn read_usize(&mut self) -> Result<usize> {
        usize::try_from(self.read_u64()?).map_err(|_| Error::UnsupportedFormat {
            message: "manifest usize field does not fit this platform".to_owned(),
        })
    }

    fn read_bytes(&mut self) -> Result<&'payload [u8]> {
        let len = self.read_u32()? as usize;
        let value = self
            .payload
            .get(self.offset..self.offset + len)
            .ok_or_else(|| invalid_manifest("short bytes"))?;
        self.offset += len;
        Ok(value)
    }

    fn read_keyspace_options(&mut self) -> Result<KeyspaceOptions> {
        Ok(KeyspaceOptions {
            allow_empty_keys: self.read_bool()?,
            compression: self.read_compression_profile()?,
            block_bytes: self.read_usize()?,
            filter_policy: self.read_filter_policy()?,
            prefix_extractor: self.read_prefix_extractor()?,
            prefix_filter_policy: self.read_prefix_filter_policy()?,
            index_search_policy: self.read_index_search_policy()?,
            blob_threshold_bytes: self.read_usize()?,
        })
    }

    fn read_compression_profile(&mut self) -> Result<CompressionProfile> {
        match self.read_u8()? {
            0 => Ok(CompressionProfile::None),
            1 => Ok(CompressionProfile::Fast),
            2 => Ok(CompressionProfile::Compact),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown manifest compression profile {tag}"),
            }),
        }
    }

    fn read_filter_policy(&mut self) -> Result<FilterPolicy> {
        match self.read_u8()? {
            0 => Ok(FilterPolicy::Disabled),
            1 => Ok(FilterPolicy::Bloom {
                bits_per_key: self.read_u8()?,
            }),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown manifest filter policy {tag}"),
            }),
        }
    }

    fn read_prefix_extractor(&mut self) -> Result<PrefixExtractor> {
        match self.read_u8()? {
            0 => Ok(PrefixExtractor::FixedLen(self.read_usize()?)),
            1 => Ok(PrefixExtractor::Separator(self.read_u8()?)),
            2 => {
                let name = String::from_utf8(self.read_bytes()?.to_vec()).map_err(|_| {
                    Error::InvalidFormat {
                        message: "manifest custom prefix extractor is not UTF-8".to_owned(),
                    }
                })?;
                Ok(PrefixExtractor::Custom(name))
            }
            3 => Ok(PrefixExtractor::Disabled),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown manifest prefix extractor {tag}"),
            }),
        }
    }

    fn read_prefix_filter_policy(&mut self) -> Result<PrefixFilterPolicy> {
        match self.read_u8()? {
            0 => Ok(PrefixFilterPolicy::Disabled),
            1 => Ok(PrefixFilterPolicy::Bloom {
                bits_per_prefix: self.read_u8()?,
            }),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown manifest prefix filter policy {tag}"),
            }),
        }
    }

    fn read_index_search_policy(&mut self) -> Result<IndexSearchPolicy> {
        match self.read_u8()? {
            0 => Ok(IndexSearchPolicy::Linear),
            1 => Ok(IndexSearchPolicy::Binary),
            2 => Ok(IndexSearchPolicy::Eytzinger),
            3 => Ok(IndexSearchPolicy::GallopingWithHint),
            4 => Ok(IndexSearchPolicy::Auto),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown manifest index search policy {tag}"),
            }),
        }
    }

    const fn is_finished(&self) -> bool {
        self.offset == self.payload.len()
    }
}
