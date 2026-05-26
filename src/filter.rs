use crate::{error::Result, prefix::PrefixExtractor};

const MAX_BLOOM_HASH_COUNT: u8 = 30;
const FNV_OFFSET_A: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_OFFSET_B: u64 = 0x8422_2325_cbf2_9ce4;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterKind {
    PointKey,
    Prefix,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefixFilterDescriptor {
    pub extractor: PrefixExtractor,
    pub partitioned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PointKeyFilter {
    bloom: BloomBits,
}

impl PointKeyFilter {
    #[must_use]
    pub(crate) fn from_keys<'key>(
        keys: impl IntoIterator<Item = &'key [u8]>,
        bits_per_key: u8,
    ) -> Self {
        Self {
            bloom: BloomBits::from_items(keys, bits_per_key),
        }
    }

    pub(crate) fn from_parts(bit_count: u64, hash_count: u8, bytes: Vec<u8>) -> Result<Self> {
        Ok(Self {
            bloom: BloomBits::from_parts(bit_count, hash_count, bytes)?,
        })
    }

    #[must_use]
    pub(crate) const fn bit_count(&self) -> u64 {
        self.bloom.bit_count()
    }

    #[must_use]
    pub(crate) const fn hash_count(&self) -> u8 {
        self.bloom.hash_count()
    }

    #[must_use]
    pub(crate) fn bytes(&self) -> &[u8] {
        self.bloom.bytes()
    }

    #[must_use]
    pub(crate) fn may_contain_key(&self, key: &[u8]) -> bool {
        self.bloom.may_contain(key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrefixFilter {
    extractor: PrefixExtractor,
    bloom: BloomBits,
}

impl PrefixFilter {
    #[must_use]
    pub(crate) fn from_keys<'key>(
        extractor: PrefixExtractor,
        keys: impl IntoIterator<Item = &'key [u8]>,
        bits_per_prefix: u8,
    ) -> Option<Self> {
        if !extractor.supports_prefix_filter() {
            return None;
        }

        let prefixes = keys
            .into_iter()
            .filter_map(|key| extractor.extract(key).map(<[u8]>::to_vec))
            .collect::<Vec<_>>();

        Some(Self {
            extractor,
            bloom: BloomBits::from_items(prefixes.iter().map(Vec::as_slice), bits_per_prefix),
        })
    }

    pub(crate) fn from_parts(
        extractor: PrefixExtractor,
        bit_count: u64,
        hash_count: u8,
        bytes: Vec<u8>,
    ) -> Result<Self> {
        Ok(Self {
            extractor,
            bloom: BloomBits::from_parts(bit_count, hash_count, bytes)?,
        })
    }

    #[must_use]
    pub(crate) const fn extractor(&self) -> &PrefixExtractor {
        &self.extractor
    }

    #[must_use]
    pub(crate) const fn bit_count(&self) -> u64 {
        self.bloom.bit_count()
    }

    #[must_use]
    pub(crate) const fn hash_count(&self) -> u8 {
        self.bloom.hash_count()
    }

    #[must_use]
    pub(crate) fn bytes(&self) -> &[u8] {
        self.bloom.bytes()
    }

    #[must_use]
    pub(crate) fn may_contain_prefix(&self, prefix: &[u8]) -> bool {
        self.bloom.may_contain(prefix)
    }

    #[must_use]
    pub(crate) fn may_contain_query_prefix(
        &self,
        query_prefix: &[u8],
        query_extractor: &PrefixExtractor,
    ) -> bool {
        if query_extractor != &self.extractor {
            return true;
        }

        query_extractor
            .query_filter_prefix(query_prefix)
            .is_none_or(|prefix| self.bloom.may_contain(prefix))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BloomBits {
    bit_count: u64,
    hash_count: u8,
    bytes: Vec<u8>,
}

impl BloomBits {
    fn from_items<'item>(items: impl IntoIterator<Item = &'item [u8]>, bits_per_item: u8) -> Self {
        let items = items.into_iter().collect::<Vec<_>>();
        if items.is_empty() {
            return Self::empty();
        }

        let bit_count = usize_to_u64_saturating(items.len())
            .saturating_mul(u64::from(bits_per_item))
            .max(1);
        let hash_count = bloom_hash_count(bits_per_item);
        let byte_len = bloom_byte_len_saturating(bit_count);
        let mut bloom = Self {
            bit_count,
            hash_count,
            bytes: vec![0; byte_len],
        };

        for item in items {
            bloom.insert(item);
        }

        bloom
    }

    fn from_parts(bit_count: u64, hash_count: u8, bytes: Vec<u8>) -> Result<Self> {
        let expected_len = bloom_byte_len(bit_count)?;
        if bytes.len() != expected_len {
            return Err(crate::Error::InvalidFormat {
                message: "bloom filter byte length does not match bit count".to_owned(),
            });
        }
        if bit_count == 0 {
            if hash_count != 0 || !bytes.is_empty() {
                return Err(crate::Error::InvalidFormat {
                    message: "empty bloom filter has non-empty metadata".to_owned(),
                });
            }
        } else if hash_count == 0 || hash_count > MAX_BLOOM_HASH_COUNT {
            return Err(crate::Error::InvalidFormat {
                message: "invalid bloom filter hash count".to_owned(),
            });
        }

        Ok(Self {
            bit_count,
            hash_count,
            bytes,
        })
    }

    const fn empty() -> Self {
        Self {
            bit_count: 0,
            hash_count: 0,
            bytes: Vec::new(),
        }
    }

    const fn bit_count(&self) -> u64 {
        self.bit_count
    }

    const fn hash_count(&self) -> u8 {
        self.hash_count
    }

    fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn insert(&mut self, item: &[u8]) {
        if self.bit_count == 0 {
            return;
        }

        bloom_visit_indexes(item, self.bit_count, self.hash_count, |bit_index| {
            let byte_index =
                usize::try_from(bit_index / 8).expect("bloom bit index fits byte slice");
            let bit_offset = u32::try_from(bit_index % 8).expect("bloom bit offset fits u32");
            self.bytes[byte_index] |= 1_u8 << bit_offset;
        });
    }

    fn may_contain(&self, item: &[u8]) -> bool {
        if self.bit_count == 0 {
            return false;
        }

        let mut contains = true;
        bloom_visit_indexes(item, self.bit_count, self.hash_count, |bit_index| {
            let byte_index =
                usize::try_from(bit_index / 8).expect("bloom bit index fits byte slice");
            let bit_offset = u32::try_from(bit_index % 8).expect("bloom bit offset fits u32");
            contains &= (self.bytes[byte_index] & (1_u8 << bit_offset)) != 0;
        });
        contains
    }
}

fn bloom_hash_count(bits_per_item: u8) -> u8 {
    let hash_count = u16::from(bits_per_item).saturating_mul(69) / 100;
    u8::try_from(hash_count)
        .unwrap_or(MAX_BLOOM_HASH_COUNT)
        .clamp(1, MAX_BLOOM_HASH_COUNT)
}

fn bloom_byte_len(bit_count: u64) -> Result<usize> {
    let byte_len = bit_count
        .saturating_add(7)
        .checked_div(8)
        .expect("division by non-zero");
    usize::try_from(byte_len).map_err(|_| crate::Error::InvalidFormat {
        message: "bloom filter bit count exceeds this platform".to_owned(),
    })
}

fn bloom_byte_len_saturating(bit_count: u64) -> usize {
    bloom_byte_len(bit_count).unwrap_or(usize::MAX)
}

fn bloom_visit_indexes(item: &[u8], bit_count: u64, hash_count: u8, mut visit: impl FnMut(u64)) {
    let hash_a = bloom_hash(FNV_OFFSET_A, item);
    let hash_b = bloom_hash(FNV_OFFSET_B, item) | 1;

    for index in 0..hash_count {
        let bit_index = hash_a.wrapping_add(u64::from(index).wrapping_mul(hash_b)) % bit_count;
        visit(bit_index);
    }
}

fn bloom_hash(seed: u64, item: &[u8]) -> u64 {
    let mut hash = seed;
    for byte in item {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    match u64::try_from(value) {
        Ok(value) => value,
        Err(_) => u64::MAX,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_filter_bit_count_tracks_bits_per_key() {
        let keys = (0..128)
            .map(|index| format!("key-{index:03}").into_bytes())
            .collect::<Vec<_>>();
        let small = PointKeyFilter::from_keys(keys.iter().map(Vec::as_slice), 4);
        let large = PointKeyFilter::from_keys(keys.iter().map(Vec::as_slice), 12);

        assert_eq!(small.bit_count(), 512);
        assert_eq!(large.bit_count(), 1536);
        assert!(large.bytes().len() > small.bytes().len());
        assert!(small.hash_count() < large.hash_count());
    }

    #[test]
    fn point_filter_round_trips_from_parts() {
        let keys = [b"alpha".as_slice(), b"beta".as_slice(), b"gamma".as_slice()];
        let filter = PointKeyFilter::from_keys(keys, 10);
        let decoded = PointKeyFilter::from_parts(
            filter.bit_count(),
            filter.hash_count(),
            filter.bytes().to_vec(),
        )
        .expect("filter parts decode");

        for key in keys {
            assert!(decoded.may_contain_key(key));
        }
    }

    #[test]
    fn prefix_filter_uses_extractor_prefixes() {
        let keys = [
            b"user:001".as_slice(),
            b"user:002".as_slice(),
            b"post:001".as_slice(),
        ];
        let extractor = PrefixExtractor::Separator(b':');
        let filter =
            PrefixFilter::from_keys(extractor.clone(), keys, 10).expect("prefix filter builds");

        assert!(filter.may_contain_query_prefix(b"user:", &extractor));
        assert!(filter.may_contain_query_prefix(b"post:", &extractor));
    }

    #[test]
    fn malformed_bloom_parts_fail_closed() {
        let error = PointKeyFilter::from_parts(16, 1, vec![0]).expect_err("short bitset fails");
        assert!(error.to_string().contains("byte length"));

        let error =
            PointKeyFilter::from_parts(16, 0, vec![0, 0]).expect_err("zero hash count fails");
        assert!(error.to_string().contains("hash count"));
    }
}
