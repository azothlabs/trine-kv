use std::path::{Path, PathBuf};

use trine_kv::{Bucket, Db, Error, Result, WriteBatch, WriteOptions};

fn main() -> Result<()> {
    let path = temp_path("trine-kv-event-index");
    reset_dir(&path)?;

    let log = EventLog::open(&path)?;
    log.append(&Event::new("000001", "acct-a", "invoice-created"))?;
    log.append(&Event::new("000002", "acct-b", "invoice-created"))?;
    log.append(&Event::new("000003", "acct-a", "invoice-paid"))?;

    assert_eq!(
        log.events_for_account("acct-a")?
            .iter()
            .map(|event| event.body.as_str())
            .collect::<Vec<_>>(),
        ["invoice-created", "invoice-paid"]
    );

    log.flush()?;
    drop(log);

    let reopened = EventLog::open(&path)?;
    assert_eq!(
        reopened
            .events_for_account("acct-b")?
            .first()
            .map(|event| event.id.as_str()),
        Some("000002")
    );

    drop(reopened);
    std::fs::remove_dir_all(path)?;
    Ok(())
}

struct EventLog {
    db: Db,
    events: Bucket,
    by_account: Bucket,
}

impl EventLog {
    fn open(path: &Path) -> Result<Self> {
        let db = Db::open_persistent_sync(path)?;
        let events = db.bucket_sync("events")?;
        let by_account = db.bucket_sync("events_by_account")?;
        Ok(Self {
            db,
            events,
            by_account,
        })
    }

    fn append(&self, event: &Event) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put_bucket("events", event_key(&event.id), event.encode()?)?;
        batch.put_bucket(
            "events_by_account",
            account_event_key(&event.account_id, &event.id),
            event.id.as_bytes(),
        )?;
        self.db.write_sync(batch, WriteOptions::default())?;
        Ok(())
    }

    fn events_for_account(&self, account_id: &str) -> Result<Vec<Event>> {
        self.by_account
            .prefix_sync(account_event_prefix(account_id))?
            .map(|item| {
                let index = item?;
                let event_id = std::str::from_utf8(&index.value)
                    .map_err(|_| invalid_event("index value is not UTF-8"))?;
                let bytes = self.events.get_sync(&event_key(event_id))?.ok_or_else(|| {
                    Error::Corruption {
                        message: format!("event index points at missing event {event_id}"),
                    }
                })?;
                Event::decode(&bytes)
            })
            .collect()
    }

    fn flush(&self) -> Result<()> {
        self.db.flush_sync()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Event {
    id: String,
    account_id: String,
    body: String,
}

impl Event {
    fn new(id: &str, account_id: &str, body: &str) -> Self {
        Self {
            id: id.to_owned(),
            account_id: account_id.to_owned(),
            body: body.to_owned(),
        }
    }

    fn encode(&self) -> Result<Vec<u8>> {
        encode_fields(&[&self.id, &self.account_id, &self.body])
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut fields = FieldCursor::new(bytes);
        let event = Self {
            id: fields.read_string()?,
            account_id: fields.read_string()?,
            body: fields.read_string()?,
        };
        fields.finish()?;
        Ok(event)
    }
}

fn event_key(id: &str) -> Vec<u8> {
    format!("event/{id}").into_bytes()
}

fn account_event_key(account_id: &str, event_id: &str) -> Vec<u8> {
    format!("account/{account_id}/event/{event_id}").into_bytes()
}

fn account_event_prefix(account_id: &str) -> Vec<u8> {
    format!("account/{account_id}/event/").into_bytes()
}

fn encode_fields(fields: &[&str]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for field in fields {
        let len = u32::try_from(field.len())
            .map_err(|_| Error::invalid_options("event field exceeds u32::MAX"))?;
        bytes.extend_from_slice(&len.to_le_bytes());
        bytes.extend_from_slice(field.as_bytes());
    }
    Ok(bytes)
}

struct FieldCursor<'bytes> {
    bytes: &'bytes [u8],
    offset: usize,
}

impl<'bytes> FieldCursor<'bytes> {
    const fn new(bytes: &'bytes [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_string(&mut self) -> Result<String> {
        let len_bytes = self
            .bytes
            .get(self.offset..self.offset.saturating_add(4))
            .ok_or_else(|| invalid_event("short field length"))?;
        self.offset += 4;

        let len =
            u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| invalid_event("field length overflows usize"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| invalid_event("short field bytes"))?;
        self.offset = end;

        std::str::from_utf8(value)
            .map(str::to_owned)
            .map_err(|_| invalid_event("field is not UTF-8"))
    }

    fn finish(&self) -> Result<()> {
        if self.offset == self.bytes.len() {
            return Ok(());
        }
        Err(invalid_event("trailing bytes"))
    }
}

fn invalid_event(message: &'static str) -> Error {
    Error::InvalidFormat {
        message: format!("invalid event record: {message}"),
    }
}

fn temp_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{name}-{}", std::process::id()))
}

fn reset_dir(path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(Error::Io(error)),
    }
    Ok(())
}
