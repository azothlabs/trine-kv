use std::path::{Path, PathBuf};

use trine_kv::{
    Bucket, BucketOptions, Db, DbOptions, DurabilityMode, Error, PrefixExtractor, Result,
    TransactionOptions,
};

fn main() -> Result<()> {
    let path = temp_path("trine-kv-user-store");
    reset_dir(&path)?;

    let store = UserStore::open(&path)?;
    store.put_user(&User::new("001", "Ada", "ada@example.test"))?;
    store.put_user(&User::new("002", "Lin", "lin@example.test"))?;

    let users = store.list_users()?;
    assert_eq!(
        users
            .iter()
            .map(|user| user.display_name.as_str())
            .collect::<Vec<_>>(),
        ["Ada", "Lin"]
    );

    assert!(store.rename_if_email_matches("001", "ada@example.test", "Ada Lovelace")?);
    assert!(!store.rename_if_email_matches("002", "other@example.test", "Someone Else")?);
    store.flush()?;
    drop(store);

    let reopened = UserStore::open(&path)?;
    assert_eq!(
        reopened.get_user("001")?,
        Some(User::new("001", "Ada Lovelace", "ada@example.test"))
    );

    drop(reopened);
    std::fs::remove_dir_all(path)?;
    Ok(())
}

struct UserStore {
    db: Db,
    users: Bucket,
}

impl UserStore {
    fn open(path: &Path) -> Result<Self> {
        let db = Db::open(DbOptions::persistent(path).with_durability(DurabilityMode::Flush))?;
        let users = db.open_bucket_with_options(
            "users",
            BucketOptions::default().with_prefix_extractor(PrefixExtractor::Separator(b':')),
        )?;
        Ok(Self { db, users })
    }

    fn put_user(&self, user: &User) -> Result<()> {
        self.users.put(user_key(&user.id), user.encode()?)
    }

    fn get_user(&self, id: &str) -> Result<Option<User>> {
        self.users
            .get(&user_key(id))?
            .map(|bytes| User::decode(&bytes))
            .transpose()
    }

    fn list_users(&self) -> Result<Vec<User>> {
        self.users
            .prefix(b"user:")?
            .map(|item| item.and_then(|key_value| User::decode(&key_value.value)))
            .collect()
    }

    fn rename_if_email_matches(
        &self,
        id: &str,
        expected_email: &str,
        new_name: &str,
    ) -> Result<bool> {
        let key = user_key(id);
        let mut transaction = self.db.transaction(TransactionOptions::default());
        let Some(bytes) = transaction.get("users", &key)? else {
            return Ok(false);
        };
        let mut user = User::decode(&bytes)?;
        if user.email != expected_email {
            return Ok(false);
        }

        new_name.clone_into(&mut user.display_name);
        transaction.put("users", key, user.encode()?);
        transaction.commit()?;
        Ok(true)
    }

    fn flush(&self) -> Result<()> {
        self.db.flush()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct User {
    id: String,
    display_name: String,
    email: String,
}

impl User {
    fn new(id: &str, display_name: &str, email: &str) -> Self {
        Self {
            id: id.to_owned(),
            display_name: display_name.to_owned(),
            email: email.to_owned(),
        }
    }

    fn encode(&self) -> Result<Vec<u8>> {
        encode_fields(&[&self.id, &self.display_name, &self.email])
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut fields = FieldCursor::new(bytes);
        let user = Self {
            id: fields.read_string()?,
            display_name: fields.read_string()?,
            email: fields.read_string()?,
        };
        fields.finish()?;
        Ok(user)
    }
}

fn user_key(id: &str) -> Vec<u8> {
    format!("user:{id}").into_bytes()
}

fn encode_fields(fields: &[&str]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for field in fields {
        let len = u32::try_from(field.len())
            .map_err(|_| Error::invalid_options("user field exceeds u32::MAX"))?;
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
            .ok_or_else(|| invalid_user("short field length"))?;
        self.offset += 4;

        let len =
            u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| invalid_user("field length overflows usize"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| invalid_user("short field bytes"))?;
        self.offset = end;

        std::str::from_utf8(value)
            .map(str::to_owned)
            .map_err(|_| invalid_user("field is not UTF-8"))
    }

    fn finish(&self) -> Result<()> {
        if self.offset == self.bytes.len() {
            return Ok(());
        }
        Err(invalid_user("trailing bytes"))
    }
}

fn invalid_user(message: &'static str) -> Error {
    Error::InvalidFormat {
        message: format!("invalid user record: {message}"),
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
