use trine_kv::{BucketOptions, Db, DbOptions, Error, KeyRange};

const WORKER_BLOB_THRESHOLD_BYTES: usize = 4;

pub(crate) fn test_namespace(name: &str) -> String {
    let millis = js_sys::Date::now().to_string().replace('.', "-");
    let random = js_sys::Math::random().to_string().replace('.', "-");
    format!("browser-tests/{name}-{millis}-{random}")
}

pub(crate) async fn run_trine_db_round_trip(namespace: &str) -> Result<(), String> {
    let db = Db::open(worker_db_options(namespace))
        .await
        .map_err(display_error)?;

    db.put(b"worker:wal", b"first")
        .await
        .map_err(display_error)?;
    db.put(b"worker:deleted", b"gone")
        .await
        .map_err(display_error)?;
    db.delete(b"worker:deleted").await.map_err(display_error)?;

    for index in 0_u8..64 {
        let key = format!("worker:append:{index:03}");
        db.put(key.into_bytes(), vec![index; 128])
            .await
            .map_err(display_error)?;
    }

    let docs = db
        .bucket_with_options("worker-docs", worker_bucket_options())
        .await
        .map_err(display_error)?;
    let blob_value = b"value-stored-through-browser-worker".to_vec();
    docs.put(b"doc:blob", blob_value.clone())
        .await
        .map_err(display_error)?;

    db.flush().await.map_err(display_error)?;
    db.put(b"worker:after-flush", b"tail")
        .await
        .map_err(display_error)?;
    db.flush().await.map_err(display_error)?;
    db.compact_range(KeyRange::all())
        .await
        .map_err(display_error)?;
    drop(docs);
    drop(db);

    let db = Db::open(worker_read_only_db_options(namespace))
        .await
        .map_err(display_error)?;
    expect_value(
        db.get(b"worker:wal").await.map_err(display_error)?,
        b"first",
        "worker:wal",
    )?;
    expect_value(
        db.get(b"worker:append:063").await.map_err(display_error)?,
        &[63_u8; 128],
        "worker:append:063",
    )?;
    expect_none(
        db.get(b"worker:deleted").await.map_err(display_error)?,
        "worker:deleted",
    )?;
    expect_value(
        db.get(b"worker:after-flush").await.map_err(display_error)?,
        b"tail",
        "worker:after-flush",
    )?;

    let docs = db
        .bucket_with_options("worker-docs", worker_bucket_options())
        .await
        .map_err(display_error)?;
    expect_value(
        docs.get(b"doc:blob").await.map_err(display_error)?,
        &blob_value,
        "worker-docs/doc:blob",
    )
}

fn worker_db_options(namespace: &str) -> DbOptions {
    DbOptions::browser_persistent_at(namespace).with_default_bucket_options(worker_bucket_options())
}

fn worker_read_only_db_options(namespace: &str) -> DbOptions {
    DbOptions::browser_persistent_read_only_at(namespace)
        .with_default_bucket_options(worker_bucket_options())
}

fn worker_bucket_options() -> BucketOptions {
    BucketOptions::default().with_blob_threshold_bytes(WORKER_BLOB_THRESHOLD_BYTES)
}

fn expect_value(actual: Option<Vec<u8>>, expected: &[u8], label: &str) -> Result<(), String> {
    match actual {
        Some(actual) if actual == expected => Ok(()),
        Some(actual) => Err(format!(
            "{label} mismatch: expected {} bytes, read {} bytes",
            expected.len(),
            actual.len()
        )),
        None => Err(format!("{label} was missing")),
    }
}

fn expect_none(actual: Option<Vec<u8>>, label: &str) -> Result<(), String> {
    match actual {
        None => Ok(()),
        Some(actual) => Err(format!(
            "{label} should have been deleted, read {} bytes",
            actual.len()
        )),
    }
}

fn display_error(error: Error) -> String {
    let message = error.to_string();
    drop(error);
    message
}
