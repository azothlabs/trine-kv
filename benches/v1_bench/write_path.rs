use super::{
    BenchResult, ColdReadDiagnostics, Db, DbOptions, Instant, ROWS, WAL_REPLAY_DIAGNOSTIC_RUNS,
    benchmark_persistent_options, cleanup_dir, duration_micros, key, labelled, temp_dir, value,
};

#[derive(Default)]
pub(super) struct WritePathDiagnostics {
    open_append_requests: u64,
    append_requests: u64,
    persist_requests: u64,
    write_object_requests: u64,
    publish_manifest_requests: u64,
    sync_directory_requests: u64,
    delete_object_requests: u64,
    open_append_micros: u64,
    append_micros: u64,
    persist_micros: u64,
    write_object_micros: u64,
    publish_manifest_micros: u64,
    sync_directory_micros: u64,
    delete_object_micros: u64,
    pending_sync_bytes: u64,
}

impl WritePathDiagnostics {
    pub(super) fn record_delta(&mut self, before: &trine_kv::DbStats, after: &trine_kv::DbStats) {
        self.open_append_requests = self.open_append_requests.saturating_add(
            after
                .storage_operations
                .open_append
                .requests
                .saturating_sub(before.storage_operations.open_append.requests),
        );
        self.append_requests = self.append_requests.saturating_add(
            after
                .storage_operations
                .append
                .requests
                .saturating_sub(before.storage_operations.append.requests),
        );
        self.persist_requests = self.persist_requests.saturating_add(
            after
                .storage_operations
                .persist
                .requests
                .saturating_sub(before.storage_operations.persist.requests),
        );
        self.write_object_requests = self.write_object_requests.saturating_add(
            after
                .storage_operations
                .write_object
                .requests
                .saturating_sub(before.storage_operations.write_object.requests),
        );
        self.publish_manifest_requests = self.publish_manifest_requests.saturating_add(
            after
                .storage_operations
                .publish_manifest
                .requests
                .saturating_sub(before.storage_operations.publish_manifest.requests),
        );
        self.sync_directory_requests = self.sync_directory_requests.saturating_add(
            after
                .storage_operations
                .sync_directory_after_renames
                .requests
                .saturating_sub(
                    before
                        .storage_operations
                        .sync_directory_after_renames
                        .requests,
                ),
        );
        self.delete_object_requests = self.delete_object_requests.saturating_add(
            after
                .storage_operations
                .delete_object
                .requests
                .saturating_sub(before.storage_operations.delete_object.requests),
        );
        self.record_latency_delta(before, after);
        self.pending_sync_bytes = self
            .pending_sync_bytes
            .saturating_add(after.wal_bytes_pending_sync);
    }

    fn record_latency_delta(&mut self, before: &trine_kv::DbStats, after: &trine_kv::DbStats) {
        self.open_append_micros = self.open_append_micros.saturating_add(
            after
                .storage_operations
                .open_append
                .total_latency_micros
                .saturating_sub(before.storage_operations.open_append.total_latency_micros),
        );
        self.append_micros = self.append_micros.saturating_add(
            after
                .storage_operations
                .append
                .total_latency_micros
                .saturating_sub(before.storage_operations.append.total_latency_micros),
        );
        self.persist_micros = self.persist_micros.saturating_add(
            after
                .storage_operations
                .persist
                .total_latency_micros
                .saturating_sub(before.storage_operations.persist.total_latency_micros),
        );
        self.write_object_micros = self.write_object_micros.saturating_add(
            after
                .storage_operations
                .write_object
                .total_latency_micros
                .saturating_sub(before.storage_operations.write_object.total_latency_micros),
        );
        self.publish_manifest_micros = self.publish_manifest_micros.saturating_add(
            after
                .storage_operations
                .publish_manifest
                .total_latency_micros
                .saturating_sub(
                    before
                        .storage_operations
                        .publish_manifest
                        .total_latency_micros,
                ),
        );
        self.sync_directory_micros = self.sync_directory_micros.saturating_add(
            after
                .storage_operations
                .sync_directory_after_renames
                .total_latency_micros
                .saturating_sub(
                    before
                        .storage_operations
                        .sync_directory_after_renames
                        .total_latency_micros,
                ),
        );
        self.delete_object_micros = self.delete_object_micros.saturating_add(
            after
                .storage_operations
                .delete_object
                .total_latency_micros
                .saturating_sub(before.storage_operations.delete_object.total_latency_micros),
        );
    }

    pub(super) fn push_results(&self, results: &mut Vec<BenchResult>, label: &'static str) {
        results.push(BenchResult::diagnostic(
            labelled(label, "storage open append requests"),
            self.open_append_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage append requests"),
            self.append_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage persist requests"),
            self.persist_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage write object requests"),
            self.write_object_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage publish manifest requests"),
            self.publish_manifest_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage sync directory requests"),
            self.sync_directory_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage delete object requests"),
            self.delete_object_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage open append micros"),
            self.open_append_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage append micros"),
            self.append_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage persist micros"),
            self.persist_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage write object micros"),
            self.write_object_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage publish manifest micros"),
            self.publish_manifest_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage sync directory micros"),
            self.sync_directory_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage delete object micros"),
            self.delete_object_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "pending sync bytes"),
            self.pending_sync_bytes,
        ));
    }
}

pub(super) fn extend_wal_replay_open_diagnostics(
    results: &mut Vec<BenchResult>,
    label: &'static str,
    read_only: bool,
) {
    let mut open_diagnostics = ColdReadDiagnostics::default();
    let mut first_read_diagnostics = ColdReadDiagnostics::default();
    let mut open_wall_micros = 0_u64;
    let mut open_memtable_bytes = 0_u64;
    let mut open_visible_sequence = 0_u64;
    let mut open_wal_shards = 0_u64;
    let mut open_wal_open_shards = 0_u64;

    for _ in 0..WAL_REPLAY_DIAGNOSTIC_RUNS {
        let dir = temp_dir("wal-replay-diagnostics");
        let options = benchmark_persistent_options(&dir);
        populate_wal_replay_dir(options.clone());
        let open_options = if read_only {
            options.read_only()
        } else {
            options
        };

        let start = Instant::now();
        let db = Db::open_sync(open_options).expect("persistent db reopens");
        open_wall_micros = open_wall_micros.saturating_add(duration_micros(start.elapsed()));

        let open_stats = db.stats();
        open_diagnostics.record(&open_stats);
        open_memtable_bytes = open_memtable_bytes.saturating_add(open_stats.memtable_bytes);
        open_visible_sequence =
            open_visible_sequence.saturating_add(open_stats.commit_visible_sequence);
        open_wal_shards = open_wal_shards.saturating_add(open_stats.wal_shards as u64);
        open_wal_open_shards =
            open_wal_open_shards.saturating_add(open_stats.wal_open_shards as u64);

        let bucket = db.default_bucket_sync().expect("bucket reopens");
        let value_len = bucket
            .get_sync(&key(ROWS / 2))
            .expect("get succeeds")
            .map_or(0, |value| value.len());
        assert!(value_len > 0, "WAL replay diagnostic must read a value");

        let after_first_read = db.stats();
        first_read_diagnostics.record_delta(&open_stats, &after_first_read);
        drop(db);
        cleanup_dir(&dir);
    }

    results.push(BenchResult::diagnostic(
        labelled(label, "wall micros"),
        open_wall_micros,
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "memtable bytes"),
        open_memtable_bytes,
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "visible sequence"),
        open_visible_sequence,
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "configured shards"),
        open_wal_shards,
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "active shards"),
        open_wal_open_shards,
    ));
    open_diagnostics.push_results_with_label(results, label);
    first_read_diagnostics.push_results_with_label(results, labelled(label, "first read"));
}

pub(super) fn populate_wal_replay_dir(options: DbOptions) {
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..ROWS {
        bucket
            .put_sync(key(index), value(index))
            .expect("put succeeds");
    }
}
