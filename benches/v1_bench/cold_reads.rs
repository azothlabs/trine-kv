use super::*;

pub(super) fn bench_cold_table_read() -> BenchResult {
    let dir = temp_dir("cold-read");
    let options = benchmark_persistent_options(&dir);
    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        for index in 0..ROWS {
            bucket
                .put_sync(key(index), value(index))
                .expect("put succeeds");
        }
        db.flush_sync().expect("flush succeeds");
    }

    let result = measure("cold table read", 32, || {
        let mut checksum = 0;
        for _ in 0..32 {
            let db = Db::open_sync(options.clone()).expect("persistent db reopens");
            let bucket = db.default_bucket_sync().expect("bucket reopens");
            checksum += bucket
                .get_sync(&key(ROWS / 2))
                .expect("get succeeds")
                .map_or(0, |value| value.len() as u64);
        }
        checksum
    });
    cleanup_dir(&dir);
    result
}

pub(super) fn bench_cold_table_read_only() -> BenchResult {
    let dir = temp_dir("cold-read-only");
    let options = benchmark_persistent_options(&dir);
    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        for index in 0..ROWS {
            bucket
                .put_sync(key(index), value(index))
                .expect("put succeeds");
        }
        db.flush_sync().expect("flush succeeds");
    }

    let read_only_options = options.read_only();
    let result = measure("cold table read-only", 32, || {
        let mut checksum = 0;
        for _ in 0..32 {
            let db =
                Db::open_sync(read_only_options.clone()).expect("read-only persistent db reopens");
            let bucket = db.default_bucket_sync().expect("bucket reopens");
            checksum += bucket
                .get_sync(&key(ROWS / 2))
                .expect("get succeeds")
                .map_or(0, |value| value.len() as u64);
        }
        checksum
    });
    cleanup_dir(&dir);
    result
}

pub(super) fn bench_cold_table_open_wall_diagnostics() -> Vec<BenchResult> {
    let mut results = Vec::new();
    extend_cold_table_open_wall_diagnostics(&mut results, false);
    extend_cold_table_open_wall_diagnostics(&mut results, true);
    results
}

pub(super) fn extend_cold_table_open_wall_diagnostics(
    results: &mut Vec<BenchResult>,
    read_only: bool,
) {
    let dir = if read_only {
        temp_dir("cold-open-wall-read-only")
    } else {
        temp_dir("cold-open-wall")
    };
    let options = benchmark_persistent_options(&dir);
    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        for index in 0..ROWS {
            bucket
                .put_sync(key(index), value(index))
                .expect("put succeeds");
        }
        db.flush_sync().expect("flush succeeds");
    }

    let open_options = if read_only {
        options.read_only()
    } else {
        options
    };
    let label = if read_only {
        "cold table read-only"
    } else {
        "cold table"
    };
    let mut open_wall_micros = 0_u64;
    let mut first_read_wall_micros = 0_u64;
    let mut close_wall_micros = 0_u64;
    let mut checksum = 0_u64;
    for _ in 0..32 {
        let open_start = Instant::now();
        let db = Db::open_sync(open_options.clone()).expect("persistent db reopens");
        open_wall_micros = open_wall_micros.saturating_add(duration_micros(open_start.elapsed()));

        let bucket = db.default_bucket_sync().expect("bucket reopens");
        let read_start = Instant::now();
        checksum = checksum.saturating_add(
            bucket
                .get_sync(&key(ROWS / 2))
                .expect("get succeeds")
                .map_or(0, |value| value.len() as u64),
        );
        first_read_wall_micros =
            first_read_wall_micros.saturating_add(duration_micros(read_start.elapsed()));
        drop(bucket);

        let close_start = Instant::now();
        drop(db);
        close_wall_micros =
            close_wall_micros.saturating_add(duration_micros(close_start.elapsed()));
    }
    assert!(checksum > 0, "cold open wall diagnostic must read values");
    cleanup_dir(&dir);

    results.push(BenchResult::diagnostic(
        labelled(label, "open wall micros"),
        open_wall_micros,
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "first read wall micros"),
        first_read_wall_micros,
    ));
    results.push(BenchResult::diagnostic(
        labelled(label, "close wall micros"),
        close_wall_micros,
    ));
}

pub(super) fn bench_read_pruning_diagnostics() -> Vec<BenchResult> {
    let mut results = Vec::new();
    extend_cold_table_read_diagnostics(&mut results, false);
    extend_cold_table_read_diagnostics(&mut results, true);
    extend_l0_stack_read_diagnostics(&mut results);
    extend_range_guard_diagnostics(&mut results);
    extend_prefix_partition_diagnostics(&mut results);
    results
}

pub(super) fn extend_cold_table_read_diagnostics(results: &mut Vec<BenchResult>, read_only: bool) {
    let dir = if read_only {
        temp_dir("read-pruning-cold-read-only")
    } else {
        temp_dir("read-pruning-cold-read")
    };
    let options = benchmark_persistent_options(&dir);
    {
        let db = Db::open_sync(options.clone()).expect("persistent db opens");
        let bucket = db.default_bucket_sync().expect("bucket opens");
        for index in 0..ROWS {
            bucket
                .put_sync(key(index), value(index))
                .expect("put succeeds");
        }
        db.flush_sync().expect("flush succeeds");
    }

    let open_options = if read_only {
        options.read_only()
    } else {
        options
    };
    let mut diagnostics = ColdReadDiagnostics::default();
    let mut open_diagnostics = ColdReadDiagnostics::default();
    let mut first_read_diagnostics = ColdReadDiagnostics::default();
    for _ in 0..32 {
        let db = Db::open_sync(open_options.clone()).expect("persistent db reopens");
        let open_stats = db.stats();
        open_diagnostics.record(&open_stats);

        let bucket = db.default_bucket_sync().expect("bucket reopens");
        let value_len = bucket
            .get_sync(&key(ROWS / 2))
            .expect("get succeeds")
            .map_or(0, |value| value.len());
        assert!(value_len > 0, "cold-read diagnostic must read a value");
        let stats = db.stats();
        diagnostics.record(&stats);
        first_read_diagnostics.record_delta(&open_stats, &stats);
    }
    cleanup_dir(&dir);

    diagnostics.push_results(results, read_only);
    open_diagnostics.push_phase_results(results, read_only, "open");
    first_read_diagnostics.push_phase_results(results, read_only, "first read");
}

pub(super) fn extend_l0_stack_read_diagnostics(results: &mut Vec<BenchResult>) {
    const L0_TABLES: usize = 8;
    const ROWS_PER_TABLE: usize = 16;

    let dir = temp_dir("read-pruning-l0-stack");
    let mut options = benchmark_persistent_options(&dir);
    options.background_worker_count = 0;
    options.max_l0_files = L0_TABLES * 4;
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for table_index in 0..L0_TABLES {
        for row_index in 0..ROWS_PER_TABLE {
            let index = table_index * ROWS_PER_TABLE + row_index;
            bucket
                .put_sync(key(index), value(index))
                .expect("put succeeds");
        }
        db.flush_sync().expect("flush succeeds");
    }

    let stats = db.stats();
    assert_eq!(stats.l0_tables, L0_TABLES, "diagnostic needs an L0 stack");

    let target_index = ROWS_PER_TABLE / 2;
    let keys = (0..OPS).map(|_| key(target_index)).collect::<Vec<_>>();
    let before = db.stats();
    let start = Instant::now();
    let checksum = sequential_point_batch_checksum(&bucket, &keys);
    assert!(checksum > 0, "L0 stack diagnostic must read values");
    let elapsed_micros = duration_micros(start.elapsed());
    let after = db.stats();

    let label = "read pruning L0 stack diagnostic sequential";
    let mut diagnostics = ColdReadDiagnostics::default();
    diagnostics.record_delta(&before, &after);
    results.push(BenchResult::diagnostic(
        labelled(label, "wall micros"),
        elapsed_micros,
    ));
    diagnostics.push_results_with_label(results, label);

    let before = db.stats();
    let start = Instant::now();
    let checksum = batched_point_read_checksum(&bucket, &keys, POINT_READ_BATCH);
    assert!(checksum > 0, "L0 stack batch diagnostic must read values");
    let elapsed_micros = duration_micros(start.elapsed());
    let after = db.stats();

    let label = "read pruning L0 stack diagnostic batch 4";
    let mut diagnostics = ColdReadDiagnostics::default();
    diagnostics.record_delta(&before, &after);
    results.push(BenchResult::diagnostic(
        labelled(label, "wall micros"),
        elapsed_micros,
    ));
    diagnostics.push_results_with_label(results, label);

    drop(db);
    cleanup_dir(&dir);
}

pub(super) fn extend_range_guard_diagnostics(results: &mut Vec<BenchResult>) {
    const L0_TABLES: usize = 4;
    const ROWS_PER_TABLE: usize = 16;

    let dir = temp_dir("read-pruning-range-guard");
    let mut options = benchmark_persistent_options(&dir);
    options.background_worker_count = 0;
    options.max_l0_files = L0_TABLES * 4;
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for table_index in 0..L0_TABLES {
        for row_index in 0..ROWS_PER_TABLE {
            let index = table_index * ROWS_PER_TABLE + row_index;
            bucket
                .put_sync(key(index), value(index))
                .expect("put succeeds");
        }
        db.flush_sync().expect("flush succeeds");
    }

    let range = KeyRange::half_open(key(ROWS_PER_TABLE), key(ROWS_PER_TABLE * 2));
    let before = db.stats();
    let checksum = range_checksum(&bucket, &range);
    assert!(checksum > 0, "range guard diagnostic must read rows");
    let after = db.stats();
    push_range_diagnostics(results, "read pruning range guarded", &before, &after);

    bucket
        .delete_range_sync(range.clone())
        .expect("range delete succeeds");
    db.flush_sync().expect("flush tombstone-only table");

    let before = db.stats();
    let checksum = range_checksum(&bucket, &range);
    assert_eq!(checksum, 0, "range tombstone diagnostic must hide rows");
    let after = db.stats();
    push_range_diagnostics(
        results,
        "read pruning range tombstone guarded",
        &before,
        &after,
    );

    drop(db);
    cleanup_dir(&dir);
}

pub(super) fn range_checksum(bucket: &trine_kv::Bucket, range: &KeyRange) -> u64 {
    bucket
        .range_sync(range)
        .expect("range succeeds")
        .map(|item| item.expect("range item").value.len() as u64)
        .sum()
}

pub(super) fn push_range_diagnostics(
    results: &mut Vec<BenchResult>,
    name_prefix: &'static str,
    before: &trine_kv::DbStats,
    after: &trine_kv::DbStats,
) {
    results.push(BenchResult::diagnostic(
        labelled(name_prefix, "table probes"),
        after
            .read_path
            .range_table_probes
            .saturating_sub(before.read_path.range_table_probes),
    ));
    results.push(BenchResult::diagnostic(
        labelled(name_prefix, "L0 table probes"),
        after
            .read_path
            .range_l0_table_probes
            .saturating_sub(before.read_path.range_l0_table_probes),
    ));
    results.push(BenchResult::diagnostic(
        labelled(name_prefix, "non-L0 table probes"),
        after
            .read_path
            .range_non_l0_table_probes
            .saturating_sub(before.read_path.range_non_l0_table_probes),
    ));
    results.push(BenchResult::diagnostic(
        labelled(name_prefix, "tombstone table probes"),
        after
            .read_path
            .range_tombstone_table_probes
            .saturating_sub(before.read_path.range_tombstone_table_probes),
    ));
}

#[derive(Default)]
pub(super) struct ColdReadDiagnostics {
    pub(super) table_probes: u64,
    pub(super) l0_table_probes: u64,
    pub(super) non_l0_table_probes: u64,
    pub(super) l0_lookup_keys: u64,
    pub(super) l0_overlap_extra_table_probes: u64,
    pub(super) batch_input_keys: u64,
    pub(super) batch_unique_keys: u64,
    pub(super) batch_table_groups: u64,
    pub(super) batch_l0_lookup_keys: u64,
    pub(super) batch_l0_overlap_extra_table_probes: u64,
    pub(super) block_metadata_probes: u64,
    pub(super) data_block_reads: u64,
    pub(super) filter_misses: u64,
    pub(super) cache_misses: u64,
    pub(super) open_read_requests: u64,
    pub(super) len_requests: u64,
    pub(super) read_exact_at_owned_requests: u64,
    pub(super) read_object_bytes_requests: u64,
    pub(super) read_current_manifest_requests: u64,
    pub(super) open_append_requests: u64,
    pub(super) acquire_writer_lease_requests: u64,
    pub(super) list_directory_files_requests: u64,
    pub(super) list_objects_requests: u64,
    pub(super) open_read_micros: u64,
    pub(super) len_micros: u64,
    pub(super) read_exact_at_owned_micros: u64,
    pub(super) read_object_bytes_micros: u64,
    pub(super) read_current_manifest_micros: u64,
    pub(super) open_append_micros: u64,
    pub(super) acquire_writer_lease_micros: u64,
    pub(super) list_directory_files_micros: u64,
    pub(super) list_objects_micros: u64,
}

impl ColdReadDiagnostics {
    pub(super) fn record(&mut self, stats: &trine_kv::DbStats) {
        self.record_read_path(stats);
        self.record_storage_requests(stats);
        self.record_storage_latencies(stats);
    }

    fn record_read_path(&mut self, stats: &trine_kv::DbStats) {
        self.table_probes = self
            .table_probes
            .saturating_add(stats.read_path.point_table_probes);
        self.l0_table_probes = self
            .l0_table_probes
            .saturating_add(stats.read_path.point_l0_table_probes);
        self.non_l0_table_probes = self
            .non_l0_table_probes
            .saturating_add(stats.read_path.point_non_l0_table_probes);
        self.l0_lookup_keys = self
            .l0_lookup_keys
            .saturating_add(stats.read_path.point_l0_lookup_keys);
        self.l0_overlap_extra_table_probes = self
            .l0_overlap_extra_table_probes
            .saturating_add(stats.read_path.point_l0_overlap_extra_table_probes);
        self.batch_input_keys = self
            .batch_input_keys
            .saturating_add(stats.read_path.batch_point_input_keys);
        self.batch_unique_keys = self
            .batch_unique_keys
            .saturating_add(stats.read_path.batch_point_unique_keys);
        self.batch_table_groups = self
            .batch_table_groups
            .saturating_add(stats.read_path.batch_point_table_groups);
        self.batch_l0_lookup_keys = self
            .batch_l0_lookup_keys
            .saturating_add(stats.read_path.batch_point_l0_lookup_keys);
        self.batch_l0_overlap_extra_table_probes = self
            .batch_l0_overlap_extra_table_probes
            .saturating_add(stats.read_path.batch_point_l0_overlap_extra_table_probes);
        self.block_metadata_probes = self
            .block_metadata_probes
            .saturating_add(stats.read_path.point_block_metadata_probes);
        self.data_block_reads = self
            .data_block_reads
            .saturating_add(stats.read_path.point_data_block_reads);
        self.filter_misses = self
            .filter_misses
            .saturating_add(stats.read_path.point_filter_misses);
        self.cache_misses = self.cache_misses.saturating_add(stats.block_cache_misses);
    }

    fn record_storage_requests(&mut self, stats: &trine_kv::DbStats) {
        self.open_read_requests = self
            .open_read_requests
            .saturating_add(stats.storage_operations.open_read.requests);
        self.len_requests = self
            .len_requests
            .saturating_add(stats.storage_operations.len.requests);
        self.read_exact_at_owned_requests = self
            .read_exact_at_owned_requests
            .saturating_add(stats.storage_operations.read_exact_at_owned.requests);
        self.read_object_bytes_requests = self
            .read_object_bytes_requests
            .saturating_add(stats.storage_operations.read_object_bytes.requests);
        self.read_current_manifest_requests = self
            .read_current_manifest_requests
            .saturating_add(stats.storage_operations.read_current_manifest.requests);
        self.open_append_requests = self
            .open_append_requests
            .saturating_add(stats.storage_operations.open_append.requests);
        self.acquire_writer_lease_requests = self
            .acquire_writer_lease_requests
            .saturating_add(stats.storage_operations.acquire_writer_lease.requests);
        self.list_directory_files_requests = self
            .list_directory_files_requests
            .saturating_add(stats.storage_operations.list_directory_files.requests);
        self.list_objects_requests = self
            .list_objects_requests
            .saturating_add(stats.storage_operations.list_objects.requests);
    }

    fn record_storage_latencies(&mut self, stats: &trine_kv::DbStats) {
        self.open_read_micros = self
            .open_read_micros
            .saturating_add(stats.storage_operations.open_read.total_latency_micros);
        self.len_micros = self
            .len_micros
            .saturating_add(stats.storage_operations.len.total_latency_micros);
        self.read_exact_at_owned_micros = self.read_exact_at_owned_micros.saturating_add(
            stats
                .storage_operations
                .read_exact_at_owned
                .total_latency_micros,
        );
        self.read_object_bytes_micros = self.read_object_bytes_micros.saturating_add(
            stats
                .storage_operations
                .read_object_bytes
                .total_latency_micros,
        );
        self.read_current_manifest_micros = self.read_current_manifest_micros.saturating_add(
            stats
                .storage_operations
                .read_current_manifest
                .total_latency_micros,
        );
        self.open_append_micros = self
            .open_append_micros
            .saturating_add(stats.storage_operations.open_append.total_latency_micros);
        self.acquire_writer_lease_micros = self.acquire_writer_lease_micros.saturating_add(
            stats
                .storage_operations
                .acquire_writer_lease
                .total_latency_micros,
        );
        self.list_directory_files_micros = self.list_directory_files_micros.saturating_add(
            stats
                .storage_operations
                .list_directory_files
                .total_latency_micros,
        );
        self.list_objects_micros = self
            .list_objects_micros
            .saturating_add(stats.storage_operations.list_objects.total_latency_micros);
    }

    pub(super) fn record_delta(&mut self, before: &trine_kv::DbStats, after: &trine_kv::DbStats) {
        self.record_read_path_delta(before, after);
        self.record_storage_request_delta(before, after);
        self.record_storage_latency_delta(before, after);
    }

    fn record_read_path_delta(&mut self, before: &trine_kv::DbStats, after: &trine_kv::DbStats) {
        self.table_probes = self.table_probes.saturating_add(
            after
                .read_path
                .point_table_probes
                .saturating_sub(before.read_path.point_table_probes),
        );
        self.l0_table_probes = self.l0_table_probes.saturating_add(
            after
                .read_path
                .point_l0_table_probes
                .saturating_sub(before.read_path.point_l0_table_probes),
        );
        self.non_l0_table_probes = self.non_l0_table_probes.saturating_add(
            after
                .read_path
                .point_non_l0_table_probes
                .saturating_sub(before.read_path.point_non_l0_table_probes),
        );
        self.l0_lookup_keys = self.l0_lookup_keys.saturating_add(
            after
                .read_path
                .point_l0_lookup_keys
                .saturating_sub(before.read_path.point_l0_lookup_keys),
        );
        self.l0_overlap_extra_table_probes = self.l0_overlap_extra_table_probes.saturating_add(
            after
                .read_path
                .point_l0_overlap_extra_table_probes
                .saturating_sub(before.read_path.point_l0_overlap_extra_table_probes),
        );
        self.batch_input_keys = self.batch_input_keys.saturating_add(
            after
                .read_path
                .batch_point_input_keys
                .saturating_sub(before.read_path.batch_point_input_keys),
        );
        self.batch_unique_keys = self.batch_unique_keys.saturating_add(
            after
                .read_path
                .batch_point_unique_keys
                .saturating_sub(before.read_path.batch_point_unique_keys),
        );
        self.batch_table_groups = self.batch_table_groups.saturating_add(
            after
                .read_path
                .batch_point_table_groups
                .saturating_sub(before.read_path.batch_point_table_groups),
        );
        self.batch_l0_lookup_keys = self.batch_l0_lookup_keys.saturating_add(
            after
                .read_path
                .batch_point_l0_lookup_keys
                .saturating_sub(before.read_path.batch_point_l0_lookup_keys),
        );
        self.batch_l0_overlap_extra_table_probes =
            self.batch_l0_overlap_extra_table_probes.saturating_add(
                after
                    .read_path
                    .batch_point_l0_overlap_extra_table_probes
                    .saturating_sub(before.read_path.batch_point_l0_overlap_extra_table_probes),
            );
        self.block_metadata_probes = self.block_metadata_probes.saturating_add(
            after
                .read_path
                .point_block_metadata_probes
                .saturating_sub(before.read_path.point_block_metadata_probes),
        );
        self.data_block_reads = self.data_block_reads.saturating_add(
            after
                .read_path
                .point_data_block_reads
                .saturating_sub(before.read_path.point_data_block_reads),
        );
        self.filter_misses = self.filter_misses.saturating_add(
            after
                .read_path
                .point_filter_misses
                .saturating_sub(before.read_path.point_filter_misses),
        );
        self.cache_misses = self.cache_misses.saturating_add(
            after
                .block_cache_misses
                .saturating_sub(before.block_cache_misses),
        );
    }

    fn record_storage_request_delta(
        &mut self,
        before: &trine_kv::DbStats,
        after: &trine_kv::DbStats,
    ) {
        self.open_read_requests = self.open_read_requests.saturating_add(
            after
                .storage_operations
                .open_read
                .requests
                .saturating_sub(before.storage_operations.open_read.requests),
        );
        self.len_requests = self.len_requests.saturating_add(
            after
                .storage_operations
                .len
                .requests
                .saturating_sub(before.storage_operations.len.requests),
        );
        self.read_exact_at_owned_requests = self.read_exact_at_owned_requests.saturating_add(
            after
                .storage_operations
                .read_exact_at_owned
                .requests
                .saturating_sub(before.storage_operations.read_exact_at_owned.requests),
        );
        self.read_object_bytes_requests = self.read_object_bytes_requests.saturating_add(
            after
                .storage_operations
                .read_object_bytes
                .requests
                .saturating_sub(before.storage_operations.read_object_bytes.requests),
        );
        self.read_current_manifest_requests = self.read_current_manifest_requests.saturating_add(
            after
                .storage_operations
                .read_current_manifest
                .requests
                .saturating_sub(before.storage_operations.read_current_manifest.requests),
        );
        self.open_append_requests = self.open_append_requests.saturating_add(
            after
                .storage_operations
                .open_append
                .requests
                .saturating_sub(before.storage_operations.open_append.requests),
        );
        self.acquire_writer_lease_requests = self.acquire_writer_lease_requests.saturating_add(
            after
                .storage_operations
                .acquire_writer_lease
                .requests
                .saturating_sub(before.storage_operations.acquire_writer_lease.requests),
        );
        self.list_directory_files_requests = self.list_directory_files_requests.saturating_add(
            after
                .storage_operations
                .list_directory_files
                .requests
                .saturating_sub(before.storage_operations.list_directory_files.requests),
        );
        self.list_objects_requests = self.list_objects_requests.saturating_add(
            after
                .storage_operations
                .list_objects
                .requests
                .saturating_sub(before.storage_operations.list_objects.requests),
        );
    }

    fn record_storage_latency_delta(
        &mut self,
        before: &trine_kv::DbStats,
        after: &trine_kv::DbStats,
    ) {
        self.open_read_micros = self.open_read_micros.saturating_add(
            after
                .storage_operations
                .open_read
                .total_latency_micros
                .saturating_sub(before.storage_operations.open_read.total_latency_micros),
        );
        self.len_micros = self.len_micros.saturating_add(
            after
                .storage_operations
                .len
                .total_latency_micros
                .saturating_sub(before.storage_operations.len.total_latency_micros),
        );
        self.read_exact_at_owned_micros = self.read_exact_at_owned_micros.saturating_add(
            after
                .storage_operations
                .read_exact_at_owned
                .total_latency_micros
                .saturating_sub(
                    before
                        .storage_operations
                        .read_exact_at_owned
                        .total_latency_micros,
                ),
        );
        self.read_object_bytes_micros = self.read_object_bytes_micros.saturating_add(
            after
                .storage_operations
                .read_object_bytes
                .total_latency_micros
                .saturating_sub(
                    before
                        .storage_operations
                        .read_object_bytes
                        .total_latency_micros,
                ),
        );
        self.read_current_manifest_micros = self.read_current_manifest_micros.saturating_add(
            after
                .storage_operations
                .read_current_manifest
                .total_latency_micros
                .saturating_sub(
                    before
                        .storage_operations
                        .read_current_manifest
                        .total_latency_micros,
                ),
        );
        self.open_append_micros = self.open_append_micros.saturating_add(
            after
                .storage_operations
                .open_append
                .total_latency_micros
                .saturating_sub(before.storage_operations.open_append.total_latency_micros),
        );
        self.acquire_writer_lease_micros = self.acquire_writer_lease_micros.saturating_add(
            after
                .storage_operations
                .acquire_writer_lease
                .total_latency_micros
                .saturating_sub(
                    before
                        .storage_operations
                        .acquire_writer_lease
                        .total_latency_micros,
                ),
        );
        self.list_directory_files_micros = self.list_directory_files_micros.saturating_add(
            after
                .storage_operations
                .list_directory_files
                .total_latency_micros
                .saturating_sub(
                    before
                        .storage_operations
                        .list_directory_files
                        .total_latency_micros,
                ),
        );
        self.list_objects_micros = self.list_objects_micros.saturating_add(
            after
                .storage_operations
                .list_objects
                .total_latency_micros
                .saturating_sub(before.storage_operations.list_objects.total_latency_micros),
        );
    }

    pub(super) fn push_results(&self, results: &mut Vec<BenchResult>, read_only: bool) {
        let label = if read_only {
            "read pruning cold read-only"
        } else {
            "read pruning cold"
        };
        self.push_results_with_label(results, label);
    }

    fn push_phase_results(
        &self,
        results: &mut Vec<BenchResult>,
        read_only: bool,
        phase: &'static str,
    ) {
        let label = if read_only {
            labelled3("read pruning cold read-only", phase, "phase")
        } else {
            labelled3("read pruning cold", phase, "phase")
        };
        self.push_results_with_label(results, label);
    }

    pub(super) fn push_results_with_label(
        &self,
        results: &mut Vec<BenchResult>,
        label: &'static str,
    ) {
        self.push_read_path_results(results, label);
        self.push_storage_request_results(results, label);
        self.push_storage_latency_results(results, label);
    }

    pub(super) fn push_read_path_results(
        &self,
        results: &mut Vec<BenchResult>,
        label: &'static str,
    ) {
        results.push(BenchResult::diagnostic(
            labelled(label, "point table probes"),
            self.table_probes,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "point L0 table probes"),
            self.l0_table_probes,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "point non-L0 table probes"),
            self.non_l0_table_probes,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "point L0 lookup keys"),
            self.l0_lookup_keys,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "point L0 overlap extra table probes"),
            self.l0_overlap_extra_table_probes,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "batch point input keys"),
            self.batch_input_keys,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "batch point unique keys"),
            self.batch_unique_keys,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "batch point table groups"),
            self.batch_table_groups,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "batch point L0 lookup keys"),
            self.batch_l0_lookup_keys,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "batch point L0 overlap extra table probes"),
            self.batch_l0_overlap_extra_table_probes,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "point block metadata probes"),
            self.block_metadata_probes,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "point data block reads"),
            self.data_block_reads,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "point filter skips"),
            self.filter_misses,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "point cache misses"),
            self.cache_misses,
        ));
    }

    fn push_storage_request_results(&self, results: &mut Vec<BenchResult>, label: &'static str) {
        results.push(BenchResult::diagnostic(
            labelled(label, "storage open read requests"),
            self.open_read_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage len requests"),
            self.len_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage read owned requests"),
            self.read_exact_at_owned_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage read object bytes requests"),
            self.read_object_bytes_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage current manifest requests"),
            self.read_current_manifest_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage open append requests"),
            self.open_append_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage acquire writer lease requests"),
            self.acquire_writer_lease_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage list directory files requests"),
            self.list_directory_files_requests,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage list objects requests"),
            self.list_objects_requests,
        ));
    }

    fn push_storage_latency_results(&self, results: &mut Vec<BenchResult>, label: &'static str) {
        results.push(BenchResult::diagnostic(
            labelled(label, "storage open read micros"),
            self.open_read_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage len micros"),
            self.len_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage read owned micros"),
            self.read_exact_at_owned_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage read object bytes micros"),
            self.read_object_bytes_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage current manifest micros"),
            self.read_current_manifest_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage open append micros"),
            self.open_append_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage acquire writer lease micros"),
            self.acquire_writer_lease_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage list directory files micros"),
            self.list_directory_files_micros,
        ));
        results.push(BenchResult::diagnostic(
            labelled(label, "storage list objects micros"),
            self.list_objects_micros,
        ));
    }
}

pub(super) fn extend_prefix_partition_diagnostics(results: &mut Vec<BenchResult>) {
    let dir = temp_dir("read-pruning-prefix");
    let mut options = benchmark_persistent_options(&dir);
    options.default_bucket_options = prefix_options(true);
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..ROWS {
        bucket
            .put_sync(prefix_key(index), value(index))
            .expect("put succeeds");
    }
    db.flush_sync().expect("flush succeeds");

    let before = db.stats();
    let matching_checksum = prefix_scan_checksum(&bucket, 128, false);
    assert!(
        matching_checksum > 0,
        "matching prefix diagnostic must return rows"
    );
    let after_matching = db.stats();
    push_prefix_diagnostics(
        results,
        "read pruning prefix matching",
        &before,
        &after_matching,
    );

    let nonmatching_checksum = prefix_scan_checksum(&bucket, 128, true);
    assert_eq!(
        nonmatching_checksum, 0,
        "nonmatching prefix diagnostic must skip all rows"
    );
    let after_nonmatching = db.stats();
    push_prefix_diagnostics(
        results,
        "read pruning prefix nonmatching",
        &after_matching,
        &after_nonmatching,
    );
    drop(db);
    cleanup_dir(&dir);
}

pub(super) fn push_prefix_diagnostics(
    results: &mut Vec<BenchResult>,
    name_prefix: &'static str,
    before: &trine_kv::DbStats,
    after: &trine_kv::DbStats,
) {
    results.push(BenchResult::diagnostic(
        labelled(name_prefix, "table probes"),
        after
            .read_path
            .prefix_table_probes
            .saturating_sub(before.read_path.prefix_table_probes),
    ));
    results.push(BenchResult::diagnostic(
        labelled(name_prefix, "tombstone table probes"),
        after
            .read_path
            .prefix_tombstone_table_probes
            .saturating_sub(before.read_path.prefix_tombstone_table_probes),
    ));
    results.push(BenchResult::diagnostic(
        labelled(name_prefix, "block metadata probes"),
        after
            .read_path
            .prefix_block_metadata_probes
            .saturating_sub(before.read_path.prefix_block_metadata_probes),
    ));
    results.push(BenchResult::diagnostic(
        labelled(name_prefix, "data block reads"),
        after
            .read_path
            .prefix_data_block_reads
            .saturating_sub(before.read_path.prefix_data_block_reads),
    ));
    results.push(BenchResult::diagnostic(
        labelled(name_prefix, "filter skips"),
        after
            .read_path
            .prefix_filter_misses
            .saturating_sub(before.read_path.prefix_filter_misses),
    ));
    results.push(BenchResult::diagnostic(
        labelled(name_prefix, "table filter misses"),
        after
            .filters
            .table_prefix_misses
            .saturating_sub(before.filters.table_prefix_misses),
    ));
    results.push(BenchResult::diagnostic(
        labelled(name_prefix, "block filter misses"),
        after
            .filters
            .block_prefix_misses
            .saturating_sub(before.filters.block_prefix_misses),
    ));
    results.push(BenchResult::diagnostic(
        labelled(name_prefix, "cache misses"),
        after
            .block_cache_misses
            .saturating_sub(before.block_cache_misses),
    ));
}
