use std::{
    env, fs,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use trine_kv::{Db, DbOptions, DurabilityMode, KeyRange, MaintenanceBudget};

const DEFAULT_CRASH_ROUNDS: usize = 4;
const CRASH_KEYS_PER_ROUND: usize = 32;
const DEFAULT_SOAK_OPERATIONS: usize = 10_000;
const DEFAULT_SOAK_THREADS: usize = 4;
const KEY_SLOTS_PER_THREAD: usize = 128;
const FORCED_EXIT_CODE: i32 = 86;

#[test]
#[ignore = "production evidence: repeatedly exits a child process without running destructors"]
fn forced_process_exit_recovery() {
    let path = temp_db_path("forced-exit-recovery");
    let rounds = env_usize("TRINE_MATURITY_CRASH_ROUNDS", DEFAULT_CRASH_ROUNDS);
    let started = Instant::now();

    for round in 0..rounds {
        let status =
            Command::new(env::current_exe().expect("current test executable is available"))
                .arg("--exact")
                .arg("helper_forced_exit_after_confirmed_writes")
                .arg("--ignored")
                .arg("--nocapture")
                .arg("--test-threads=1")
                .env("TRINE_MATURITY_CRASH_PATH", &path)
                .env("TRINE_MATURITY_CRASH_ROUND", round.to_string())
                .status()
                .expect("forced-exit child starts");
        assert_eq!(
            status.code(),
            Some(FORCED_EXIT_CODE),
            "forced-exit child must bypass Rust destructors"
        );

        let db = Db::open_sync(DbOptions::persistent(&path))
            .unwrap_or_else(|error| panic!("round {round} recovery open failed: {error}"));
        for committed_round in 0..=round {
            assert_crash_round_visible(&db, committed_round);
        }
        db.close_sync();
    }

    let elapsed = started.elapsed();
    append_report(&format!(
        concat!(
            "{{\"scenario\":\"forced_process_exit_recovery\",",
            "\"os\":\"{}\",\"arch\":\"{}\",\"rounds\":{},",
            "\"confirmed_writes\":{},\"elapsed_ms\":{}}}"
        ),
        env::consts::OS,
        env::consts::ARCH,
        rounds,
        rounds.saturating_mul(CRASH_KEYS_PER_ROUND),
        elapsed.as_millis()
    ));
    println!(
        "production maturity: forced-exit rounds={rounds} writes={} elapsed_ms={}",
        rounds.saturating_mul(CRASH_KEYS_PER_ROUND),
        elapsed.as_millis()
    );
    fs::remove_dir_all(path).expect("forced-exit test directory removes");
}

#[test]
#[ignore = "helper invoked by forced_process_exit_recovery"]
fn helper_forced_exit_after_confirmed_writes() {
    let Some(path) = env::var_os("TRINE_MATURITY_CRASH_PATH").map(PathBuf::from) else {
        return;
    };
    let round = env_usize("TRINE_MATURITY_CRASH_ROUND", 0);
    let db = Db::open_sync(DbOptions::persistent(&path)).expect("child database opens");
    for index in 0..CRASH_KEYS_PER_ROUND {
        db.put_sync(crash_key(round, index), crash_value(round, index))
            .expect("confirmed child write succeeds");
    }
    eprintln!("forced-exit child committed round {round}");
    std::process::exit(FORCED_EXIT_CODE);
}

#[test]
#[ignore = "production evidence: configurable concurrent mixed-load soak"]
fn concurrent_mixed_load_soak_reopens_cleanly() {
    let path = temp_db_path("mixed-load-soak");
    let operation_target = env_usize("TRINE_MATURITY_OPERATIONS", DEFAULT_SOAK_OPERATIONS);
    let writer_threads = env_usize("TRINE_MATURITY_THREADS", DEFAULT_SOAK_THREADS).max(1);
    let seed = env_u64("TRINE_MATURITY_SEED", 0x51a7_2026_0712_cafe);
    let options = soak_options(&path);

    let db = Arc::new(Db::open_sync(options.clone()).expect("soak database opens"));
    let barrier = Arc::new(Barrier::new(writer_threads));
    let writers_done = Arc::new(AtomicBool::new(false));
    let maintenance_passes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let started = Instant::now();

    let maintenance = env_bool("TRINE_MATURITY_COOPERATIVE_MAINTENANCE", true).then(|| {
        let db = Arc::clone(&db);
        let writers_done = Arc::clone(&writers_done);
        let maintenance_passes = Arc::clone(&maintenance_passes);
        thread::spawn(move || {
            while !writers_done.load(Ordering::Acquire) {
                db.run_maintenance_with_budget_sync(MaintenanceBudget::new(2, 2))
                    .expect("cooperative maintenance succeeds");
                maintenance_passes.fetch_add(1, Ordering::Relaxed);
                thread::sleep(Duration::from_millis(2));
            }
        })
    });

    let mut writers = Vec::with_capacity(writer_threads);
    for worker in 0..writer_threads {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let worker_operations = operations_for_worker(operation_target, writer_threads, worker);
        writers.push(thread::spawn(move || {
            run_soak_writer(&db, &barrier, worker, worker_operations, seed)
        }));
    }

    let mut models = Vec::with_capacity(writer_threads);
    for writer in writers {
        models.push(writer.join().expect("soak writer joins"));
    }
    writers_done.store(true, Ordering::Release);
    if let Some(maintenance) = maintenance {
        maintenance.join().expect("maintenance worker joins");
    }

    db.persist_sync(DurabilityMode::SyncAll)
        .expect("soak WAL persistence succeeds");
    db.flush_sync().expect("soak flush succeeds");
    db.compact_range_sync(KeyRange::all())
        .expect("soak compaction succeeds");
    let stats = db.stats();
    db.close_sync();
    drop(db);
    verify_soak_reopen(options, &models, seed);

    let elapsed = started.elapsed();
    let actual_operations = models.iter().map(SoakModel::operations).sum::<usize>();
    let operations_per_second = if elapsed.is_zero() {
        0
    } else {
        u64::try_from(actual_operations)
            .unwrap_or(u64::MAX)
            .saturating_mul(1_000)
            / u64::try_from(elapsed.as_millis().max(1)).unwrap_or(u64::MAX)
    };
    let maintenance_passes = maintenance_passes.load(Ordering::Relaxed);
    append_report(&format!(
        concat!(
            "{{\"scenario\":\"concurrent_mixed_load_soak\",",
            "\"os\":\"{}\",\"arch\":\"{}\",\"seed\":{},",
            "\"threads\":{},\"operations\":{},\"elapsed_ms\":{},",
            "\"operations_per_second\":{},\"maintenance_passes\":{},",
            "\"tables_before_reopen\":{},\"compactions\":{}}}"
        ),
        env::consts::OS,
        env::consts::ARCH,
        seed,
        writer_threads,
        actual_operations,
        elapsed.as_millis(),
        operations_per_second,
        maintenance_passes,
        stats.total_tables,
        stats.compaction_runs
    ));
    println!(
        "production maturity: soak seed={} threads={} operations={} elapsed_ms={} \
         ops_per_second={} maintenance_passes={}",
        seed,
        writer_threads,
        actual_operations,
        elapsed.as_millis(),
        operations_per_second,
        maintenance_passes
    );
    fs::remove_dir_all(path).expect("soak test directory removes");
}

fn soak_options(path: &Path) -> DbOptions {
    let mut options = DbOptions::persistent(path).with_durability(DurabilityMode::Buffered);
    options.write_buffer_bytes = 32 * 1024;
    options.target_table_bytes = 64 * 1024;
    options.max_immutable_memtables = 4;
    options.max_l0_files = env_usize("TRINE_MATURITY_MAX_L0_FILES", 4);
    options.background_worker_count = env_usize_allow_zero("TRINE_MATURITY_BACKGROUND_WORKERS", 2);
    options.block_cache_bytes = 8 * 1024 * 1024;
    options.blob_gc_enabled = env_bool("TRINE_MATURITY_BLOB_GC", true);
    options
}

fn verify_soak_reopen(options: DbOptions, models: &[SoakModel], seed: u64) {
    let reopened = Db::open_sync(options).expect("soak database reopens");
    let bucket = reopened.default_bucket_sync().expect("soak bucket reopens");
    for (worker, model) in models.iter().enumerate() {
        for (slot, expected) in model.values.iter().enumerate() {
            assert_eq!(
                bucket
                    .get_sync(&soak_key(worker, slot))
                    .expect("reopened soak value reads"),
                *expected,
                "reopen mismatch for worker {worker} slot {slot} seed {seed}"
            );
        }
    }
    reopened.close_sync();
}

fn run_soak_writer(
    db: &Db,
    barrier: &Barrier,
    worker: usize,
    operations: usize,
    seed: u64,
) -> SoakModel {
    let bucket = db.default_bucket_sync().expect("writer bucket opens");
    let mut rng = TestRng::new(seed ^ (worker as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
    let mut values = vec![None; KEY_SLOTS_PER_THREAD];
    barrier.wait();

    for step in 0..operations {
        let slot = rng.usize(KEY_SLOTS_PER_THREAD);
        let key = soak_key(worker, slot);
        match rng.usize(10) {
            0..=5 => {
                let value = format!("worker-{worker}-step-{step}-{:016x}", rng.next()).into_bytes();
                bucket
                    .put_sync(key.as_slice(), value.clone())
                    .expect("soak put succeeds");
                values[slot] = Some(value);
            }
            6..=7 => {
                bucket
                    .delete_sync(key.as_slice())
                    .expect("soak delete succeeds");
                values[slot] = None;
            }
            _ => {
                let actual = bucket.get_sync(&key).expect("soak point read succeeds");
                if actual != values[slot] {
                    panic_soak_point_mismatch(
                        db,
                        &bucket,
                        &key,
                        values[slot].as_deref(),
                        actual.as_deref(),
                        SoakMismatchContext { worker, step, seed },
                    );
                }
            }
        }

        if step % 257 == 0 {
            let snapshot = db.snapshot();
            assert_eq!(
                bucket
                    .get_at_sync(&snapshot, &key)
                    .expect("soak snapshot read succeeds"),
                values[slot],
                "snapshot mismatch for worker {worker} step {step} seed {seed}"
            );
        }
        if worker == 0 && step != 0 && step % 2_048 == 0 {
            db.persist_sync(DurabilityMode::SyncAll)
                .expect("periodic soak persistence succeeds");
        }
    }

    SoakModel { values, operations }
}

#[derive(Clone, Copy)]
struct SoakMismatchContext {
    worker: usize,
    step: usize,
    seed: u64,
}

fn panic_soak_point_mismatch(
    db: &Db,
    bucket: &trine_kv::Bucket,
    key: &[u8],
    expected: Option<&[u8]>,
    actual: Option<&[u8]>,
    context: SoakMismatchContext,
) -> ! {
    let scanned = bucket
        .range_sync(&KeyRange::all())
        .expect("diagnostic range opens")
        .find_map(|row| {
            let row = row.expect("diagnostic range row reads");
            (row.key == key).then_some(row.value)
        });
    let stats = db.stats();
    panic!(
        "point mismatch worker={} step={} seed={} key={:?} expected={:?} actual={:?} \
         scan={:?} latest={} l0={} tables={} levels={:?}",
        context.worker,
        context.step,
        context.seed,
        String::from_utf8_lossy(key),
        expected.map(String::from_utf8_lossy),
        actual.map(String::from_utf8_lossy),
        scanned.as_deref().map(String::from_utf8_lossy),
        db.latest_read_version().as_u64(),
        stats.l0_tables,
        stats.total_tables,
        stats.level_tables
    )
}

struct SoakModel {
    values: Vec<Option<Vec<u8>>>,
    operations: usize,
}

impl SoakModel {
    const fn operations(&self) -> usize {
        self.operations
    }
}

fn assert_crash_round_visible(db: &Db, round: usize) {
    for index in 0..CRASH_KEYS_PER_ROUND {
        assert_eq!(
            db.get_sync(&crash_key(round, index))
                .expect("recovered confirmed value reads"),
            Some(crash_value(round, index)),
            "confirmed write missing after forced exit: round {round} index {index}"
        );
    }
}

fn crash_key(round: usize, index: usize) -> Vec<u8> {
    format!("crash-round-{round:03}-key-{index:03}").into_bytes()
}

fn crash_value(round: usize, index: usize) -> Vec<u8> {
    format!("confirmed-value-{round:03}-{index:03}").into_bytes()
}

fn soak_key(worker: usize, slot: usize) -> Vec<u8> {
    format!("worker-{worker:03}-key-{slot:03}").into_bytes()
}

fn operations_for_worker(total: usize, workers: usize, worker: usize) -> usize {
    total / workers + usize::from(worker < total % workers)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_usize_allow_zero(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name).map_or(default, |value| {
        !matches!(value.as_str(), "0" | "false" | "no" | "off")
    })
}

fn append_report(line: &str) {
    let Some(path) = env::var_os("TRINE_MATURITY_REPORT").map(PathBuf::from) else {
        return;
    };
    let mut report = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("production maturity report opens");
    writeln!(report, "{line}").expect("production maturity report appends");
}

fn temp_db_path(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    env::temp_dir().join(format!("trine-kv-{name}-{}-{nonce}", std::process::id()))
}

struct TestRng(u64);

impl TestRng {
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }

    fn usize(&mut self, upper: usize) -> usize {
        debug_assert!(upper > 0);
        usize::try_from(self.next() % upper as u64).expect("random index fits usize")
    }
}
