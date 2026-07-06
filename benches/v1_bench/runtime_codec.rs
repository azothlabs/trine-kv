use super::*;

pub(super) fn bench_runtime_block_decode_reads() -> Vec<BenchResult> {
    vec![
        bench_runtime_block_decode_read(
            "native runtime block decode read",
            "native-runtime-block-decode",
            RuntimeOptions::native_threads(),
        ),
        bench_runtime_block_decode_read(
            "inline runtime block decode read",
            "inline-runtime-block-decode",
            RuntimeOptions::inline(),
        ),
    ]
}

pub(super) fn bench_runtime_block_decode_read(
    name: &'static str,
    dir_name: &str,
    runtime: RuntimeOptions,
) -> BenchResult {
    let dir = temp_dir(dir_name);
    let mut options = benchmark_persistent_options(&dir);
    options.runtime = runtime;
    options.block_cache_bytes = 0;
    if !runtime.capabilities().background_threads() {
        options.background_worker_count = 0;
    }
    options.default_bucket_options = BucketOptions {
        block_bytes: 512,
        ..BucketOptions::default()
    };
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    for index in 0..ROWS {
        bucket
            .put_sync(key(index), value(index))
            .expect("put succeeds");
    }
    db.flush_sync().expect("flush succeeds");

    let result = measure(name, OPS, || {
        let mut checksum = 0_u64;
        let mut seed = 0xa51c_f00d_u64;
        for _ in 0..OPS {
            seed = xorshift(seed);
            let index = seed_index(seed, ROWS);
            checksum = checksum.saturating_add(
                bucket
                    .get_sync(&key(index))
                    .expect("get succeeds")
                    .map_or(0, |value| value.len() as u64),
            );
        }

        let stats = db.stats();
        assert!(
            stats.read_path.point_data_block_reads >= OPS as u64,
            "benchmark must exercise table data-block reads"
        );
        assert_eq!(
            stats.block_cache_hits, 0,
            "benchmark disables the block cache to force decode reads"
        );
        assert!(
            stats.block_cache_misses >= OPS as u64,
            "benchmark must miss the disabled cache before loading blocks"
        );
        checksum
            .saturating_add(stats.read_path.point_data_block_reads)
            .saturating_add(stats.block_cache_misses)
    });
    drop(db);
    cleanup_dir(&dir);
    result
}

pub(super) fn bench_index_seek_policies() -> Vec<BenchResult> {
    let mut results = Vec::new();
    for (size, label) in [(64, "small"), (1_024, "medium"), (8_192, "large")] {
        for (policy, policy_label) in [
            (IndexSearchPolicy::Linear, "linear"),
            (IndexSearchPolicy::Binary, "binary"),
            (IndexSearchPolicy::Auto, "auto"),
        ] {
            results.push(bench_index_seek_policy(size, label, policy, policy_label));
        }
    }
    results
}

pub(super) fn bench_index_seek_policy(
    size: usize,
    size_label: &'static str,
    policy: IndexSearchPolicy,
    policy_label: &'static str,
) -> BenchResult {
    let bucket_options = BucketOptions {
        index_search_policy: policy,
        // Smaller blocks create enough block-index entries for this tiny
        // harness to exercise the configured lookup policy.
        block_bytes: 512,
        ..BucketOptions::default()
    };
    let (dir, db, bucket) = flushed_persistent_db(
        &format!("index-{policy_label}-{size_label}"),
        size,
        bucket_options,
    );
    let result = measure(
        labelled3("index seek policy", policy_label, size_label),
        OPS,
        || {
            let mut checksum = 0;
            for index in 0..OPS {
                let row = (index * 17) % size;
                checksum += bucket
                    .get_sync(&key(row))
                    .expect("get succeeds")
                    .map_or(0, |value| value.len() as u64);
            }
            black_box(policy);
            checksum
        },
    );
    drop(db);
    cleanup_dir(&dir);
    result
}

pub(super) fn bench_long_shared_prefix_get() -> BenchResult {
    let dir = temp_dir("long-shared-prefix");
    let bucket_options = BucketOptions {
        block_bytes: 512,
        ..BucketOptions::default()
    };
    let mut options = benchmark_persistent_options(&dir);
    options.default_bucket_options = bucket_options;
    let db = Db::open_sync(options).expect("persistent db opens");
    let bucket = db.default_bucket_sync().expect("bucket opens");
    let keys = (0..ROWS).map(long_shared_prefix_key).collect::<Vec<_>>();

    for (index, key) in keys.iter().enumerate() {
        bucket
            .put_sync(key.as_slice(), value(index))
            .expect("put succeeds");
    }
    db.flush_sync().expect("flush succeeds");

    let result = measure("long shared-prefix get", OPS, || {
        let mut checksum = 0;
        for index in 0..OPS {
            let row = (index * 17) % ROWS;
            checksum += bucket
                .get_sync(&keys[row])
                .expect("get succeeds")
                .map_or(0, |value| value.len() as u64);
        }
        black_box(&keys);
        checksum
    });
    drop(db);
    cleanup_dir(&dir);
    result
}

pub(super) fn bench_iterator_advance_to() -> Vec<BenchResult> {
    let items = (0..8192).map(|index| index * 2).collect::<Vec<usize>>();
    vec![
        measure("iterator advance_to near targets", OPS, || {
            let mut current = 0;
            let mut checksum = 0;
            for _ in 0..OPS {
                let target = items[current].saturating_add(2_usize);
                current = search::advance_to(&items, current, &target).unwrap_or(current);
                checksum += current as u64;
            }
            checksum
        }),
        measure("iterator advance_to far targets", OPS, || {
            let mut current = 0;
            let mut checksum = 0;
            for step in 0..OPS {
                let target = (step * 97) % (items.len() * 2);
                current = search::advance_to(&items, current, &target).unwrap_or(current);
                checksum += current as u64;
            }
            checksum
        }),
        measure("iterator advance_to random targets", OPS, || {
            let mut current = 0;
            let mut seed = 0xfeed_f00d_u64;
            let mut checksum = 0;
            for _ in 0..OPS {
                seed = xorshift(seed);
                let target = seed_index(seed, items.len() * 2);
                current = search::advance_to(&items, current, &target).unwrap_or(current);
                checksum += current as u64;
            }
            checksum
        }),
    ]
}

pub(super) fn bench_codec_comparison() -> Vec<BenchResult> {
    let data_block = repeated_bytes(b"data-block-", 4096);
    let index_block = repeated_bytes(b"index-block-", 2048);
    let tombstone_block = repeated_bytes(b"range-tombstone-", 2048);
    let mut results = Vec::new();
    for (label, bytes) in [
        ("Trine data blocks", data_block),
        ("Trine index blocks", index_block),
        ("Trine range tombstone blocks", tombstone_block),
    ] {
        results.push(bench_codec("codec none", label, CodecBench::None, &bytes));
        results.push(bench_codec_decode_only(
            "codec decode only none",
            label,
            CodecBench::None,
            &bytes,
        ));
        results.push(bench_codec(
            "codec fast block compression",
            label,
            CodecBench::FastLz4Block,
            &bytes,
        ));
        results.push(bench_codec_decode_only(
            "codec decode only fast block compression",
            label,
            CodecBench::FastLz4Block,
            &bytes,
        ));
    }
    results
}

#[derive(Debug, Clone, Copy)]
pub(super) enum CodecBench {
    None,
    FastLz4Block,
}

pub(super) fn bench_codec(
    name: &'static str,
    label: &'static str,
    codec: CodecBench,
    bytes: &[u8],
) -> BenchResult {
    measure(labelled(name, label), OPS, || {
        let mut checksum = 0;
        for _ in 0..OPS {
            let encoded = encode_bench_block(codec, bytes);
            let decoded = decode_bench_block(codec, &encoded, bytes.len());
            checksum += (encoded.len() + decoded.len()) as u64;
        }
        checksum
    })
}

pub(super) fn bench_codec_decode_only(
    name: &'static str,
    label: &'static str,
    codec: CodecBench,
    bytes: &[u8],
) -> BenchResult {
    let encoded = encode_bench_block(codec, bytes);
    measure(labelled(name, label), OPS, || {
        let mut checksum = 0;
        for _ in 0..OPS {
            let decoded = decode_bench_block(codec, &encoded, bytes.len());
            checksum += decoded.len() as u64;
        }
        checksum
    })
}

pub(super) fn encode_bench_block(codec: CodecBench, bytes: &[u8]) -> Vec<u8> {
    match codec {
        CodecBench::None => bytes.to_vec(),
        CodecBench::FastLz4Block => lz4_flex::block::compress(bytes),
    }
}

pub(super) fn decode_bench_block(
    codec: CodecBench,
    bytes: &[u8],
    uncompressed_len: usize,
) -> Vec<u8> {
    match codec {
        CodecBench::None => {
            assert_eq!(bytes.len(), uncompressed_len);
            bytes.to_vec()
        }
        CodecBench::FastLz4Block => {
            lz4_flex::block::decompress(bytes, uncompressed_len).expect("lz4 block decodes")
        }
    }
}
