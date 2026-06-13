# WAL Replay Writer-Lease Cost

Date: 2026-06-13

Command:

```text
cargo bench --bench v1_bench
```

## Question

The `0.4.0` release benchmark check showed `WAL replay` as the main row to
investigate. The first step was to determine whether the time was spent in WAL
read/decode/replay or in writable-open setup around replay.

## Diagnostic Rows

The benchmark now reports both the existing writable `WAL replay` row and a
read-only variant, plus reopen-only diagnostics for writable and read-only
opens. The diagnostic runs create a database with unflushed WAL records, then
measure only the reopen path 32 times.

Before the writer-lease change:

| row | value |
| --- | ---: |
| `WAL replay` | 35430 us |
| `WAL replay read-only` | 29501 us |
| writable reopen wall time, 32 runs | 106515 us |
| writable writer-lease latency, 32 runs | 88076 us |
| read-only reopen wall time, 32 runs | 17582 us |
| writable WAL object reads, 32 runs | 1664 us |

The WAL bytes were cheap to read. Writable reopen was dominated by writer-lease
acquisition, specifically syncing the human-readable owner text in the `LOCK`
file.

After changing writer-lease owner persistence from storage sync to ordinary
write/flush:

| row | value |
| --- | ---: |
| `WAL replay` | 31693 us |
| `WAL replay read-only` | 31706 us |
| writable reopen wall time, 32 runs | 18706 us |
| writable writer-lease latency, 32 runs | 1888 us |
| read-only reopen wall time, 32 runs | 15349 us |
| writable WAL object reads, 32 runs | 1520 us |

## Interpretation

The slowdown was not WAL replay. It was the writer-lease owner text being
`sync_all`ed on every writable open. The single-writer guarantee comes from
exclusive `create_new` creation of the `LOCK` file and the file's existence
while the database is open. The owner text is a drop-time guard and diagnostic
aid; syncing it to storage does not provide the parent-directory durability
needed for a crash-proof lease protocol, and it was much more expensive than
the WAL read/replay work.

The fix keeps the safety boundary intact:

- `create_new` still fails closed when a lock file already exists.
- The owner text is still written so `Drop` removes only the lock file created
  by this handle.
- The default native path, platform-io thread-pool path, and native platform I/O
  path now share the same cheaper owner-text semantics.

## Verification

- `cargo fmt --check`
- `cargo test -q writer_lease`
- `cargo test -q --features platform-io writer_lease`
- `cargo test -q --features platform-io-native writer_lease`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo bench --bench v1_bench`
