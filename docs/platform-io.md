# Platform I/O And Feature Selection

Use this page when you need to answer three practical questions:

- Which Cargo feature should I enable?
- How do I make a database use the platform I/O path?
- How do I verify which operation class actually ran?

`platform-io` is Trine's portable async storage boundary. The KV engine asks
for complete Trine storage operations such as random read, WAL append, table
publish, directory sync, and object delete. Platform-io decides how the selected
target completes those operations.

## Feature Choices

| feature | use when | dependency shape | completion class |
| --- | --- | --- | --- |
| none | You want the normal async API without the platform I/O driver. | No platform I/O driver dependencies. | Runtime/storage defaults. |
| `platform-io` | You want portable async completion for native-file storage without native async backend crates. | Adds Trine's bounded thread-pool backend. | `ThreadPoolManagedAsync` on native-thread targets. |
| `platform-io-threadpool` | You want to name the thread-pool baseline explicitly. | Alias for `platform-io`. | Same as `platform-io`. |
| `platform-io-native` | You want native async where Trine has audited support, with thread-pool fallback for the rest. | Adds native backend crates plus the baseline thread pool. | `TruePlatformAsync`, `PlatformNativeAsyncButPartial`, or `ThreadPoolManagedAsync` by operation. |
| `s3` | You want the `object_store` crate backed `ObjectClient` helper. | Adds object-store client dependencies. | Separate object-store async path. |

Enable the portable baseline:

```toml
[dependencies]
trine-kv = { version = "0.3", features = ["platform-io"] }
```

Enable native-first platform I/O:

```toml
[dependencies]
trine-kv = { version = "0.3", features = ["platform-io-native"] }
```

The feature only makes the driver available. Select it for a database with
`RuntimeOptions::platform_io()`:

```rust
use trine_kv::{Db, DbOptions, RuntimeOptions};

let mut options = DbOptions::new("./trine-data");
options.runtime = RuntimeOptions::platform_io();

let db = Db::open(options).await?;
db.put(b"k", b"v").await?;
db.flush().await?;
```

Run the checked example:

```text
cargo run --example platform_io --features platform-io
cargo run --example platform_io --features platform-io-native
```

## What The Classes Mean

`TruePlatformAsync` means the complete Trine operation is submitted through a
native platform completion path.

`PlatformNativeAsyncButPartial` means one or more lower-level steps use native
async completion, but the complete Trine operation still includes helper-managed
steps. This is still an async completion to the caller; it is not an engine
fallback.

`ThreadPoolManagedAsync` means platform-io owns the operation and completes
blocking native-file work on Trine's bounded thread pool. This is the portable
baseline for native-thread targets.

`BlockingFallback` means platform-io had to run a synchronous fallback inside
the selected driver. New code should treat this as diagnostic evidence, not as
the goal.

`Unsupported` means the target cannot honestly provide the operation through
that platform I/O class.

## Current Native Matrix

With `platform-io`, native-thread targets use `ThreadPoolManagedAsync` for the
current native-file operation set. With `platform-io-native`, Linux has the
strongest true native coverage, while Windows, macOS, FreeBSD, and
Solaris-family targets use partial native rows where the native backend covers
only part of the complete operation.

```text
Operation                     Linux            Windows          macOS            BSD/Solaris      Generic fallback
length lookup                 TruePlatformAsync ThreadPool       ThreadPool       ThreadPool       ThreadPool
random read                   TruePlatformAsync Partial          Partial          Partial          ThreadPool
whole-object read             TruePlatformAsync Partial          Partial          Partial          ThreadPool
temp write + rename publish   TruePlatformAsync Partial          Partial          Partial          ThreadPool
append open                   TruePlatformAsync ThreadPool       Partial          ThreadPool       ThreadPool
append                        TruePlatformAsync Partial          Partial          Partial          ThreadPool
persist/fsync                 TruePlatformAsync ThreadPool       Partial          Partial          ThreadPool
WAL rewrite                   TruePlatformAsync Partial          Partial          Partial          ThreadPool
delete                        TruePlatformAsync ThreadPool       ThreadPool       ThreadPool       ThreadPool
directory create              TruePlatformAsync ThreadPool       ThreadPool       ThreadPool       ThreadPool
directory sync                TruePlatformAsync ThreadPool       Partial          Partial          ThreadPool
directory listing             ThreadPool        ThreadPool       ThreadPool       ThreadPool       ThreadPool
writer lease                  TruePlatformAsync Partial          Partial          Partial          ThreadPool/Unsupported
```

Legend: `Partial` means `PlatformNativeAsyncButPartial`, and `ThreadPool` means
`ThreadPoolManagedAsync`.

This table is operation-level. It does not say that an OS has or lacks a
specific primitive; it says whether Trine can prove the complete storage
operation is true native async, partial native async, thread-pool managed, or
unsupported.

## Verify In Your Application

Use `DbStats` after running a workload:

```rust
let stats = db.stats();
let total = stats.storage_platform_io_operations.total();

if stats.storage_uses_platform_io_driver {
    println!("platform I/O completions: {}", total.total());
    println!("true native async: {}", total.true_platform_async);
    println!(
        "partial native async: {}",
        total.platform_native_async_but_partial,
    );
    println!("thread-pool managed: {}", total.thread_pool_managed_async);
}
```

For operation-specific diagnostics:

```rust
let ops = db.stats().storage_platform_io_operations;

if ops.wal_rewrite.uses_non_true_platform_async() {
    println!("WAL rewrite used partial/threadpool/fallback work");
}
```

## Target Boundaries

Browser-style `wasm32-unknown-unknown` builds can enable `platform-io` or
`platform-io-native` without pulling native-only dependencies, but they do not
advertise native-thread platform I/O. Use the browser persistent backend and
the async API there.

WASI persistent storage uses the host-preopened filesystem boundary and does
not currently advertise platform async I/O.

Object-store databases use their own async object-storage path. The `s3`
feature is unrelated to native platform I/O.
