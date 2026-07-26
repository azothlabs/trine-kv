use std::{
    io,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::{
    error::{Error, Result},
    storage::StorageObjectKind,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageFaultPoint {
    WalAppend,
    WalAppendPartial,
    WalPersist,
    ObjectPublish,
    ManifestPublish,
    ManifestDirectorySync,
    WalRewritePublish,
    DirectorySync,
    ObjectDelete,
}

#[derive(Debug)]
struct StorageFault {
    root: PathBuf,
    point: StorageFaultPoint,
    kind: Option<StorageObjectKind>,
    fail_on_call: usize,
    calls: AtomicUsize,
}

#[derive(Debug)]
pub(crate) struct StorageFaultGuard {
    fault: Arc<StorageFault>,
}

impl StorageFaultGuard {
    pub(crate) fn install(
        root: impl Into<PathBuf>,
        point: StorageFaultPoint,
        kind: Option<StorageObjectKind>,
        fail_on_call: usize,
    ) -> Self {
        assert!(fail_on_call > 0, "fault call index must be non-zero");
        let fault = Arc::new(StorageFault {
            root: root.into(),
            point,
            kind,
            fail_on_call,
            calls: AtomicUsize::new(0),
        });
        fault_registry()
            .lock()
            .expect("storage fault registry lock")
            .push(Arc::downgrade(&fault));
        Self { fault }
    }

    pub(crate) fn calls(&self) -> usize {
        self.fault.calls.load(Ordering::Acquire)
    }
}

pub(crate) fn check(
    point: StorageFaultPoint,
    kind: Option<StorageObjectKind>,
    path: &Path,
) -> Result<()> {
    let faults = {
        let mut registry = fault_registry()
            .lock()
            .expect("storage fault registry lock");
        registry.retain(|fault| fault.strong_count() > 0);
        registry
            .iter()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>()
    };

    for fault in faults {
        if fault.point != point || fault.kind != kind || !path.starts_with(&fault.root) {
            continue;
        }
        let call = fault.calls.fetch_add(1, Ordering::AcqRel) + 1;
        if call == fault.fail_on_call {
            return Err(Error::Io(io::Error::other(format!(
                "injected storage fault at {point:?} for {}",
                path.display()
            ))));
        }
    }
    Ok(())
}

fn fault_registry() -> &'static Mutex<Vec<Weak<StorageFault>>> {
    static REGISTRY: OnceLock<Mutex<Vec<Weak<StorageFault>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}
