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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageFaultPhase {
    Before,
    After,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StorageFaultBoundary {
    pub(crate) point: StorageFaultPoint,
    pub(crate) phase: StorageFaultPhase,
}

impl StorageFaultBoundary {
    pub(crate) const ALL: [Self; 17] = [
        Self::before(StorageFaultPoint::WalAppend),
        Self::before(StorageFaultPoint::WalAppendPartial),
        Self::before(StorageFaultPoint::WalPersist),
        Self::before(StorageFaultPoint::ObjectPublish),
        Self::before(StorageFaultPoint::ManifestPublish),
        Self::before(StorageFaultPoint::ManifestDirectorySync),
        Self::before(StorageFaultPoint::WalRewritePublish),
        Self::before(StorageFaultPoint::DirectorySync),
        Self::before(StorageFaultPoint::ObjectDelete),
        Self::after(StorageFaultPoint::WalAppend),
        Self::after(StorageFaultPoint::WalPersist),
        Self::after(StorageFaultPoint::ObjectPublish),
        Self::after(StorageFaultPoint::ManifestPublish),
        Self::after(StorageFaultPoint::ManifestDirectorySync),
        Self::after(StorageFaultPoint::WalRewritePublish),
        Self::after(StorageFaultPoint::DirectorySync),
        Self::after(StorageFaultPoint::ObjectDelete),
    ];

    const fn before(point: StorageFaultPoint) -> Self {
        Self {
            point,
            phase: StorageFaultPhase::Before,
        }
    }

    const fn after(point: StorageFaultPoint) -> Self {
        Self {
            point,
            phase: StorageFaultPhase::After,
        }
    }

    fn is_supported(point: StorageFaultPoint, phase: StorageFaultPhase) -> bool {
        Self::ALL
            .iter()
            .any(|boundary| boundary.point == point && boundary.phase == phase)
    }
}

#[derive(Debug)]
struct StorageFault {
    root: PathBuf,
    point: StorageFaultPoint,
    phase: StorageFaultPhase,
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
        Self::install_at(root, point, StorageFaultPhase::Before, kind, fail_on_call)
    }

    pub(crate) fn install_at(
        root: impl Into<PathBuf>,
        point: StorageFaultPoint,
        phase: StorageFaultPhase,
        kind: Option<StorageObjectKind>,
        fail_on_call: usize,
    ) -> Self {
        assert!(fail_on_call > 0, "fault call index must be non-zero");
        assert!(
            StorageFaultBoundary::is_supported(point, phase),
            "unsupported storage fault boundary {point:?}/{phase:?}"
        );
        let fault = Arc::new(StorageFault {
            root: root.into(),
            point,
            phase,
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
    check_at(point, StorageFaultPhase::Before, kind, path)
}

pub(crate) fn check_after(
    point: StorageFaultPoint,
    kind: Option<StorageObjectKind>,
    path: &Path,
) -> Result<()> {
    check_at(point, StorageFaultPhase::After, kind, path)
}

fn check_at(
    point: StorageFaultPoint,
    phase: StorageFaultPhase,
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
        if fault.point != point
            || fault.phase != phase
            || fault.kind != kind
            || !path.starts_with(&fault.root)
        {
            continue;
        }
        let call = fault.calls.fetch_add(1, Ordering::AcqRel) + 1;
        if call == fault.fail_on_call {
            return Err(Error::Io(io::Error::other(format!(
                "injected storage fault at {point:?}/{phase:?} for {}",
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

#[cfg(test)]
mod tests {
    use super::{StorageFaultBoundary, StorageFaultPhase, StorageFaultPoint};

    #[test]
    fn fault_boundary_catalog_is_unique_and_covers_each_operation() {
        for (index, boundary) in StorageFaultBoundary::ALL.iter().enumerate() {
            assert!(
                !StorageFaultBoundary::ALL[..index].contains(boundary),
                "duplicate fault boundary {boundary:?}"
            );
        }

        let operations = [
            StorageFaultPoint::WalAppend,
            StorageFaultPoint::WalAppendPartial,
            StorageFaultPoint::WalPersist,
            StorageFaultPoint::ObjectPublish,
            StorageFaultPoint::ManifestPublish,
            StorageFaultPoint::ManifestDirectorySync,
            StorageFaultPoint::WalRewritePublish,
            StorageFaultPoint::DirectorySync,
            StorageFaultPoint::ObjectDelete,
        ];
        for point in operations {
            assert!(StorageFaultBoundary::is_supported(
                point,
                StorageFaultPhase::Before
            ));
            if point != StorageFaultPoint::WalAppendPartial {
                assert!(StorageFaultBoundary::is_supported(
                    point,
                    StorageFaultPhase::After
                ));
            }
        }
    }
}
