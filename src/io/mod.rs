use std::{
    future::Future,
    panic::{self, AssertUnwindSafe},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

#[cfg(all(feature = "platform-io", any(unix, windows)))]
use std::fs::File;
#[cfg(feature = "platform-io")]
use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering},
    thread,
};

#[cfg(feature = "platform-io")]
use crate::storage::NativeFileStorageMetrics;
use crate::{
    error::{Error, Result},
    options::DurabilityMode,
    runtime::Runtime,
    storage::StorageReadBuffer,
};

mod core;
#[cfg(feature = "platform-io")]
mod platform;

pub(crate) use core::*;
#[cfg(feature = "platform-io")]
pub(crate) use platform::*;

#[cfg(test)]
mod tests;
