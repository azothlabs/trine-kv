//! Read futures, target bounds, and owned read completions.

use std::{future::Future, pin::Pin, sync::Arc};

use bytes::Bytes;

use crate::error::Result;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) type StorageFuture<'op, T> = Pin<Box<dyn Future<Output = Result<T>> + 'op>>;

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) type StorageFuture<'op, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'op>>;

pub(crate) type StorageReadFuture<'op, T> = StorageFuture<'op, T>;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) trait StorageThreadBound {}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl<T: ?Sized> StorageThreadBound for T {}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) trait StorageThreadBound: Send {}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl<T: Send + ?Sized> StorageThreadBound for T {}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) trait StorageSharedBound {}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl<T: ?Sized> StorageSharedBound for T {}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) trait StorageSharedBound: Send + Sync {}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl<T: Send + Sync + ?Sized> StorageSharedBound for T {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageReadBuffer {
    offset: usize,
    bytes: Bytes,
}

impl StorageReadBuffer {
    fn new(offset: usize, bytes: Bytes) -> Self {
        Self { offset, bytes }
    }

    /// Wraps an owned byte vector as a read completion. Used by block decode
    /// fallbacks that read into a heap buffer before handing it to the decoder.
    pub(crate) fn from_vec(offset: usize, bytes: Vec<u8>) -> Self {
        Self::new(offset, Bytes::from(bytes))
    }

    pub(crate) const fn offset(&self) -> usize {
        self.offset
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn into_bytes(self) -> Bytes {
        self.bytes
    }

    pub(crate) fn into_arc_bytes(self) -> Arc<[u8]> {
        Arc::from(self.bytes.as_ref())
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}
