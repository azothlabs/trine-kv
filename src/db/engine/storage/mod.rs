//! Public persistence and maintenance entry-point orchestration.

mod compact;
mod flush;
mod maintenance;
mod persist;
mod sync_support;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod browser;
