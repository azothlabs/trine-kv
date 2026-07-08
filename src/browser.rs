//! Browser storage helpers for OPFS-backed persistent databases.
//!
//! This module is available on `wasm32-unknown-unknown` browser builds. It
//! exposes the browser `StorageManager` operations Trine users need before
//! putting durable data in origin-private storage: quota estimates, persisted
//! status, and a request path for persistent storage.

#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]

use std::io;

use js_sys::{Function, Promise, Reflect};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;

use crate::{Error, Result};

/// Browser-origin storage usage and quota reported by `navigator.storage`.
///
/// `usage_bytes` is the browser's best estimate of bytes already charged to
/// the current origin. `quota_bytes` is the browser's best estimate of the
/// current origin limit. Browsers may omit either number, round values, or
/// change the quota as device state changes, so callers should treat this as
/// guidance for admission control and user messaging rather than as a hard
/// reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrowserStorageEstimate {
    /// Estimated bytes already used by the current origin, when the browser
    /// reports it.
    pub usage_bytes: Option<u64>,
    /// Estimated bytes currently available to the current origin, when the
    /// browser reports it.
    pub quota_bytes: Option<u64>,
}

/// Returns the browser's current storage usage and quota estimate.
///
/// This calls `navigator.storage.estimate()` for the current origin. The
/// result can be used before opening [`DbOptions::browser_persistent_at`] to
/// decide whether the application should warn the user, flush data elsewhere,
/// or reduce local retention. The returned numbers are estimates: browsers can
/// round them for privacy and can change quota after this call.
///
/// # Errors
///
/// Returns [`Error::UnsupportedBackend`] when the browser does not expose
/// `navigator.storage.estimate()`. JavaScript exceptions from the browser
/// storage manager are returned as I/O errors with browser context.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> trine_kv::Result<()> {
/// let estimate = trine_kv::browser::browser_storage_estimate().await?;
/// if let (Some(usage), Some(quota)) = (estimate.usage_bytes, estimate.quota_bytes) {
///     if quota > 0 && usage.saturating_mul(100) / quota >= 80 {
///         // The application can shed cache entries or ask the user to free space.
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// [`DbOptions::browser_persistent_at`]: crate::DbOptions::browser_persistent_at
pub async fn browser_storage_estimate() -> Result<BrowserStorageEstimate> {
    let storage = browser_storage_manager()?;
    let estimate = storage_function(&storage, "estimate")?;
    let promise = estimate
        .call0(&storage)
        .map_err(|error| map_js_value_error(&error, "request browser storage estimate"))?
        .dyn_into::<Promise>()
        .map_err(|_| Error::unsupported_backend("browser storage estimate promise"))?;
    let value = JsFuture::from(promise)
        .await
        .map_err(|error| map_js_value_error(&error, "await browser storage estimate"))?;
    Ok(BrowserStorageEstimate {
        usage_bytes: reflect_optional_u64(&value, "usage")?,
        quota_bytes: reflect_optional_u64(&value, "quota")?,
    })
}

/// Returns whether the current origin is already marked as persistent.
///
/// This calls `navigator.storage.persisted()`. A `true` result means the
/// browser has granted the origin stronger protection from storage eviction.
/// A `false` result does not mean Trine cannot write; it means the browser may
/// still reclaim the origin's OPFS data under its normal storage pressure
/// policy.
///
/// # Errors
///
/// Returns [`Error::UnsupportedBackend`] when the browser does not expose
/// `navigator.storage.persisted()`. JavaScript exceptions are returned as I/O
/// errors with browser context.
pub async fn browser_persistent_storage_granted() -> Result<bool> {
    call_storage_bool_method("persisted").await
}

/// Requests persistent storage for the current origin.
///
/// This calls `navigator.storage.persist()`. Browsers decide whether to grant
/// the request based on user engagement, installed-app status, settings, and
/// implementation-specific policy. A `true` result means the origin is now
/// marked as persistent; a `false` result means Trine can still use OPFS, but
/// callers should treat the data as more vulnerable to browser eviction.
///
/// Call this from user-driven application setup before opening a browser
/// persistent database when local data loss would be expensive for the user.
///
/// # Errors
///
/// Returns [`Error::UnsupportedBackend`] when the browser does not expose
/// `navigator.storage.persist()`. JavaScript exceptions are returned as I/O
/// errors with browser context.
pub async fn request_browser_persistent_storage() -> Result<bool> {
    call_storage_bool_method("persist").await
}

async fn call_storage_bool_method(method: &'static str) -> Result<bool> {
    let storage = browser_storage_manager()?;
    let function = storage_function(&storage, method)?;
    let promise = function
        .call0(&storage)
        .map_err(|error| map_js_value_error(&error, "request browser storage status"))?
        .dyn_into::<Promise>()
        .map_err(|_| Error::unsupported_backend("browser storage status promise"))?;
    let value = JsFuture::from(promise)
        .await
        .map_err(|error| map_js_value_error(&error, "await browser storage status"))?;
    value
        .as_bool()
        .ok_or_else(|| Error::Io(io::Error::other("browser storage status was not a boolean")))
}

fn browser_storage_manager() -> Result<JsValue> {
    let navigator = Reflect::get(&js_sys::global(), &JsValue::from_str("navigator"))
        .map_err(|error| map_js_value_error(&error, "read browser navigator"))?;
    if navigator.is_null() || navigator.is_undefined() {
        return Err(Error::unsupported_backend("browser navigator"));
    }
    let storage = Reflect::get(&navigator, &JsValue::from_str("storage"))
        .map_err(|error| map_js_value_error(&error, "read browser storage manager"))?;
    if storage.is_null() || storage.is_undefined() {
        return Err(Error::unsupported_backend("browser storage manager"));
    }
    Ok(storage)
}

fn storage_function(storage: &JsValue, method: &'static str) -> Result<Function> {
    Reflect::get(storage, &JsValue::from_str(method))
        .map_err(|error| map_js_value_error(&error, "read browser storage function"))?
        .dyn_into::<Function>()
        .map_err(|_| Error::unsupported_backend("browser storage manager function"))
}

fn reflect_optional_u64(value: &JsValue, property: &'static str) -> Result<Option<u64>> {
    let value = Reflect::get(value, &JsValue::from_str(property))
        .map_err(|error| map_js_value_error(&error, "read browser storage estimate field"))?;
    if value.is_null() || value.is_undefined() {
        return Ok(None);
    }
    let Some(number) = value.as_f64() else {
        return Err(Error::Io(io::Error::other(format!(
            "browser storage estimate field {property} was not a number"
        ))));
    };
    if !number.is_finite() || number < 0.0 || number > u64::MAX as f64 {
        return Err(Error::Io(io::Error::other(format!(
            "browser storage estimate field {property} was out of range"
        ))));
    }
    Ok(Some(number as u64))
}

fn map_js_value_error(error: &JsValue, action: &'static str) -> Error {
    let message = js_value_property(error, "message")
        .or_else(|| js_value_property(error, "name"))
        .or_else(|| error.as_string())
        .unwrap_or_else(|| format!("{error:?}"));
    Error::Io(io::Error::other(format!(
        "browser storage manager failed to {action}: {message}"
    )))
}

fn js_value_property(value: &JsValue, property: &str) -> Option<String> {
    Reflect::get(value, &JsValue::from_str(property))
        .ok()
        .and_then(|value| value.as_string())
}
