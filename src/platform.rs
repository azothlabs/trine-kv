//! Small target-specific platform capabilities used by storage semantics.

use crate::{Error, Result};

/// Returns the host wall clock as Unix milliseconds.
///
/// Expiry coordinates are durable values, so WebAssembly uses the JavaScript
/// host clock instead of `std::time::SystemTime`, which is unsupported on
/// `wasm32-unknown-unknown`.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) fn now_unix_millis() -> Result<u64> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::Io(std::io::Error::other(error)))?;
    u64::try_from(duration.as_millis())
        .map_err(|_| Error::invalid_options("system time milliseconds exceed u64::MAX"))
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) fn now_unix_millis() -> Result<u64> {
    let millis = js_sys::Date::now();
    // ECMAScript TimeClip limits valid dates to 8.64e15 milliseconds, well
    // below u64::MAX, so the checked non-negative integer is safe to cast.
    if !millis.is_finite() || millis < 0.0 || millis.fract() != 0.0 {
        return Err(Error::invalid_options(
            "JavaScript host clock must be a non-negative integral Unix millisecond",
        ));
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    Ok(millis as u64)
}
