//! Durable content-upload lifecycle.
//!
//! The public lifecycle is intentionally split by responsibility: session
//! creation/resume, sealing, abort/cleanup, quota accounting, and operator
//! maintenance. Each operation remains an inherent [`Db`](super::Db) method,
//! while transition-specific helpers stay inside this module boundary.

mod abort;
mod maintenance;
mod quota;
mod seal;
mod session;
