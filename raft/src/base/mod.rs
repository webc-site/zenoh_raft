//! Basic types and traits with optional feature support.
//!
//! This module provides foundational traits that adapt based on feature flags,
//! allowing Openraft to work in both multi-threaded and single-threaded environments.
//!
//! ## Key Traits
//!
//! - [`OptionalSend`] - `Send` when not `single-threaded`, empty otherwise
//! - [`OptionalSync`] - `Sync` when not `single-threaded`, empty otherwise
//! - [`OptionalSerde`] - Serde traits when `serde` feature enabled
//! - [`OptionalFeatures`] - Combines all optional traits
//!
//! ## Type Aliases
//!
//! - [`BoxFuture`] - Boxed future, optionally `Send`
//! - [`BoxAsyncOnceMut`] - Boxed async FnOnce with mutable access
//! - [`BoxOnce`] - Boxed FnOnce closure
//! - [`BoxAny`] - Boxed Any type
//!
//! ## Overview
//!
//! These types allow Openraft to be used in:
//! - **Multi-threaded** contexts (default): Types are `Send` + `Sync`
//! - **Single-threaded** contexts (feature `single-threaded`): No `Send` + `Sync` bounds
//! - **With/without serde** (feature `serde`): Optional serialization support
//!
//! Applications rarely need to use these types directly - they're used internally
//! to make Openraft flexible across different environments.

pub(crate) mod finalized;
pub(crate) mod shared_id_generator;

pub use crate::async_runtime::BoxAny;
pub use crate::async_runtime::BoxAsyncOnceMut;
pub use crate::async_runtime::BoxFuture;
pub use crate::async_runtime::BoxIterator;
pub use crate::async_runtime::BoxMaybeAsyncOnceMut;
pub use crate::async_runtime::BoxOnce;
pub use crate::async_runtime::BoxStream;
pub use crate::async_runtime::OptionalSend;
pub use crate::async_runtime::OptionalSync;

/// A trait that combines foundational traits.
pub trait OptionalFeatures: OptionalSend + OptionalSync + Unpin {}

impl<T> OptionalFeatures for T where T: OptionalSend + OptionalSync + Unpin {}
