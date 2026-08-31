//! A container that stores elements efficiently by avoiding heap allocation for single elements.

mod batch_trait;
pub mod inline_batch;

pub use batch_trait::Batch;
pub use inline_batch::InlineBatch;
