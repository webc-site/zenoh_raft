use std::fmt;
use std::fmt::Formatter;
use std::ops::Deref;
use std::time::Instant;

use crate::display_ext::DisplayInstantExt;

/// A wrapper for `Instant` that supports serialization and deserialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SerdeInstant<I = Instant> {
    inner: I,
}

impl<I> Deref for SerdeInstant<I> {
    type Target = I;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<I> From<I> for SerdeInstant<I> {
    fn from(inner: I) -> Self {
        Self { inner }
    }
}

impl<I: DisplayInstantExt> fmt::Display for SerdeInstant<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.inner.display().fmt(f)
    }
}

impl<I> SerdeInstant<I> {
    /// Create a new SerdeInstant wrapping the given Instant.
    pub fn new(inner: I) -> Self {
        Self { inner }
    }

    /// Extract the inner Instant value.
    pub fn into_inner(self) -> I {
        self.inner
    }
}
