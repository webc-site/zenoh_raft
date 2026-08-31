/// This helper trait extracts information from an `Option<LogIndex>`.
///
/// In openraft, `LogIndex` is a `u64`.
pub trait LogIndexOptionExt {
    /// Return the next log index.
    ///
    /// If self is `None`, it returns 0.
    fn next_index(&self) -> u64;

    /// Return the previous log index.
    ///
    /// If self is `None`, it panics.
    fn prev_index(&self) -> Self;
}

impl LogIndexOptionExt for Option<u64> {
    fn next_index(&self) -> u64 {
        match self {
            None => 0,
            Some(v) => v + 1,
        }
    }

    fn prev_index(&self) -> Self {
        self.and_then(|v| v.checked_sub(1))
    }
}
