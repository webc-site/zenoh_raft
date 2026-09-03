use std::{fmt, io};

use crate::{
  RaftTypeConfig,
  type_config::{
    TypeConfigExt,
    alias::{LogIdOf, SnapshotSignatureOf},
  },
};

/// Convert error to StorageError::IO();
pub trait ToStorageResult<C, T>
where
  C: RaftTypeConfig,
{
  /// Convert `Result<T, E>` to `Result<T, StorageError>`
  ///
  /// `f` provides error context for building the StorageError.
  fn sto_res<F>(self, f: F) -> Result<T, StorageError<C>>
  where
    F: FnOnce() -> (ErrorSubject<C>, ErrorVerb);
}

impl<C, T> ToStorageResult<C, T> for Result<T, io::Error>
where
  C: RaftTypeConfig,
{
  fn sto_res<F>(self, f: F) -> Result<T, StorageError<C>>
  where
    F: FnOnce() -> (ErrorSubject<C>, ErrorVerb),
  {
    match self {
      Ok(x) => Ok(x),
      Err(e) => {
        let (subject, verb) = f();
        let io_err = StorageError::new(subject, verb, C::err_from_error(&e));
        Err(io_err)
      }
    }
  }
}

/// The subject of a storage error, indicating what operation or component failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorSubject<C>
where
  C: RaftTypeConfig,
{
  /// A general storage error
  Store,

  /// HardState related error.
  Vote,

  /// Error that happened when operating a series of log entries
  Logs,

  /// Error about a single log entry
  Log(LogIdOf<C>),

  /// Error about a single log entry without knowing the log term.
  LogIndex(u64),

  /// Error happened when applying a log entry
  Apply(LogIdOf<C>),

  /// Error that happened when operating state machine.
  StateMachine,

  /// Error that happened when operating snapshots.
  Snapshot(Option<SnapshotSignatureOf<C>>),

  /// No specific subject for this error.
  None,
}

/// What it is doing when an error occurs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, bitcode::Encode, bitcode::Decode)]
pub enum ErrorVerb {
  /// Reading data.
  Read,
  /// Writing data.
  Write,
  /// Seeking in data.
  Seek,
  /// Deleting data.
  Delete,
}

impl fmt::Display for ErrorVerb {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{:?}", self)
  }
}

impl<C> From<StorageError<C>> for io::Error
where
  C: RaftTypeConfig,
{
  fn from(e: StorageError<C>) -> Self {
    io::Error::other(e.to_string())
  }
}

/// Error that occurs when operating the store.
///
/// It indicates a data crash.
/// An application returning this error will shut down the Openraft node immediately to prevent
/// further damage.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub struct StorageError<C>
where
  C: RaftTypeConfig,
{
  subject: ErrorSubject<C>,
  verb: ErrorVerb,
  source: C::ErrorSource,
}

impl<C> fmt::Display for StorageError<C>
where
  C: RaftTypeConfig,
{
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "when {:?} {:?}: {}",
      self.verb, self.subject, self.source
    )
  }
}

impl<C> StorageError<C>
where
  C: RaftTypeConfig,
{
  /// Create a new StorageError.
  pub fn new(subject: ErrorSubject<C>, verb: ErrorVerb, source: C::ErrorSource) -> Self {
    Self {
      subject,
      verb,
      source,
    }
  }

  /// Create a StorageError from a Error.
  pub fn from_io_error(subject: ErrorSubject<C>, verb: ErrorVerb, io_error: io::Error) -> Self {
    StorageError::new(subject, verb, C::err_from_error(&io_error))
  }

  /// Create an error for writing a log entry.
  pub fn write_log_entry(log_id: LogIdOf<C>, source: C::ErrorSource) -> Self {
    Self::new(ErrorSubject::Log(log_id), ErrorVerb::Write, source)
  }

  /// Create an error for reading a log entry at an index.
  pub fn read_log_at_index(log_index: u64, source: C::ErrorSource) -> Self {
    Self::new(ErrorSubject::LogIndex(log_index), ErrorVerb::Read, source)
  }

  /// Create an error for reading a log entry.
  pub fn read_log_entry(log_id: LogIdOf<C>, source: C::ErrorSource) -> Self {
    Self::new(ErrorSubject::Log(log_id), ErrorVerb::Read, source)
  }

  /// Create an error for writing multiple log entries.
  pub fn write_logs(source: C::ErrorSource) -> Self {
    Self::new(ErrorSubject::Logs, ErrorVerb::Write, source)
  }

  /// Create an error for reading multiple log entries.
  pub fn read_logs(source: C::ErrorSource) -> Self {
    Self::new(ErrorSubject::Logs, ErrorVerb::Read, source)
  }

  /// Create an error for writing vote state.
  pub fn write_vote(source: C::ErrorSource) -> Self {
    Self::new(ErrorSubject::Vote, ErrorVerb::Write, source)
  }

  /// Create an error for reading vote state.
  pub fn read_vote(source: C::ErrorSource) -> Self {
    Self::new(ErrorSubject::Vote, ErrorVerb::Read, source)
  }

  /// Create an error for applying a log entry to the state machine.
  pub fn apply(log_id: LogIdOf<C>, source: C::ErrorSource) -> Self {
    Self::new(ErrorSubject::Apply(log_id), ErrorVerb::Write, source)
  }

  /// Create an error for writing to the state machine.
  pub fn write_state_machine(source: C::ErrorSource) -> Self {
    Self::new(ErrorSubject::StateMachine, ErrorVerb::Write, source)
  }

  /// Create an error for reading from the state machine.
  pub fn read_state_machine(source: C::ErrorSource) -> Self {
    Self::new(ErrorSubject::StateMachine, ErrorVerb::Read, source)
  }

  /// Create an error for writing a snapshot.
  pub fn write_snapshot(signature: Option<SnapshotSignatureOf<C>>, source: C::ErrorSource) -> Self {
    Self::new(ErrorSubject::Snapshot(signature), ErrorVerb::Write, source)
  }

  /// Create an error for reading a snapshot.
  pub fn read_snapshot(signature: Option<SnapshotSignatureOf<C>>, source: C::ErrorSource) -> Self {
    Self::new(ErrorSubject::Snapshot(signature), ErrorVerb::Read, source)
  }

  /// General read error
  pub fn read(source: C::ErrorSource) -> Self {
    Self::new(ErrorSubject::Store, ErrorVerb::Read, source)
  }

  /// General write error
  pub fn write(source: C::ErrorSource) -> Self {
    Self::new(ErrorSubject::Store, ErrorVerb::Write, source)
  }
}

#[cfg(test)]
mod tests {
  use anyerror::AnyError;

  #[test]
  fn test_storage_error_to_io_error() {
    use std::{io, io::ErrorKind};

    use super::StorageError;
    use crate::engine::testing::{UTConfig, log_id};

    let storage_err: StorageError<UTConfig> =
      StorageError::write_log_entry(log_id(1, 2, 3), AnyError::error("disk full"));
    let io_err: io::Error = storage_err.into();

    assert_eq!(io_err.kind(), ErrorKind::Other);
    assert!(io_err.to_string().contains("Write"));
    assert!(io_err.to_string().contains("disk full"));

    let storage_err: StorageError<UTConfig> =
      StorageError::read_vote(AnyError::error("permission denied"));
    let io_err: io::Error = storage_err.into();

    assert_eq!(io_err.kind(), ErrorKind::Other);
    assert!(io_err.to_string().contains("Read"));
    assert!(io_err.to_string().contains("Vote"));
    assert!(io_err.to_string().contains("permission denied"));
  }
}
