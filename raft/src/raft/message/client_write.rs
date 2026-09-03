use std::{fmt, fmt::Debug};

use display_more::DisplayOptionExt;

use crate::{Membership, RaftTypeConfig, errors::ClientWriteError, type_config::alias::LogIdOf};

/// The result of a write request to Raft.
pub type ClientWriteResult<C> = Result<ClientWriteResponse<C>, ClientWriteError<C>>;

/// The response to a client-request.
#[derive(Clone, PartialEq, Eq)]
pub struct ClientWriteResponse<C: RaftTypeConfig> {
  /// The id of the log that is applied.
  pub log_id: LogIdOf<C>,

  /// Application specific response data.
  pub data: C::R,

  /// If the log entry is a change-membership entry.
  pub membership: Option<Membership<C::NodeId, C::Node>>,
}

impl<C> ClientWriteResponse<C>
where
  C: RaftTypeConfig,
{
  pub fn log_id(&self) -> &LogIdOf<C> {
    &self.log_id
  }

  pub fn response(&self) -> &C::R {
    &self.data
  }

  /// Return membership config if the log entry is a change-membership entry.
  pub fn membership(&self) -> &Option<Membership<C::NodeId, C::Node>> {
    &self.membership
  }
}

impl<C: RaftTypeConfig> Debug for ClientWriteResponse<C>
where
  C::R: Debug,
{
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ClientWriteResponse")
      .field("log_id", &self.log_id)
      .field("data", &self.data)
      .field("membership", &self.membership)
      .finish()
  }
}

impl<C> fmt::Display for ClientWriteResponse<C>
where
  C: RaftTypeConfig,
{
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "ClientWriteResponse{{log_id:{}, membership:{}}}",
      self.log_id,
      self.membership.display()
    )
  }
}
