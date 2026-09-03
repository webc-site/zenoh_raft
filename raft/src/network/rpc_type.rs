use std::fmt;

/// Types of RPC requests in the Raft protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RPCTypes {
  /// Vote request RPC.
  Vote,
  /// AppendEntries request RPC.
  AppendEntries,
  /// InstallSnapshot request RPC.
  InstallSnapshot,
  /// TransferLeader request RPC.
  TransferLeader,
}

impl fmt::Display for RPCTypes {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{:?}", self)
  }
}
