use std::sync::Arc;

use crate::{
  RaftTypeConfig,
  errors::ReplicationClosed,
  replication::{inflight_append_queue::InflightAppendQueue, stream_state::StreamState},
  storage::RaftLogStorage,
  type_config::alias::MutexOf,
};

/// Context passed through the AppendEntries request stream.
///
/// This struct is used with `futures_util::stream::unfold` to generate
/// AppendEntries requests. It holds both the mutable state for reading
/// log entries and a queue for tracking in-flight requests.
pub(crate) struct StreamContext<C, LS>
where
  C: RaftTypeConfig,
  LS: RaftLogStorage<C>,
{
  /// Shared state for generating the next request.
  pub(crate) stream_state: Arc<MutexOf<C, StreamState<C, LS>>>,

  /// Tracks in-flight requests for RTT measurement.
  pub(crate) inflight_append_queue: InflightAppendQueue<C>,

  /// Fatal error found while generating the request stream.
  pub(crate) fatal_error: Arc<MutexOf<C, Option<ReplicationClosed>>>,
}
