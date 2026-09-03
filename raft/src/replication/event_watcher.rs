use crate::{
  RaftTypeConfig, async_runtime::WatchReceiver, raft_state::IOId,
  replication::replicate::Replicate, type_config::alias::LogIdOf,
};

#[derive(Clone)]
pub(crate) struct EventWatcher<C>
where
  C: RaftTypeConfig,
{
  pub(crate) replicate_rx: WatchReceiver<Replicate<C>>,
  pub(crate) committed_rx: WatchReceiver<Option<LogIdOf<C>>>,

  pub(crate) io_accepted_rx: WatchReceiver<IOId<C>>,
  pub(crate) io_submitted_rx: WatchReceiver<IOId<C>>,
}
