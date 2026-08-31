//! State machine control handle

use crate::RaftTypeConfig;
use crate::async_runtime::MpscSender;
use crate::async_runtime::MpscWeakSender;
use crate::async_runtime::SendError;
use crate::core::sm;
use crate::storage::RaftStateMachine;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::JoinHandleOf;
use crate::type_config::alias::SmSnapshotOf;

/// State machine worker handle for sending command to it.
pub(crate) struct Handle<C, SM = ()>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
{
    pub(in crate::core::sm) cmd_tx: MpscSender<sm::Command<C, SM>>,

    pub(in crate::core::sm) _join_handle: JoinHandleOf<C, ()>,
}

impl<C, SM> Handle<C, SM>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
{
    pub(crate) async fn send(
        &mut self,
        cmd: sm::Command<C, SM>,
    ) -> Result<(), SendError<sm::Command<C, SM>>> {
        log::debug!("sending command to state machine worker: {:?}", cmd);
        self.cmd_tx.send(cmd).await
    }

    /// Create a weak sender for direct access to the SM command channel.
    ///
    /// It is weak because the [`Worker`] watches the close event of this channel for shutdown.
    ///
    /// [`Worker`]: sm::worker::Worker
    pub(crate) fn downgrade_sender(&self) -> MpscWeakSender<sm::Command<C, SM>> {
        MpscSender::<sm::Command<C, SM>>::downgrade(&self.cmd_tx)
    }

    /// Create a [`SnapshotReader`] to get the current snapshot from the state machine.
    pub(crate) fn new_snapshot_reader(&self) -> SnapshotReader<C, SM> {
        SnapshotReader::<C, SM> {
            cmd_tx: MpscSender::<sm::Command<C, SM>>::downgrade(&self.cmd_tx),
        }
    }
}

/// A handle for retrieving a snapshot from the state machine.
pub(crate) struct SnapshotReader<C, SM = ()>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
{
    /// Weak command sender to the state machine worker.
    ///
    /// It is weak because the [`Worker`] watches the close event of this channel for shutdown.
    ///
    /// [`Worker`]: sm::worker::Worker
    cmd_tx: MpscWeakSender<sm::Command<C, SM>>,
}

impl<C, SM> SnapshotReader<C, SM>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
{
    /// Get a snapshot from the state machine.
    ///
    /// If the state machine worker has shutdown, it will return an error.
    /// If there is no snapshot available, it will return `Ok(None)`.
    pub(crate) async fn get_snapshot(&self) -> Result<Option<SmSnapshotOf<C, SM>>, &'static str> {
        let (tx, rx) = C::oneshot();

        let cmd = sm::Command::<C, SM>::get_snapshot(tx);
        log::debug!("SnapshotReader sending command to sm::Worker: {:?}", cmd);

        let Some(cmd_tx) = MpscWeakSender::<sm::Command<C, SM>>::upgrade(&self.cmd_tx) else {
            log::info!("failed to upgrade cmd_tx, sm::Worker may have shutdown");
            return Err("failed to upgrade cmd_tx, sm::Worker may have shutdown");
        };

        // If fail to send command, cmd is dropped and tx will be dropped.
        cmd_tx.send(cmd).await.ok();

        let snapshot = match rx.await {
            Ok(x) => x,
            Err(_e) => {
                log::error!("failed to receive snapshot, sm::Worker may have shutdown");
                return Err("failed to receive snapshot, sm::Worker may have shutdown");
            }
        };

        Ok(snapshot)
    }
}
