use std::collections::BTreeMap;
use std::fmt;

use display_more::DisplayOptionExt;
use display_more::DisplaySliceExt;

use crate::ChangeMembers;
use crate::RaftState;
use crate::RaftTypeConfig;
use crate::base::BoxOnce;
use crate::core::raft_msg::external_command::ExternalCommand;
use crate::core::raft_msg::membership_payloads::MembershipPayloads;
use crate::display_ext::DisplayBTreeMapDebugValueExt;
use crate::errors::Infallible;
use crate::errors::InitializeError;
use crate::errors::LinearizableReadError;
use crate::impls::ProgressResponder;
use crate::raft::AppendEntriesRequest;
use crate::raft::ClientWriteResult;
use crate::raft::Precondition;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::raft::linearizable_read::Linearizer;
use crate::raft::linearizable_read::LinearizerOption;
use crate::raft::responder::core_responder::CoreResponder;
use crate::raft::stream_append::StreamAppendResult;
use crate::type_config::alias::BatchOf;
use crate::type_config::alias::CommittedLeaderIdOf;
use crate::type_config::alias::LogIdOf;
use crate::type_config::alias::OneshotSenderOf;
use crate::type_config::alias::PayloadOf;
use crate::type_config::alias::VoteOf;

pub(crate) mod external_command;
pub(crate) mod install_full_snapshot_request;
pub(crate) mod membership_payloads;

/// A oneshot TX to send result from `RaftCore` to external caller, e.g. `Raft::append_entries`.
pub(crate) type ResultSender<C, T, E = Infallible> = OneshotSenderOf<C, Result<T, E>>;

/// TX for Vote Response
pub(crate) type VoteTx<C> = OneshotSenderOf<C, VoteResponse<C>>;

/// TX for Append Entries Response
pub(crate) type AppendEntriesTx<C> = OneshotSenderOf<C, StreamAppendResult<C>>;

/// TX for Linearizable Read Response
pub(crate) type ClientReadTx<C> = ResultSender<C, Linearizer<C>, LinearizableReadError<C>>;

/// A message sent by application to the [`RaftCore`].
///
/// [`RaftCore`]: crate::core::RaftCore
pub(crate) enum RaftMsg<C>
where
    C: RaftTypeConfig,
{
    AppendEntries {
        rpc: AppendEntriesRequest<C>,
        tx: AppendEntriesTx<C>,
    },

    RequestVote {
        rpc: VoteRequest<C>,
        tx: VoteTx<C>,
    },

    /// A Pre-Vote request: probe whether a quorum would grant a vote without changing any state.
    RequestPreVote {
        rpc: VoteRequest<C>,
        tx: VoteTx<C>,
    },

    ClientWrite {
        payloads: BatchOf<C, PayloadOf<C>>,
        responders: BatchOf<C, Option<CoreResponder<C>>>,
        expected_leader: Option<CommittedLeaderIdOf<C>>,
    },

    GetLinearizer {
        linearizer_option: LinearizerOption,
        tx: ClientReadTx<C>,
    },

    Initialize {
        members: BTreeMap<C::NodeId, C::Node>,
        tx: ResultSender<C, (), InitializeError<C>>,
    },

    ChangeMembership {
        changes: ChangeMembers<C::NodeId, C::Node>,

        /// Payloads whose membership OpenRaft replaces with the computed membership.
        ///
        /// `RaftCore` computes the membership, so it also picks the payload that matches the
        /// shape of that membership.
        payloads: MembershipPayloads<C>,

        /// If `retain` is `true`, then the voters that are not in the new
        /// config will be converted into learners, otherwise they will be removed.
        retain: bool,

        preconditions: BatchOf<C, Precondition<C>>,

        tx: ProgressResponder<C, ClientWriteResult<C>>,
    },

    /// Append a caller-built membership as one log entry, with no intermediate joint membership.
    ///
    /// Unlike [`RaftMsg::ChangeMembership`], `RaftCore` does not compute the membership here. It
    /// reads back the membership the caller already bound into `payload`, validates that exact
    /// value, and appends `payload` unchanged.
    AppendMembership {
        /// The caller's payload, after [`RaftPayload::with_membership()`] bound the proposed
        /// membership into it.
        ///
        /// This payload is the single source of truth for the entry: `RaftCore` reads the
        /// membership to validate out of it, and writes this same value to the log. Carrying the
        /// membership in a second field would let the validated value and the written value drift
        /// apart.
        ///
        /// [`RaftPayload::with_membership()`]: crate::entry::RaftPayload::with_membership
        payload: C::Payload,

        preconditions: BatchOf<C, Precondition<C>>,

        tx: ProgressResponder<C, ClientWriteResult<C>>,
    },

    WithRaftState {
        req: BoxOnce<'static, RaftState<C>>,
    },

    /// Transfer Leader to another node.
    ///
    /// If this node is `to`, reset Leader lease and start election.
    /// Otherwise, just reset Leader lease so that the node `to` can become Leader.
    HandleTransferLeader {
        /// The vote of the Leader that is transferring the leadership.
        from: VoteOf<C>,
        /// The assigned node to be the next Leader.
        to: C::NodeId,
        /// The last log id the target must have locally before starting election.
        last_log_id: Option<LogIdOf<C>>,
    },

    ExternalCommand {
        cmd: ExternalCommand<C>,
    },
}

impl<C> fmt::Display for RaftMsg<C>
where
    C: RaftTypeConfig,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RaftMsg::AppendEntries { rpc, .. } => {
                write!(f, "AppendEntries: {}", rpc)
            }
            RaftMsg::RequestVote { rpc, .. } => {
                write!(f, "RequestVote: {}", rpc)
            }
            RaftMsg::RequestPreVote { rpc, .. } => {
                write!(f, "RequestPreVote: {}", rpc)
            }
            RaftMsg::ClientWrite { .. } => write!(f, "ClientWrite"),
            RaftMsg::GetLinearizer {
                linearizer_option, ..
            } => {
                write!(f, "GetLinearizer: {}", linearizer_option)
            }
            RaftMsg::Initialize { members, .. } => {
                write!(f, "Initialize: {}", members.display())
            }
            RaftMsg::ChangeMembership {
                changes,
                retain,
                preconditions,
                ..
            } => {
                write!(
                    f,
                    "ChangeMembership: {}, retain: {}, preconditions: {}",
                    changes,
                    retain,
                    preconditions.as_ref().display()
                )
            }
            RaftMsg::AppendMembership { preconditions, .. } => {
                write!(
                    f,
                    "AppendMembership: preconditions: {}",
                    preconditions.as_ref().display()
                )
            }
            RaftMsg::WithRaftState { .. } => write!(f, "WithRaftState"),
            RaftMsg::HandleTransferLeader {
                from,
                to,
                last_log_id,
            } => {
                write!(
                    f,
                    "TransferLeader: from_leader: vote={}, to: {}, last_log_id: {}",
                    from,
                    to,
                    last_log_id.display()
                )
            }
            RaftMsg::ExternalCommand { cmd } => {
                write!(f, "ExternalCommand: {}", cmd)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::Batch;
    use crate::engine::testing::UTConfig;
    use crate::engine::testing::log_id;
    use crate::entry::EntryPayload;

    /// `AppendMembership` prints the preconditions the caller supplied.
    #[test]
    fn test_append_membership_display() {
        let (tx, _rx) = ProgressResponder::<UTConfig, ClientWriteResult<UTConfig>>::complete_only();

        let precondition = Precondition::LastMembershipLogId {
            last_membership_log_id: Some(log_id(1, 2, 3)),
        };
        let msg = RaftMsg::<UTConfig>::AppendMembership {
            payload: EntryPayload::Blank,
            preconditions: BatchOf::<UTConfig, _>::of([precondition]),
            tx,
        };

        assert_eq!(
            "AppendMembership: preconditions: [LastMembershipLogId(T1-N2.3)]",
            msg.to_string()
        );
    }
}
