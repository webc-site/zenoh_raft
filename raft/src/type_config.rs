//! Define the configuration of types used by the Raft, such as [`NodeId`], log [`Entry`], etc.
//!
//! [`NodeId`]: `RaftTypeConfig::NodeId`
//! [`Entry`]: `RaftTypeConfig::Entry`

pub(crate) mod util;

use std::{fmt::Debug, io::Error, time::Instant};

pub use util::TypeConfigExt;

pub use crate::async_runtime::OneshotSender;
use crate::{
  AppData, AppDataResponse, Node, NodeId, OptionalSend, OptionalSync,
  async_runtime::{
    JoinHandle, MpscReceiver, MpscSender, MpscWeakSender, Mutex, OneshotReceiver,
    OneshotSender as AsyncOneshotSender, WatchReceiver, WatchSender,
  },
  batch::Batch,
  entry::{RaftEntry, RaftPayload},
  errors::ErrorSource,
  metrics::SerdeInstant,
  raft::responder::Responder,
  vote::{RaftLeaderId, RaftTerm, raft_vote::RaftVote},
};

/// Type configuration for customizing Raft components.
pub trait RaftTypeConfig:
  OptionalSend
  + OptionalSync
  + 'static
  + Clone
  + Copy
  + Default
  + Eq
  + PartialEq
  + Ord
  + PartialOrd
  + Debug
  + Unpin
{
  /// Application-specific request data passed to the state machine.
  type D: AppData;

  /// Application-specific response data returned by the state machine.
  type R: AppDataResponse;

  /// Node identifier type.
  type NodeId: NodeId + Unpin;

  /// Node information type.
  type Node: Node + Unpin;

  /// Raft term type.
  type Term: RaftTerm + Unpin;

  /// Leader identifier type for term-based leadership tracking.
  type LeaderId: RaftLeaderId<Term = Self::Term, NodeId = Self::NodeId> + Unpin;

  /// Vote type tracking leadership grants.
  type Vote: RaftVote<LeaderId = Self::LeaderId> + Unpin;

  /// Log entry payload type.
  type Payload: RaftPayload<D = Self::D, NodeId = Self::NodeId, Node = Self::Node> + Unpin;

  /// Raft log entry with the configured payload.
  type Entry: RaftEntry<
      CommittedLeaderId = <Self::LeaderId as RaftLeaderId>::Committed,
      Payload = Self::Payload,
    > + Unpin;

  /// Responder type for sending client write responses asynchronously.
  type Responder<T>: Responder<Self, T> + Unpin
  where
    T: OptionalSend + 'static;

  type Batch<T>: Batch<T> + Unpin
  where
    T: OptionalSend + 'static;

  /// Error wrapper type for storage and network errors.
  type ErrorSource: ErrorSource + Unpin;
}

pub trait InstantConfig {
  type Instant;
  type SerdeInstant;
}

impl<C: ?Sized> InstantConfig for C {
  type Instant = Instant;
  type SerdeInstant = SerdeInstant<Instant>;
}

pub trait RuntimeTypeHelper<T> {
  type MpscSender;
  type MpscReceiver;
  type MpscWeakSender;
  type OneshotSender;
  type OneshotReceiver;
  type WatchSender;
  type WatchReceiver;
  type JoinHandle;
  type JoinError;
  type Mutex;
}

impl<C: ?Sized, T: 'static> RuntimeTypeHelper<T> for C {
  type MpscSender = MpscSender<T>;
  type MpscReceiver = MpscReceiver<T>;
  type MpscWeakSender = MpscWeakSender<T>;
  type OneshotSender = AsyncOneshotSender<T>;
  type OneshotReceiver = OneshotReceiver<T>;
  type WatchSender = WatchSender<T>;
  type WatchReceiver = WatchReceiver<T>;
  type JoinHandle = JoinHandle<T>;
  type JoinError = Error;
  type Mutex = Mutex<T>;
}

/// Type alias for types used in `RaftTypeConfig`.
pub mod alias {
  use super::{InstantConfig, RuntimeTypeHelper};
  use crate::{
    Entry, EntryPayload, LogId, MembershipState, RaftTypeConfig, StoredMembership,
    engine::log_id_list::LogIdList,
    errors::{ChangeMembershipError, InProgress},
    log_id::ref_log_id::RefLogId,
    raft::message::ClientWriteResult,
    storage::{RaftStateMachine, Snapshot, SnapshotMeta, SnapshotSignature},
    vote::{RaftLeaderId, committed::CommittedVote, non_committed::UncommittedVote},
  };

  pub type DOf<C> = <C as RaftTypeConfig>::D;
  pub type ROf<C> = <C as RaftTypeConfig>::R;
  pub type AppDataOf<C> = <C as RaftTypeConfig>::D;
  pub type AppResponseOf<C> = <C as RaftTypeConfig>::R;
  pub type NodeIdOf<C> = <C as RaftTypeConfig>::NodeId;
  pub type NodeOf<C> = <C as RaftTypeConfig>::Node;
  pub type TermOf<C> = <C as RaftTypeConfig>::Term;
  pub type LeaderIdOf<C> = <C as RaftTypeConfig>::LeaderId;
  pub type VoteOf<C> = <C as RaftTypeConfig>::Vote;
  pub type EntryOf<C> = <C as RaftTypeConfig>::Entry;
  pub type ResponderOf<C, T> = <C as RaftTypeConfig>::Responder<T>;
  pub type BatchOf<C, T> = <C as RaftTypeConfig>::Batch<T>;
  pub type ErrorSourceOf<C> = <C as RaftTypeConfig>::ErrorSource;

  pub type PayloadOf<C> = <EntryOf<C> as crate::RaftEntry>::Payload;

  pub type ClientWriteResultOf<C> = ClientWriteResult<C>;
  pub type WriteResponderOf<C> = <C as RaftTypeConfig>::Responder<ClientWriteResultOf<C>>;
  pub type JoinErrorOf<C> = <C as RuntimeTypeHelper<()>>::JoinError;
  pub type SnapshotDataOf<C, SM> = <SM as RaftStateMachine<C>>::SnapshotData;

  pub type InstantOf<C> = <C as InstantConfig>::Instant;
  pub type SerdeInstantOf<C> = <C as InstantConfig>::SerdeInstant;

  // Usually used types
  pub type LogIdOf<C> = LogId<CommittedLeaderIdOf<C>>;
  pub type CommittedLeaderIdOf<C> = <LeaderIdOf<C> as RaftLeaderId>::Committed;
  pub(crate) type RefLogIdOf<'a, C> = RefLogId<'a, CommittedLeaderIdOf<C>>;
  pub type EntryPayloadOf<C> = EntryPayload<DOf<C>, NodeIdOf<C>, NodeOf<C>>;
  pub type DefaultEntryOf<C> = Entry<CommittedLeaderIdOf<C>, PayloadOf<C>>;
  pub type StoredMembershipOf<C> = StoredMembership<CommittedLeaderIdOf<C>, NodeIdOf<C>, NodeOf<C>>;
  pub type SnapshotSignatureOf<C> = SnapshotSignature<CommittedLeaderIdOf<C>>;
  pub type SnapshotMetaOf<C> = SnapshotMeta<CommittedLeaderIdOf<C>, NodeIdOf<C>, NodeOf<C>>;
  pub type SnapshotOf<C, SD> = Snapshot<CommittedLeaderIdOf<C>, NodeIdOf<C>, NodeOf<C>, SD>;
  pub type SmSnapshotOf<C, SM> = SnapshotOf<C, SnapshotDataOf<C, SM>>;
  pub type MembershipStateOf<C> = MembershipState<CommittedLeaderIdOf<C>, NodeIdOf<C>, NodeOf<C>>;
  pub type ChangeMembershipErrorOf<C> = ChangeMembershipError<CommittedLeaderIdOf<C>, NodeIdOf<C>>;
  pub type InProgressOf<C> = InProgress<CommittedLeaderIdOf<C>>;

  // Projections from a LeaderId type (LID: RaftLeaderId)
  pub(crate) type LeaderTerm<LID> = <LID as RaftLeaderId>::Term;
  pub(crate) type LeaderNodeId<LID> = <LID as RaftLeaderId>::NodeId;
  pub(crate) type LeaderCommitted<LID> = <LID as RaftLeaderId>::Committed;

  // Internal vote types parameterized by C
  pub(crate) type CommittedVoteOf<C> = CommittedVote<LeaderIdOf<C>>;
  pub(crate) type UncommittedVoteOf<C> = UncommittedVote<LeaderIdOf<C>>;

  pub type LogIdListOf<C> = LogIdList<CommittedLeaderIdOf<C>>;

  pub type MpscSenderOf<C, T> = <C as RuntimeTypeHelper<T>>::MpscSender;
  pub type MpscReceiverOf<C, T> = <C as RuntimeTypeHelper<T>>::MpscReceiver;
  pub type MpscWeakSenderOf<C, T> = <C as RuntimeTypeHelper<T>>::MpscWeakSender;
  pub type OneshotSenderOf<C, T> = <C as RuntimeTypeHelper<T>>::OneshotSender;
  pub type OneshotReceiverOf<C, T> = <C as RuntimeTypeHelper<T>>::OneshotReceiver;
  pub type WatchSenderOf<C, T> = <C as RuntimeTypeHelper<T>>::WatchSender;
  pub type WatchReceiverOf<C, T> = <C as RuntimeTypeHelper<T>>::WatchReceiver;
  pub type JoinHandleOf<C, T> = <C as RuntimeTypeHelper<T>>::JoinHandle;
  pub type MutexOf<C, T> = <C as RuntimeTypeHelper<T>>::Mutex;
}
