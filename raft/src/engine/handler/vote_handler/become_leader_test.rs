use std::{sync::Arc, time::Duration};

use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::{
  Membership, Vote,
  batch::Batch,
  core::ServerState,
  engine::{
    Command, Engine, TargetProgress,
    testing::{UTConfig, log_id},
  },
  entry::RaftEntry,
  progress::{entry::ProgressEntry, inflight_id::InflightId, stream_id::StreamId},
  raft_state::IOId,
  replication::{payload::Payload, replicate::Replicate},
  type_config::{
    TypeConfigExt,
    alias::{EntryOf, StoredMembershipOf},
  },
  utime::Leased,
  vote::raft_vote::RaftVoteExt,
};

fn m01() -> Membership<u64, ()> {
  Membership::<u64, ()>::new_with_defaults(vec![btreeset! {0,1}], [])
}

fn eng() -> Engine<UTConfig> {
  let mut eng = Engine::testing_default(0);
  eng.state.enable_validation(false); // Disable validation for incomplete state

  eng.config.id = 1;
  eng.state.vote = Leased::new(
    UTConfig::<()>::now(),
    Duration::from_millis(500),
    Vote::new_committed(2, 1),
  );
  eng.state.server_state = ServerState::Candidate;
  eng
    .state
    .membership_state
    .set_effective(Arc::new(StoredMembershipOf::<UTConfig>::new(
      Some(log_id(1, 1, 1)),
      m01(),
    )));

  eng.output.take_commands();
  eng
}

#[test]
fn test_become_leader() -> anyhow::Result<()> {
  let mut eng = eng();
  eng.vote_handler().become_leader();

  let leader = eng.leader.as_ref().unwrap();
  assert_eq!(leader.noop_log_id, log_id(2, 1, 0));
  assert_eq!(leader.last_log_id(), Some(&log_id(2, 1, 0)));
  assert_eq!(*leader.committed_vote_ref(), Vote::new(2, 1).to_committed());

  assert_eq!(ServerState::Leader, eng.state.server_state);

  assert_eq!(
    eng.output.take_commands(),
    vec![
      Command::UpdateIOProgress {
        when: None,
        io_id: IOId::new_log_io(Vote::new(2, 1).to_committed(), None)
      },
      Command::RebuildReplicationStreams {
        leader_vote: Vote::new(2, 1).to_committed(),
        targets: vec![TargetProgress {
          target: 0,
          target_node: (),
          progress: ProgressEntry::empty(0, StreamId::new(1), 0),
        }],
        close_old_streams: true,
      },
      Command::AppendEntries {
        committed_vote: Vote::new(2, 1).to_committed(),
        entries: Batch::of([EntryOf::<UTConfig>::new_blank(log_id(2, 1, 0))])
      },
      // Pipeline mode: ProgressEntry::empty(0) has matching.next_index()=0 == searching_end=0
      Command::Replicate {
        target: 0,
        req: Replicate {
          inflight_id: InflightId::new(1),
          payload: Payload::LogsSince { prev: None },
        }
      }
    ]
  );

  Ok(())
}
