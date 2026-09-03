use std::time::Duration;

use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::{
  Membership, Vote,
  batch::Batch,
  core::ServerState,
  engine::{
    Command, Engine, LogIdList,
    testing::{UTConfig, log_id},
  },
  entry::RaftEntry,
  errors::{InitializeError, NotAllowed, NotInMembers},
  raft_state::LogStateReader,
  type_config::{TypeConfigExt, alias::EntryOf},
  utime::Leased,
  vote::raft_vote::RaftVoteExt,
};

#[test]
fn test_initialize_single_node() -> anyhow::Result<()> {
  let eng = || {
    let mut eng = Engine::<UTConfig>::testing_default(0);
    eng.state.enable_validation(false); // Disable validation for incomplete state

    eng.state.server_state = eng.calc_server_state();
    eng
  };

  let m1 = || Membership::<u64, ()>::new_with_defaults(vec![btreeset! {1}], []);

  log::info!("--- ok: init empty node 1 with membership(1,2)");
  log::info!("--- expect OK result, check output commands and state changes");
  {
    let mut eng = eng();
    eng.config.id = 1;

    // The first log id uses the node's own id as leader
    let log_id0 = log_id(0, 1, 0);

    eng.initialize(m1())?;

    assert_eq!(Some(log_id0), eng.state.get_log_id(0));
    assert_eq!(None, eng.state.get_log_id(1));
    assert_eq!(Some(&log_id0), eng.state.last_log_id());

    assert_eq!(ServerState::Leader, eng.state.server_state);
    assert_eq!(&m1(), eng.state.membership_state.effective().membership());

    assert_eq!(
      vec![
        //
        Command::AppendEntries {
          committed_vote: Vote::new_with_default_term(1).to_committed(),
          entries: Batch::of([EntryOf::<UTConfig>::new_membership(log_id(0, 1, 0), m1())]),
        },
      ],
      eng.output.take_commands()
    );
  }
  Ok(())
}

#[test]
fn test_initialize() -> anyhow::Result<()> {
  let eng = || {
    let mut eng = Engine::<UTConfig>::testing_default(0);
    eng.state.enable_validation(false); // Disable validation for incomplete state

    eng.state.server_state = eng.calc_server_state();
    eng
  };

  let m12 = || Membership::<u64, ()>::new_with_defaults(vec![btreeset! {1,2}], []);

  log::info!("--- ok: init empty node 1 with membership(1,2)");
  log::info!("--- expect OK result, check output commands and state changes");
  {
    let mut eng = eng();
    eng.config.id = 1;

    // The first log id uses the node's own id as leader
    let log_id0 = log_id(0, 1, 0);

    eng.initialize(m12())?;

    assert_eq!(Some(log_id0), eng.state.get_log_id(0));
    assert_eq!(None, eng.state.get_log_id(1));
    assert_eq!(Some(&log_id0), eng.state.last_log_id());

    assert_eq!(ServerState::Leader, eng.state.server_state);
    assert_eq!(&m12(), eng.state.membership_state.effective().membership());

    assert_eq!(
      vec![
        //
        Command::AppendEntries {
          committed_vote: Vote::new_with_default_term(1).to_committed(),
          entries: Batch::of([EntryOf::<UTConfig>::new_membership(log_id(0, 1, 0), m12())]),
        },
      ],
      eng.output.take_commands()
    );
  }

  // For error cases, use a pre-existing log with arbitrary log ID
  let existing_log_id = log_id(0, 0, 0);

  log::info!("--- not allowed because of last_log_id");
  {
    let mut eng = eng();
    eng.state.log_ids = LogIdList::new(None, vec![existing_log_id]);

    assert_eq!(
      Err(InitializeError::NotAllowed(NotAllowed {
        last_log_id: Some(existing_log_id),
        vote: Vote::new(0, 0),
      })),
      eng.initialize(m12())
    );
  }

  log::info!("--- not allowed because of vote");
  {
    let mut eng = eng();
    eng.state.vote = Leased::new(
      UTConfig::<()>::now(),
      Duration::from_millis(500),
      Vote::new(1, 1),
    );

    assert_eq!(
      Err(InitializeError::NotAllowed(NotAllowed {
        last_log_id: None,
        vote: Vote::new(1, 1),
      })),
      eng.initialize(m12())
    );
  }

  log::info!("--- node id 0 is not in membership");
  {
    let mut eng = eng();

    assert_eq!(
      Err(InitializeError::NotInMembers(NotInMembers {
        node_id: 0,
        membership: m12()
      })),
      eng.initialize(m12())
    );
  }

  Ok(())
}
