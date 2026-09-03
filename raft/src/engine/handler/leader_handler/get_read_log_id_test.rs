use std::{sync::Arc, time::Duration};

use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::{
  Membership, MembershipState, Vote,
  engine::{
    Engine,
    testing::{UTConfig, log_id},
  },
  raft::linearizable_read::ReadLogId,
  type_config::{TypeConfigExt, alias::StoredMembershipOf},
  utime::Leased,
};

fn m01() -> Membership<u64, ()> {
  Membership::<u64, ()>::new_with_defaults(vec![btreeset! {0,1}], [])
}

fn m23() -> Membership<u64, ()> {
  Membership::<u64, ()>::new_with_defaults(vec![btreeset! {2,3}], btreeset! {1,2,3})
}

fn eng() -> Engine<UTConfig> {
  let mut eng = Engine::testing_default(0);
  eng.state.enable_validation(false); // Disable validation for incomplete state

  eng.config.id = 1;
  eng.state.apply_progress_mut().accept(log_id(0, 1, 0));
  eng.state.vote = Leased::new(
    UTConfig::<()>::now(),
    Duration::from_millis(500),
    Vote::new_committed(3, 1),
  );
  eng.state.log_ids.append(log_id(1, 1, 1));
  eng.state.log_ids.append(log_id(2, 1, 3));
  eng.state.membership_state = MembershipState::new(
    Arc::new(StoredMembershipOf::<UTConfig>::new(
      Some(log_id(1, 1, 1)),
      m01(),
    )),
    Arc::new(StoredMembershipOf::<UTConfig>::new(
      Some(log_id(2, 1, 3)),
      m23(),
    )),
  );
  eng.testing_new_leader();
  eng.state.server_state = eng.calc_server_state();

  eng
}

#[test]
fn test_get_read_log_id() -> anyhow::Result<()> {
  let mut eng = eng();

  eng.state.apply_progress_mut().accept(log_id(0, 1, 0));
  let noop = log_id(3, 1, 4);
  eng.leader.as_mut().unwrap().noop_log_id = noop;

  let got = eng.try_leader_handler()?.get_read_log_id();
  assert_eq!(ReadLogId::new(noop, None), got);

  let committed = log_id(3, 1, 5);
  eng.state.apply_progress_mut().accept(committed);
  let got = eng.try_leader_handler()?.get_read_log_id();
  assert_eq!(ReadLogId::new(noop, Some(committed)), got);

  Ok(())
}
