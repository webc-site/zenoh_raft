//! Policy for stepping down a Leader that is removed from the membership config.

/// Policy for stepping down a Leader that is removed from a committed membership config.
///
/// It is the value of
/// [`Config::removed_leader_step_down`](crate::Config::removed_leader_step_down).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StepDownPolicy {
    /// Never step down automatically.
    ///
    /// The removed Leader keeps leading, until the application reverts it to a learner with
    /// [`Trigger::refresh_server_state()`].
    ///
    /// [`Trigger::refresh_server_state()`]: crate::raft::trigger::Trigger::refresh_server_state
    Never,

    /// Step down after the specified number of milliseconds.
    ///
    /// The countdown starts when the membership config that removes the Leader is committed.
    /// When it expires, the Leader transfers leadership to the most up-to-date voter, then
    /// reverts itself to a learner.
    After(u64),
}
