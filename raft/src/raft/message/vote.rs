use std::{borrow::Borrow, fmt};

use display_more::DisplayOptionExt;

use crate::{
  RaftTypeConfig,
  type_config::alias::{LogIdOf, VoteOf},
};

/// An RPC sent by candidates to gather votes (§5.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteRequest<C: RaftTypeConfig> {
  /// The candidate's vote requesting support.
  pub vote: VoteOf<C>,
  /// The candidate's last log id.
  pub last_log_id: Option<LogIdOf<C>>,

  /// True if the election is part of a leadership transfer authorized by the current Leader.
  ///
  /// A voter processes such a request even if the leader lease has not expired: the disruption
  /// the lease protects against is legitimate when the Leader itself initiates the election
  /// ("I have permission to disrupt the leader--it told me to!").
  /// See: Raft dissertation, section 4.2.3.
  pub leadership_transfer: bool,
  /// True if this request is a Pre-Vote probe.
  pub is_pre_vote: bool,
}

impl<C> fmt::Display for VoteRequest<C>
where
  C: RaftTypeConfig,
{
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "{{vote:{}, last_log:{}, leadership_transfer:{}, is_pre_vote:{}}}",
      self.vote,
      self.last_log_id.display(),
      self.leadership_transfer,
      self.is_pre_vote,
    )
  }
}

impl<C> VoteRequest<C>
where
  C: RaftTypeConfig,
{
  /// Create a new vote request.
  pub fn new(vote: VoteOf<C>, last_log_id: Option<LogIdOf<C>>) -> Self {
    Self {
      vote,
      last_log_id,
      leadership_transfer: false,
      is_pre_vote: false,
    }
  }

  /// Create a new pre-vote request.
  pub fn new_pre_vote(vote: VoteOf<C>, last_log_id: Option<LogIdOf<C>>) -> Self {
    Self {
      vote,
      last_log_id,
      leadership_transfer: false,
      is_pre_vote: true,
    }
  }
}

/// The response to a `VoteRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteResponse<C: RaftTypeConfig> {
  /// vote after a node handling vote-request.
  /// Thus, `resp.vote >= req.vote` always holds.
  ///
  /// `vote` that equals the candidate.vote does not mean the vote is granted.
  /// The `vote` may be updated when a previous Leader sees a higher vote.
  pub vote: VoteOf<C>,

  /// It is true if a node accepted and saved the VoteRequest.
  pub vote_granted: bool,

  /// The last log id stored on the remote voter.
  pub last_log_id: Option<LogIdOf<C>>,
}

impl<C> VoteResponse<C>
where
  C: RaftTypeConfig,
{
  /// Create a new vote response.
  pub fn new(vote: impl Borrow<VoteOf<C>>, last_log_id: Option<LogIdOf<C>>, granted: bool) -> Self {
    Self {
      vote: vote.borrow().clone(),
      vote_granted: granted,
      last_log_id: last_log_id.map(|x| x.borrow().clone()),
    }
  }

  /// Returns `true` if the response indicates that the target node has granted a vote to the
  /// candidate.
  pub fn is_granted_to(&self, candidate_vote: &VoteOf<C>) -> bool {
    &self.vote == candidate_vote
  }
}

impl<C> fmt::Display for VoteResponse<C>
where
  C: RaftTypeConfig,
{
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "{{{}, last_log:{:?}}}",
      self.vote,
      self.last_log_id.as_ref().map(|x| x.to_string())
    )
  }
}
