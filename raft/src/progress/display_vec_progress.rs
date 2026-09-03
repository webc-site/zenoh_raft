use std::fmt::{Display, Formatter, Result as FmtResult};

use super::{VecProgressEntry, vec_progress::VecProgress};
use crate::quorum::QuorumSet;

pub(crate) struct DisplayVecProgress<'a, Entry, QS, Fmt>
where
  Entry: VecProgressEntry,
  QS: QuorumSet<Id = Entry::Id>,
  Fmt: Fn(&mut Formatter<'_>, &Entry) -> FmtResult,
{
  pub(crate) inner: &'a VecProgress<Entry, QS>,
  pub(crate) f: Fmt,
}

impl<Entry, QS, Fmt> Display for DisplayVecProgress<'_, Entry, QS, Fmt>
where
  Entry: VecProgressEntry,
  QS: QuorumSet<Id = Entry::Id>,
  Fmt: Fn(&mut Formatter<'_>, &Entry) -> FmtResult,
{
  fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
    write!(f, "{{")?;
    for (i, item) in self.inner.iter().enumerate() {
      if i > 0 {
        write!(f, ", ")?;
      }
      (self.f)(f, item)?;
    }
    write!(f, "}}")?;

    Ok(())
  }
}
