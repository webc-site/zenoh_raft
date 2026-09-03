use std::fmt;

/// Convert a type `T` to string.
///
/// If `T` implements `Display`, then `T` implements `MessageSummary` too.
///
/// MessageSummary is also a handy tool for displaying `Option` and `Slice` because it is
/// implemented for:
/// - `Option<T: MessageSummary>`
/// - and `&[T]` where `T: MessageSummary`.
///
///
/// # Examples
/// ```rust,ignore
/// # use openraft::MessageSummary;
/// # use openraft::testing::log_id;
/// let lid = log_id(1, 2, 3);
/// assert_eq!("1-2-3", lid.to_string(), "LogId is Display");
/// assert_eq!("1-2-3", lid.summary(), "Thus LogId is also MessageSummary");
/// assert_eq!("Some(1-2-3)", Some(lid).summary(), "Option<LogId> can be displayed too");
/// assert_eq!("Some(1-2-3)", Some(&lid).summary(), "Option<&LogId> can be displayed too");
///
/// let slc = vec![lid, lid];
/// assert_eq!("1-2-3,1-2-3", slc.as_slice().summary(), "&[LogId] can be displayed too");
///
/// let slc = vec![&lid, &lid];
/// assert_eq!("1-2-3,1-2-3", slc.as_slice().summary(), "&[&LogId] can be displayed too");
/// ```
pub trait MessageSummary<M = Self> {
  /// Return a string of a big message
  fn summary(&self) -> String;
}

impl<T> MessageSummary<T> for T
where
  T: fmt::Display,
{
  fn summary(&self) -> String {
    self.to_string()
  }
}

impl<T> MessageSummary<T> for &[T]
where
  T: MessageSummary<T>,
{
  fn summary(&self) -> String {
    if self.is_empty() {
      return "{}".to_string();
    }
    if self.len() <= 5 {
      let mut res = String::new();
      for (i, x) in self.iter().enumerate() {
        if i > 0 {
          res.push(',');
        }
        res.push_str(&x.summary());
      }
      res
    } else {
      let first_s = self.first().unwrap().summary();
      let last_s = self.last().unwrap().summary();

      format!("{first_s} ... {last_s}")
    }
  }
}

impl<T> MessageSummary<T> for Option<T>
where
  T: MessageSummary<T>,
{
  fn summary(&self) -> String {
    match self {
      None => "None".to_string(),
      Some(x) => {
        let s = x.summary();
        format!("Some({s})")
      }
    }
  }
}

#[cfg(test)]
mod tests {

  #[test]
  fn test_display() {
    use crate::{MessageSummary, engine::testing::log_id};

    let lid = log_id(1, 2, 3);
    assert_eq!("T1-N2.3", lid.to_string());
    assert_eq!("T1-N2.3", lid.summary());
    assert_eq!("Some(T1-N2.3)", Some(&lid).summary());
    assert_eq!("Some(T1-N2.3)", Some(lid).summary());

    let slc = vec![lid, lid];
    assert_eq!("T1-N2.3,T1-N2.3", slc.as_slice().summary());

    let slc = vec![&lid, &lid];
    assert_eq!("T1-N2.3,T1-N2.3", slc.as_slice().summary());
  }
}
