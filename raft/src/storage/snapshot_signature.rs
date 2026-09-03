use crate::{log_id::LogId, vote::RaftCommittedLeaderId};

/// A small piece of information for identifying a snapshot and error tracing.
///
/// A snapshot is identified by the position it covers: two snapshots at the same `last_log_id`
/// cover the same state, even when they differ in bytes. The 0.9 `snapshot_id` identified a
/// transfer, not the snapshot, and is gone from the API.
///
/// # Compatibility with 0.9
///
/// Before 0.10.0 this type also carried the `snapshot_id`, declared last. Signatures are not
/// stored, but they travel inside errors: a `Fatal(StorageError)` returned over the v1 protocol
/// carries one to a 0.9 peer. Under a positional format (`bincode`, `postcard`,
/// `rmp-serde::to_vec`) a changed field count makes such a record unreadable, and nested inside
/// the error enums it can be misparsed silently instead of failing.
///
/// So, as in [`SnapshotMeta`](crate::storage::SnapshotMeta), the slot is reserved rather than
/// removed: this type serializes as three fields, the third being an always-empty
/// `snapshot_id`, and ignores that field when reading. 0.9 and 0.10 signatures are therefore
/// interchangeable in both directions, under named and positional formats alike.
///
/// The guarantee covers this type alone. [`StorageError`](crate::StorageError), which carries
/// the signature, was an enum in 0.9 and is a struct in 0.10, so a whole 0.10 error is not
/// parseable by a 0.9 peer regardless. Reserving the slot means the signature is never the
/// incompatibility; peers of a different version should treat error bodies as diagnostic
/// rather than parse them.
#[derive(Debug, Clone, PartialEq, Eq, bitcode::Encode, bitcode::Decode)]
pub struct SnapshotSignature<CLID>
where
  CLID: RaftCommittedLeaderId,
{
  /// Log entries up to which this snapshot includes, inclusive.
  pub last_log_id: Option<LogId<CLID>>,

  /// The last applied membership log id.
  pub last_membership_log_id: Option<Box<LogId<CLID>>>,
}

#[cfg(test)]
mod tests {

  #[test]
  fn test_snapshot_signature_bitcode() {
    use super::SnapshotSignature;
    use crate::engine::testing::{UtClid, log_id};

    let sig = SnapshotSignature::<UtClid> {
      last_log_id: Some(log_id(1, 2, 3)),
      last_membership_log_id: Some(Box::new(log_id(4, 5, 6))),
    };

    let bytes = bitcode::encode(&sig);
    let decoded: SnapshotSignature<UtClid> = bitcode::decode(&bytes).unwrap();
    assert_eq!(sig, decoded);
  }
}
