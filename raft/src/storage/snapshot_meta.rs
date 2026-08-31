use std::fmt;

use display_more::DisplayOptionExt;

use crate::StoredMembership;
use crate::log_id::LogId;
use crate::node::Node;
use crate::node::NodeId;
use crate::storage::SnapshotSignature;
use crate::vote::RaftCommittedLeaderId;

/// The metadata of a snapshot.
///
/// Including the last log id that is included in this snapshot
/// and the last membership included.
///
/// # Compatibility with 0.9
///
/// Before 0.10.0 this type also carried a `snapshot_id`, declared last. It identified a transfer,
/// not the snapshot: two snapshots at the same `last_log_id` cover the same state, even when they
/// differ in bytes. The id now lives on the wire, in
/// `openraft_legacy::network_v1::SnapshotMeta`, the metadata type of the chunked v1 protocol,
/// which keeps the full 0.9 layout.
///
/// Dropping it from the serialized form as well would have broken stored 0.9 data. A positional
/// format (`bincode`, `postcard`, `rmp-serde::to_vec`) encodes a struct as a bare sequence with no
/// field names, so two fields cannot be read off a three-element record. Worse, when this type is
/// nested in a larger struct, some of those formats misparse it *silently*: the leftover
/// `snapshot_id` bytes get consumed as the enclosing struct's next field, with no error.
///
/// So the slot is reserved rather than removed. This type serializes as three fields, the third
/// being an always-empty `snapshot_id`, and ignores that field when reading. 0.9 and 0.10 data are
/// therefore interchangeable in both directions, under named and positional formats alike.
#[derive(Debug, Clone, PartialEq, Eq, bitcode::Encode, bitcode::Decode)]
pub struct SnapshotMeta<CLID, NID, N>
where
    CLID: RaftCommittedLeaderId,
    NID: NodeId,
    N: Node,
{
    /// Log entries up to which this snapshot includes, inclusive.
    pub last_log_id: Option<LogId<CLID>>,

    /// The last applied membership config.
    pub last_membership: StoredMembership<CLID, NID, N>,
}

impl<CLID, NID, N> Default for SnapshotMeta<CLID, NID, N>
where
    CLID: RaftCommittedLeaderId,
    NID: NodeId,
    N: Node,
{
    fn default() -> Self {
        Self {
            last_log_id: None,
            last_membership: StoredMembership::default(),
        }
    }
}

impl<CLID, NID, N> fmt::Display for SnapshotMeta<CLID, NID, N>
where
    CLID: RaftCommittedLeaderId,
    NID: NodeId,
    N: Node,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{last_log:{}, last_membership: {}}}",
            self.last_log_id.display(),
            self.last_membership
        )
    }
}

impl<CLID, NID, N> SnapshotMeta<CLID, NID, N>
where
    CLID: RaftCommittedLeaderId,
    NID: NodeId,
    N: Node,
{
    /// Get the signature of this snapshot metadata for comparison and identification.
    pub fn signature(&self) -> SnapshotSignature<CLID> {
        SnapshotSignature {
            last_log_id: self.last_log_id.clone(),
            last_membership_log_id: self
                .last_membership
                .log_id()
                .as_ref()
                .map(|x| Box::new(x.clone())),
        }
    }

    /// Returns a ref to the id of the last log that is included in this snapshot.
    pub fn last_log_id(&self) -> Option<&LogId<CLID>> {
        self.last_log_id.as_ref()
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_snapshot_meta_bitcode() {
        use maplit::btreeset;

        use crate::Membership;
        use crate::StoredMembership;
        use crate::engine::testing::UTConfig;
        use crate::engine::testing::log_id;
        use crate::type_config::alias::SnapshotMetaOf;

        let meta = SnapshotMetaOf::<UTConfig> {
            last_log_id: Some(log_id(1, 2, 3)),
            last_membership: StoredMembership::new(
                Some(log_id(4, 5, 6)),
                Membership::new_with_defaults(vec![btreeset! {1,2}], []),
            ),
        };

        let bytes = bitcode::encode(&meta);
        let decoded: SnapshotMetaOf<UTConfig> = bitcode::decode(&bytes).unwrap();
        assert_eq!(meta, decoded);
    }
}
