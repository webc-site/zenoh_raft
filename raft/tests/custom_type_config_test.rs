//! Compile-time test to verify that all `RaftTypeConfig` associated types
//! can be fully customized by application developers.

#![allow(dead_code)]

use std::vec::IntoIter;

use zenoh_raft::{
  AnyError, BasicNode, Entry, EntryPayload, OptionalSend, RaftTypeConfig,
  batch::Batch,
  impls::{OneshotResponder, Vote, leader_id_adv::LeaderId},
  vote::RaftLeaderId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct VecBatch<T> {
  inner: Vec<T>,
}

impl<T> Unpin for VecBatch<T> {}

impl<T> AsRef<[T]> for VecBatch<T> {
  fn as_ref(&self) -> &[T] {
    &self.inner
  }
}

impl<T> Extend<T> for VecBatch<T> {
  fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
    self.inner.extend(iter);
  }
}

impl<T> IntoIterator for VecBatch<T> {
  type Item = T;
  type IntoIter = IntoIter<T>;

  fn into_iter(self) -> Self::IntoIter {
    self.inner.into_iter()
  }
}

impl<T> FromIterator<T> for VecBatch<T> {
  fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
    VecBatch {
      inner: iter.into_iter().collect(),
    }
  }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
struct CustomConfig;

impl RaftTypeConfig for CustomConfig {
  type D = u64;
  type R = u64;
  type NodeId = u64;
  type Node = BasicNode;
  type Term = u64;
  type LeaderId = LeaderId<u64, u64>;
  type Vote = Vote<Self::LeaderId>;
  type Payload = EntryPayload<Self::D, Self::NodeId, Self::Node>;
  type Entry = Entry<<Self::LeaderId as RaftLeaderId>::Committed, Self::Payload>;
  type Responder<T>
    = OneshotResponder<Self, T>
  where
    T: OptionalSend + 'static;
  type Batch<T>
    = VecBatch<T>
  where
    T: OptionalSend + 'static;
  type ErrorSource = AnyError;
}

fn assert_batch_trait<T, B>()
where
  T: PartialEq,
  B: Batch<T>,
{
}

fn use_batch_methods() {
  let batch = <CustomConfig as RaftTypeConfig>::Batch::<u64>::of([1, 2, 3]);
  let _len = batch.len();
  let _last = batch.last();
  let _slice: &[u64] = batch.as_ref();
}

#[test]
fn test_custom_type_config_compiles() {
  assert_batch_trait::<u64, VecBatch<u64>>();
  use_batch_methods();

  let b1 = <CustomConfig as RaftTypeConfig>::Batch::<u64>::of([42]);
  assert_eq!(b1.as_ref(), &[42]);

  let b2 = <CustomConfig as RaftTypeConfig>::Batch::<u64>::of(vec![1, 2, 3]);
  assert_eq!(b2.as_ref(), &[1, 2, 3]);

  let b3 = <CustomConfig as RaftTypeConfig>::Batch::<u64>::of(0..5);
  assert_eq!(b3.as_ref(), &[0, 1, 2, 3, 4]);
}
