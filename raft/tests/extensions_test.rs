//! 用户自定义扩展数据 API 测试
mod fixtures;

use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use anyhow::Result;
use fixtures::RaftRouter;
use maplit::btreeset;
use zenoh_raft::Config;

#[derive(Clone, Default)]
struct Counter(Arc<AtomicU64>);

impl Counter {
  fn inc(&self) -> u64 {
    self.0.fetch_add(1, Ordering::SeqCst)
  }

  fn get(&self) -> u64 {
    self.0.load(Ordering::SeqCst)
  }
}

/// 测试 extension() 允许存储与读取具有内部可变性的自定义类型
#[compio::test]
async fn test_extensions_with_counter() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());

  router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  let raft = router.get_raft_handle(&0)?;

  let counter = raft.extension::<Counter>();
  assert_eq!(counter.get(), 0, "counter should start at 0");

  counter.inc();
  counter.inc();
  counter.inc();
  assert_eq!(counter.get(), 3, "counter should be 3 after 3 increments");

  let counter2 = raft.extension::<Counter>();
  assert_eq!(counter2.get(), 3, "should see the same counter state");

  counter2.inc();
  assert_eq!(counter2.get(), 4, "should see the increment");

  assert!(raft.extensions().contains::<Counter>());

  Ok(())
}
