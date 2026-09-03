//! Deterministic RNG helper using task-local storage.

use std::{cell::Cell, future::Future};

use rand::{SeedableRng, rngs::SmallRng};

use crate::task_local::TaskLocalFuture;

const GOLDEN_RATIO: u64 = 0x9e3779b97f4a7c15;
const SQRT2: u64 = 0x6a09e667f3bcc908;

task_local! {
    static DETSIM_SEED: Cell<u64>;
}

fn splitmix64(seed: u64, gamma: u64) -> u64 {
  let z = seed.wrapping_add(gamma);
  let z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
  let z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
  z ^ (z >> 31)
}

pub fn derive_spawn_seed(seed: u64) -> u64 {
  splitmix64(seed, GOLDEN_RATIO)
}

pub fn advance_seed(seed: u64) -> u64 {
  splitmix64(seed, SQRT2)
}

pub fn take_and_advance_seed() -> u64 {
  DETSIM_SEED
    .try_with(|c| {
      let seed = c.get();
      c.set(advance_seed(seed));
      seed
    })
    .unwrap_or_else(|_| rand::random())
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DeterministicRng;

impl DeterministicRng {
  pub fn scope<F>(seed: u64, f: F) -> TaskLocalFuture<Cell<u64>, F>
  where
    F: Future,
  {
    DETSIM_SEED.scope(Cell::new(seed), f)
  }

  pub fn thread_rng() -> SmallRng {
    SmallRng::seed_from_u64(take_and_advance_seed())
  }
}
