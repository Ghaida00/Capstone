pub mod failover;
pub mod pool;
pub mod shard;

#[cfg(test)]
#[path = "shard_tests.rs"]
mod shard_tests;
