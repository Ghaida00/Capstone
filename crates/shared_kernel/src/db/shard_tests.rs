#[cfg(test)]
mod tests {
    use crate::db::shard::ShardRouter;

    #[test]
    fn shard_for_is_deterministic() {
        let a = ShardRouter::shard_for("alice");
        let b = ShardRouter::shard_for("alice");
        assert_eq!(a, b, "Same input must always produce the same shard");
    }

    #[test]
    fn shard_for_returns_valid_index() {
        // NUM_SHARDS is 2 in the current codebase (shard 2 disabled).
        for account in [
            "alice",
            "bob",
            "charlie",
            "dave",
            "",
            "🚀",
            "a".repeat(10000).as_str(),
        ] {
            let shard = ShardRouter::shard_for(account);
            assert!(
                shard < 2,
                "Shard index {} out of range for account {:?}",
                shard,
                account
            );
        }
    }

    #[test]
    fn shard_for_distributes_across_shards() {
        let mut seen = std::collections::HashSet::new();
        // With enough unique inputs, all shards should be hit.
        for i in 0..100 {
            seen.insert(ShardRouter::shard_for(&format!("account-{}", i)));
        }
        assert_eq!(
            seen.len(),
            2,
            "Expected all 2 shards to be used across 100 accounts"
        );
    }
}

/// Property-based tests using `proptest`.
/// Generates thousands of random inputs to fuzz `ShardRouter::shard_for()`.
#[cfg(test)]
mod proptests {
    use crate::db::shard::ShardRouter;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn shard_for_always_in_range(account in ".*") {
            let shard = ShardRouter::shard_for(&account);
            prop_assert!(shard < 2, "Shard index {} out of range for {:?}", shard, account);
        }

        #[test]
        fn shard_for_is_pure(account in ".*") {
            let a = ShardRouter::shard_for(&account);
            let b = ShardRouter::shard_for(&account);
            prop_assert_eq!(a, b, "shard_for must be a pure function");
        }
    }
}
