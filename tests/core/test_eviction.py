import unittest

from hypothesis import given
from hypothesis import strategies as st

from velis.core.eviction import LFU, LRU, S3FIFO, EvictionPolicy


class EvictionTests(unittest.TestCase):
    def test_policy_factories_create_independent_instances(self) -> None:
        for policy, cls in (
            (EvictionPolicy.LRU, LRU),
            (EvictionPolicy.LFU, LFU),
            (EvictionPolicy.S3_FIFO, S3FIFO),
        ):
            with self.subTest(policy=policy):
                first, second = policy.create(), policy.create()
                self.assertIsInstance(first, cls)
                first.access("a")
                self.assertNotIn("a", second)

    def test_discard_is_idempotent_and_reinsertion_is_resident(self) -> None:
        for policy in EvictionPolicy:
            with self.subTest(policy=policy):
                eviction = policy.create()
                eviction.access("a")
                eviction.access("a")
                self.assertEqual(len(eviction), 1)
                eviction.discard("a")
                eviction.discard("a")
                eviction.discard("missing")
                self.assertNotIn("a", eviction)
                self.assertEqual(len(eviction), 0)
                eviction.access("a")
                self.assertEqual(eviction.evict(), "a")
                self.assertEqual(len(eviction), 0)
                with self.assertRaisesRegex(AssertionError, "empty store"):
                    eviction.evict()

    def test_lru_access_moves_key_to_newest_end(self) -> None:
        policy = LRU()
        for key in "abca":
            policy.access(key)
        self.assertEqual([policy.evict() for _ in range(3)], ["b", "c", "a"])

    def test_lfu_frequency_then_oldest_access_breaks_ties(self) -> None:
        policy = LFU()
        for key in "abcacb":
            policy.access(key)
        self.assertEqual([policy.evict() for _ in range(3)], ["a", "c", "b"])

    def test_lfu_compaction_and_reinsertion_ignore_stale_records(self) -> None:
        policy = LFU()
        policy.access("hot")
        policy.access("cold")
        for _ in range(1000):
            policy.access("hot")
        policy.discard("cold")
        policy.access("cold")
        self.assertEqual(policy.evict(), "cold")
        self.assertEqual(policy.evict(), "hot")

    def test_lfu_eviction_reclaims_stale_metadata_when_residency_shrinks(self) -> None:
        policy = LFU()
        for i in range(1000):
            policy.access(str(i))
        for _ in range(500):
            policy.access("0")
        while len(policy) > 1:
            policy.evict()
        self.assertLessEqual(len(policy._heap), 2)

    def test_s3_fifo_deletion_trims_old_ghosts_to_current_residency(self) -> None:
        policy = S3FIFO()
        for i in range(1000):
            policy.access(str(i))
        for _ in range(400):
            policy.evict()
        for i in range(400, 999):
            policy.discard(str(i))
        self.assertEqual(len(policy), 1)
        self.assertLessEqual(len(policy._ghost), 1)
        self.assertIn("399", policy._ghost)

    def test_s3_fifo_filters_one_hit_keys_without_reordering(self) -> None:
        policy = S3FIFO()
        for key in "abc":
            policy.access(key)
        policy.access("a")
        self.assertEqual(policy.evict(), "a")
        self.assertNotIn("a", policy)
        self.assertEqual(len(policy), 2)

    def test_s3_fifo_promotes_reused_keys_and_evicts_exactly_one(self) -> None:
        policy = S3FIFO()
        for key in "abcaa":
            policy.access(key)
        self.assertEqual(policy.evict(), "b")
        self.assertEqual(len(policy), 2)
        self.assertIn("a", policy)
        self.assertEqual(policy.evict(), "c")
        for _ in range(100):
            policy.access("a")
        self.assertEqual(policy.evict(), "a")

    def test_s3_fifo_ghost_hit_enters_main_but_discard_forgets_history(self) -> None:
        for forget in (False, True):
            with self.subTest(forget=forget):
                policy = S3FIFO()
                for key in "abc":
                    policy.access(key)
                self.assertEqual(policy.evict(), "a")
                if forget:
                    policy.discard("a")
                policy.access("a")
                self.assertEqual(policy.evict(), "b")
                self.assertEqual(policy.evict(), "c")
                policy.access("d")
                self.assertEqual(policy.evict(), "a" if forget else "d")

    def test_s3_fifo_rejects_invalid_ratios(self) -> None:
        for kwargs in (
            {"small_ratio": 0},
            {"small_ratio": 1},
            {"small_ratio": -0.1},
            {"ghost_ratio": 0},
            {"ghost_ratio": -1},
            {"ghost_ratio": float("nan")},
            {"ghost_ratio": float("inf")},
            {"small_ratio": float("nan")},
            {"small_ratio": float("inf")},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                S3FIFO(**kwargs)

    @given(
        st.sampled_from(tuple(EvictionPolicy)),
        st.lists(st.sampled_from("abcdefgh"), max_size=200),
    )
    def test_eviction_returns_each_resident_once(
        self, policy: EvictionPolicy, accesses: list[str]
    ) -> None:
        eviction = policy.create()
        for key in accesses:
            eviction.access(key)
        residents = set(accesses)
        self.assertEqual(len(eviction), len(residents))
        while residents:
            victim = eviction.evict()
            self.assertIn(victim, residents)
            residents.remove(victim)
            self.assertNotIn(victim, eviction)
            self.assertEqual(len(eviction), len(residents))
