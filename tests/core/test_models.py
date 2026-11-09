"""Independent models use lists and scans, without production indexing logic."""

import random
import unittest
from collections import Counter

from tests.helpers import ManualClock
from velis.core.eviction import LFU, EvictionPolicy
from velis.core.store import Store, StoreLimits, TtlStatus
from velis.errors import CapacityError


class ModelTests(unittest.TestCase):
    def test_lfu_matches_a_scanning_model_across_access_removal_and_eviction(
        self,
    ) -> None:
        for seed in range(5):
            rng = random.Random(seed)
            actual = LFU()
            counts: Counter[str] = Counter()
            accessed: dict[str, int] = {}
            for step in range(2000):
                key = str(rng.randrange(12))
                match rng.randrange(3):
                    case 0:
                        actual.access(key)
                        counts[key] += 1
                        accessed[key] = step
                    case 1:
                        actual.discard(key)
                        counts.pop(key, None)
                        accessed.pop(key, None)
                    case 2 if counts:
                        expected = min(
                            counts, key=lambda item: (counts[item], accessed[item])
                        )
                        with self.subTest(seed=seed, step=step):
                            self.assertEqual(actual.evict(), expected)
                        del counts[expected]
                        del accessed[expected]

    def test_lru_store_matches_a_scanning_model_across_ttl_and_capacity_changes(
        self,
    ) -> None:
        for seed in range(5):
            self._check_lru_model(seed)

    def _check_lru_model(self, seed: int) -> None:
        rng = random.Random(seed)
        clock = ManualClock()
        store = Store(
            clock,
            limits=StoreLimits(
                max_keys=4, max_bytes=20, eviction_policy=EvictionPolicy.LRU
            ),
        )
        values: dict[str, bytes] = {}
        deadlines: dict[str, int] = {}
        accessed: dict[str, int] = {}

        def remove(key: str) -> None:
            values.pop(key, None)
            accessed.pop(key, None)
            deadlines.pop(key, None)

        def lazy_expire(key: str) -> None:
            if deadlines.get(key, clock.now_ms() + 1) <= clock.now_ms():
                remove(key)

        def sweep() -> None:
            for key in tuple(deadlines):
                lazy_expire(key)

        def size() -> int:
            return sum(len(key.encode()) + len(value) for key, value in values.items())

        def fits(key: str, new_size: int) -> bool:
            old_size = len(key.encode()) + len(values[key]) if key in values else 0
            return (
                len(values) + (key not in values) <= 4
                and size() - old_size + new_size <= 20
            )

        for step in range(1000):
            key = rng.choice(("a", "b", "c", "d", "é"))
            with self.subTest(seed=seed, step=step):
                match rng.randrange(8):
                    case 0:
                        value = rng.randbytes(rng.randrange(24))
                        new_size = len(key.encode()) + len(value)
                        rejected = new_size > 20
                        if not rejected:
                            remove(key)

                            if not fits(key, new_size):
                                sweep()
                            while not fits(key, new_size):
                                remove(min(values, key=accessed.__getitem__))
                        if rejected:
                            with self.assertRaises(CapacityError):
                                store.set(key, value)
                        else:
                            values[key] = value
                            accessed[key] = step
                            deadlines.pop(key, None)
                            store.set(key, value)
                    case 1:
                        lazy_expire(key)
                        self.assertEqual(store.get(key), values.get(key))
                        if key in values:
                            accessed[key] = step
                    case 2:
                        lazy_expire(key)
                        expected = key in values
                        remove(key)
                        self.assertEqual(store.delete(key), expected)
                    case 3:
                        lazy_expire(key)
                        ttl = rng.randrange(8)
                        expected = key in values
                        if expected:
                            deadlines[key] = clock.now_ms() + ttl
                        self.assertEqual(store.expire(key, ttl), expected)
                    case 4:
                        lazy_expire(key)
                        expected = deadlines.pop(key, None) is not None
                        self.assertEqual(store.persist(key), expected)
                    case 5:
                        clock.advance(rng.randrange(8))
                    case 6:
                        sweep()
                        store.sweep()
                    case 7:
                        lazy_expire(key)
                        expected = (
                            TtlStatus.MISSING
                            if key not in values
                            else deadlines[key] - clock.now_ms()
                            if key in deadlines
                            else TtlStatus.PERSISTENT
                        )
                        self.assertEqual(store.ttl(key), expected)
                self.assertEqual(len(store), len(values))
                for candidate in ("a", "b", "c", "d", "é"):
                    self.assertEqual(
                        store._inner._inner._inner.get(candidate), values.get(candidate)
                    )
                self.assertEqual(store.stored_bytes, size())
