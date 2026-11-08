"""Independent models use lists and scans, without production indexing logic."""

import random
import unittest

from tests.helpers import ManualClock
from velis.core.eviction import Sieve
from velis.core.store import EvictionPolicy, Store, StoreLimits
from velis.errors import CapacityError


class ModelTests(unittest.TestCase):
    def test_sieve_matches_a_list_model_across_access_removal_and_eviction(
        self,
    ) -> None:
        for seed in range(5):
            rng = random.Random(seed)
            actual = Sieve()
            order: list[str] = []
            visited: set[str] = set()
            hand: str | None = None
            for step in range(2000):
                key = str(rng.randrange(12))
                match rng.randrange(3):
                    case 0:
                        actual.access(key)
                        if key in order:
                            visited.add(key)
                        else:
                            order.append(key)
                    case 1:
                        actual.discard(key)
                        if key in order:
                            if hand == key:
                                index = order.index(key) + 1
                                hand = order[index] if index < len(order) else None
                            order.remove(key)
                            visited.discard(key)
                    case 2 if order:
                        index = order.index(hand) if hand is not None else 0
                        while order[index] in visited:
                            visited.remove(order[index])
                            index = (index + 1) % len(order)
                        expected = order[index]
                        hand = order[index + 1] if index + 1 < len(order) else None
                        with self.subTest(seed=seed, step=step):
                            self.assertEqual(actual.victim(), expected)
                        actual.discard(expected)
                        order.remove(expected)

    def test_reject_store_matches_a_scanning_model_across_ttl_and_capacity_changes(
        self,
    ) -> None:
        for seed in range(5):
            self._check_reject_model(seed)

    def _check_reject_model(self, seed: int) -> None:
        rng = random.Random(seed)
        clock = ManualClock()
        store = Store(
            clock,
            limits=StoreLimits(
                max_keys=4, max_bytes=20, eviction_policy=EvictionPolicy.REJECT
            ),
        )
        values: dict[str, bytes] = {}
        deadlines: dict[str, int] = {}

        def remove(key: str) -> None:
            values.pop(key, None)
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
                            lazy_expire(key)

                            if not fits(key, new_size):
                                sweep()
                            rejected = not fits(key, new_size)
                        if rejected:
                            with self.assertRaises(CapacityError):
                                store.set(key, value)
                        else:
                            values[key] = value
                            deadlines.pop(key, None)
                            store.set(key, value)
                    case 1:
                        lazy_expire(key)
                        self.assertEqual(store.get(key), values.get(key))
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
                            -2
                            if key not in values
                            else deadlines[key] - clock.now_ms()
                            if key in deadlines
                            else -1
                        )
                        self.assertEqual(store.ttl(key), expected)
                self.assertEqual(store._values, values)
                self.assertEqual(store.stored_bytes, size())
