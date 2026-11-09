import random
import unittest

from velis.core.membership import ManyToMany


class MembershipTests(unittest.TestCase):
    def test_mirrored_indexes_match_a_set_of_pairs_and_prune_empty_entries(
        self,
    ) -> None:
        rng = random.Random(47)
        membership: ManyToMany[int, int] = ManyToMany()
        pairs: set[tuple[int, int]] = set()
        for step in range(1000):
            key, value = rng.randrange(8), rng.randrange(8)
            match rng.randrange(3):
                case 0:
                    membership.add(key, value)
                    pairs.add((key, value))
                case 1:
                    membership.discard(key, value)
                    pairs.discard((key, value))
                case 2:
                    membership.discard_value(value)
                    pairs = {(k, v) for k, v in pairs if v != value}
            with self.subTest(step=step):
                self.assertEqual(set(membership._values_by_key), {k for k, _ in pairs})
                self.assertEqual(set(membership._keys_by_value), {v for _, v in pairs})
                for candidate in range(8):
                    self.assertEqual(
                        set(membership.values_of(candidate)),
                        {v for k, v in pairs if k == candidate},
                    )
                    self.assertEqual(
                        membership.value_count(candidate),
                        sum(k == candidate for k, _ in pairs),
                    )
                    self.assertEqual(
                        membership.key_count(candidate),
                        sum(v == candidate for _, v in pairs),
                    )
