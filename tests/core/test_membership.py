import random
import unittest

from velis.core.membership import Memberships


class MembershipTests(unittest.TestCase):
    def test_mirrored_indexes_match_a_set_of_pairs_and_prune_empty_entries(
        self,
    ) -> None:
        rng = random.Random(47)
        membership: Memberships[int, int] = Memberships()
        pairs: set[tuple[int, int]] = set()
        for step in range(1000):
            key, value = rng.randrange(8), rng.randrange(8)
            match rng.randrange(3):
                case 0:
                    membership.join(key, value)
                    pairs.add((key, value))
                case 1:
                    membership.leave(key, value)
                    pairs.discard((key, value))
                case 2:
                    membership.leave_all(value)
                    pairs = {(k, v) for k, v in pairs if v != value}
            with self.subTest(step=step):
                self.assertEqual(
                    set(membership._members_by_group), {k for k, _ in pairs}
                )
                self.assertEqual(
                    set(membership._groups_by_member), {v for _, v in pairs}
                )
                for candidate in range(8):
                    for member in range(8):
                        self.assertEqual(
                            membership.is_member(candidate, member),
                            (candidate, member) in pairs,
                        )
                    self.assertEqual(
                        set(membership.members(candidate)),
                        {v for k, v in pairs if k == candidate},
                    )
                    self.assertEqual(
                        membership.member_count(candidate),
                        sum(k == candidate for k, _ in pairs),
                    )
                    self.assertEqual(
                        membership.group_count(candidate),
                        sum(v == candidate for _, v in pairs),
                    )

    def test_members_snapshot_allows_leaving_groups_during_iteration(self) -> None:
        membership: Memberships[str, int] = Memberships()
        for member in range(3):
            membership.join("a", member)
            membership.join("b", member)
        snapshot = membership.members("a")
        for member in snapshot:
            membership.leave_all(member)
        self.assertEqual(set(snapshot), {0, 1, 2})
        self.assertEqual(membership.members("a"), ())
        self.assertEqual(membership.member_count("b"), 0)
