"""Group memberships, indexed from both sides."""

from typing import final


def _remove[K, V](index: dict[K, set[V]], key: K, item: V) -> None:
    if (items := index.get(key)) is not None:
        items.discard(item)
        if not items:
            del index[key]


@final
class Memberships[G, M]:
    """Track which members belong to which groups; empty entries never linger."""

    def __init__(self) -> None:
        self._members_by_group: dict[G, set[M]] = {}
        self._groups_by_member: dict[M, set[G]] = {}

    def join(self, group: G, member: M) -> None:
        self._members_by_group.setdefault(group, set()).add(member)
        self._groups_by_member.setdefault(member, set()).add(group)

    def leave(self, group: G, member: M) -> None:
        _remove(self._members_by_group, group, member)
        _remove(self._groups_by_member, member, group)

    def leave_all(self, member: M) -> None:
        """Remove the member from every group; safe to repeat."""
        for group in self._groups_by_member.pop(member, ()):
            _remove(self._members_by_group, group, member)

    def is_member(self, group: G, member: M) -> bool:
        return member in self._members_by_group.get(group, ())

    def member_count(self, group: G) -> int:
        return len(self._members_by_group.get(group, ()))

    def group_count(self, member: M) -> int:
        return len(self._groups_by_member.get(member, ()))

    def members(self, group: G) -> tuple[M, ...]:
        """Snapshot, so callers may mutate while iterating."""
        return tuple(self._members_by_group.get(group, ()))
