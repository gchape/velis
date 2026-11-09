"""A many-to-many relation with mirrored indexes."""

from collections import defaultdict


def _prune[K, V](index: dict[K, set[V]], key: K, item: V) -> None:
    if (items := index.get(key)) is not None:
        items.discard(item)
        if not items:
            del index[key]


class ManyToMany[K, V]:
    """Relate keys to values in both directions; empty entries never linger."""

    def __init__(self) -> None:
        self._values_by_key: defaultdict[K, set[V]] = defaultdict(set)
        self._keys_by_value: defaultdict[V, set[K]] = defaultdict(set)

    def add(self, key: K, value: V) -> None:
        self._values_by_key[key].add(value)
        self._keys_by_value[value].add(key)

    def discard(self, key: K, value: V) -> None:
        _prune(self._values_by_key, key, value)
        _prune(self._keys_by_value, value, key)

    def discard_value(self, value: V) -> None:
        """Remove every pair containing the value; safe to repeat."""
        for key in self._keys_by_value.pop(value, ()):
            _prune(self._values_by_key, key, value)

    def has(self, key: K, value: V) -> bool:
        return value in self._values_by_key.get(key, ())

    def value_count(self, key: K) -> int:
        return len(self._values_by_key.get(key, ()))

    def key_count(self, value: V) -> int:
        return len(self._keys_by_value.get(value, ()))

    def values_of(self, key: K) -> tuple[V, ...]:
        """Snapshot a key's values, so callers may mutate while iterating."""
        return tuple(self._values_by_key.get(key, ()))
