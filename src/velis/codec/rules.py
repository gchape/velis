from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Validate[T]:
    """Apply a rule in both directions, returning the unchanged value.

    The rule raises an exception when it rejects a value; a return value
    is ignored, so a rule that returns False will not reject anything.
    Place this stage where its input type and meaning match the data
    being validated.
    """

    name: str
    rule: Callable[[T], None]

    def encode(self, value: T) -> T:
        self.rule(value)
        return value

    decode = encode
