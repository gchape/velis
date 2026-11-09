from velis.core.broker import Publication


class ManualClock:
    """Test clock that advances without waiting for real time."""

    def __init__(self, now_ms: int = 0) -> None:
        self._now = now_ms

    def now_ms(self) -> int:
        return self._now

    def advance(self, ms: int) -> None:
        self._now += ms


class RecordingSubscriber:
    """Record accepted messages and simulate a disconnected subscriber."""

    def __init__(self) -> None:
        self.alive = True
        self.messages: list[tuple[str, bytes]] = []

    def send(self, publication: Publication) -> bool:
        if not self.alive:
            return False
        self.messages.append((publication.channel, publication.payload))
        return True
