from dataclasses import dataclass, field

from velis.core.messages import Message


@dataclass
class ManualClock:
    time_ms: int = 0

    def now_ms(self) -> int:
        return self.time_ms

    def advance(self, ms: int) -> None:
        self.time_ms += ms


@dataclass(eq=False)
class RecordingSubscriber:
    accepting: bool = True
    messages: list[Message] = field(default_factory=list)

    def send(self, message: Message) -> bool:
        self.messages.append(message)
        return self.accepting
