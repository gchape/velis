import asyncio
import contextlib

from velis.net import protocol


async def close_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
    with contextlib.suppress(OSError):
        await writer.wait_closed()


async def cancel_task(task: asyncio.Task) -> None:
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def read_message(reader: asyncio.StreamReader) -> protocol.ServerFrame:
    async with asyncio.timeout(5):
        body = await protocol.read_frame(reader)
    if body is None:
        raise AssertionError("expected a server message before EOF")
    return protocol.SERVER_MESSAGES.decode(body)


class RecordingTransport(asyncio.Transport):
    def __init__(self) -> None:
        self.writes: list[bytes] = []
        self.closing = False
        self.aborted = False
        self.reading = True
        self.buffer_size = 0
        self.write_error: Exception | None = None

    def get_extra_info(self, name: str, default: object = None) -> object:
        return default

    def is_closing(self) -> bool:
        return self.closing

    def write(self, data) -> None:
        if self.write_error is not None:
            raise self.write_error
        self.writes.append(bytes(data))

    def close(self) -> None:
        self.closing = True

    def abort(self) -> None:
        self.closing = self.aborted = True

    def pause_reading(self) -> None:
        self.reading = False

    def resume_reading(self) -> None:
        self.reading = True

    def get_write_buffer_size(self) -> int:
        return self.buffer_size

    def messages(self) -> list[protocol.ServerFrame]:
        buf = bytearray(b"".join(self.writes))
        frames = list(protocol.iter_frames(buf, protocol.MAX_SERVER_BODY_BYTES))
        if buf and (not frames or frames[-1][1] != len(buf)):
            raise AssertionError("transport contains an incomplete frame")
        return [protocol.SERVER_MESSAGES.decode(body) for body, _ in frames]
