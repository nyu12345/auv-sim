"""The hull's flight recorder: an MCAP file with one channel per message
model, each carrying the model's JSON schema so any MCAP tool can decode it.

Written unchunked and flushed on every record. That forgoes the chunk index
(Foxglove still opens the file, just without random access) in exchange for
the property that matters on a hull: a process killed mid-tick leaves a file
readable up to the last record. Probed on mcap 1.4: the default 1 MB chunk
loses everything short of a clean close, unchunked loses nothing."""

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Iterable, Iterator

from mcap.exceptions import EndOfFile
from mcap.reader import NonSeekingReader
from mcap.writer import Writer
from messages import Heartbeat, Intent, Offload, Snapshot
from pydantic import BaseModel

MODELS: tuple[type[BaseModel], ...] = (Snapshot, Intent, Heartbeat, Offload)
BY_NAME = {m.__name__: m for m in MODELS}


class HullLog:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        # make a new file every run
        self._file: IO[bytes] = path.open("xb")
        # Bytes of this file the boat has acked. The hull advances it on each
        # ack; what lies between it and `size` is the live file's unsent tail.
        self.through = 0
        self._writer = Writer(self._file, use_chunking=False)
        self._writer.start(profile="", library="auv-sim")
        self._channels: dict[type[BaseModel], int] = {}

        for model in MODELS:
            schema_id = self._writer.register_schema(
                name=model.__name__,
                encoding="jsonschema",
                data=json.dumps(model.model_json_schema()).encode(),
            )
            self._channels[model] = self._writer.register_channel(
                topic=f"/{model.__name__.lower()}",
                message_encoding="json",
                schema_id=schema_id,
            )

    def write(self, msg: BaseModel) -> None:
        now = time.time_ns()
        ts = getattr(msg, "ts", None)
        self._writer.add_message(
            channel_id=self._channels[type(msg)],
            log_time=now,
            publish_time=int(ts * 1e9) if ts is not None else now,
            sequence=getattr(msg, "seq", 0),
            data=msg.model_dump_json().encode(),
        )
        self._file.flush()

    @property
    def size(self) -> int:
        """Bytes flushed so far. Every write flushes, so this is also the end
        of the last whole record: a span cut here never splits one."""
        return self.path.stat().st_size if self._file.closed else self._file.tell()

    def close(self) -> None:
        """Writes the summary, so an indexed reader works on a finished file."""
        self._writer.finish()
        self._file.close()


# For an MCAP log file, validate a message as one of the accepted pydantic models
# then yield and return an iterator that gives one message at a time
def read(path: Path) -> Iterator[BaseModel]:
    with path.open("rb") as f:
        reader = NonSeekingReader(f)
        try:
            for schema, _, message in reader.iter_messages(log_time_order=False):
                assert schema is not None
                yield BY_NAME[schema.name].model_validate_json(message.data)
        except EndOfFile:
            return


def slice(path: Path, start: int, limit: int) -> bytes:
    """Up to `limit` bytes of the file from `start`. Reads through a fresh
    handle, so it sees what the hull's own writer has flushed."""
    with path.open("rb") as f:
        f.seek(start)
        return f.read(limit)


@dataclass
class Tail:
    """The unsent bytes [start, end) of a finished file."""

    path: Path
    start: int
    end: int


# get a list of tails for files that still need to be offloaded successfully
def unsent(files: Iterable[Path]) -> list[Tail]:
    files = list(files)
    through: dict[str, int] = {}
    # for each file, go through each message
    # If the message is an offload message,
    # update the mapping we have for that file and how far through we successfully offloaded
    for path in files:
        for r in read(path):
            if isinstance(r, Offload):
                through[r.file] = max(through.get(r.file, 0), r.through)
    tails = [Tail(p, through.get(p.name, 0), p.stat().st_size) for p in files]
    return [t for t in tails if t.start < t.end]
