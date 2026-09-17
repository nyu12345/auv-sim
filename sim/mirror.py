"""Shore's copy of each hull's flight recorder.

Offloads arrive as byte spans of a hull's MCAP file, cut at whatever size the
hull chose. The mirror appends each span to its copy of that file and decodes
whatever whole records the new bytes complete, so shore's live picture and
its archive come from the same bytes. Once every span of a file has landed,
the copy is the hull's file byte for byte, footer and index included.

The decoder has to be resumable: a span may end mid-record, and the schema
and channel records that say how to decode a message arrive once, in the
first span of the file."""

import io
from pathlib import Path

from log import BY_NAME
from mcap.records import Channel, Message, Schema
from mcap.stream_reader import StreamReader
from messages import Span
from pydantic import BaseModel, ValidationError

MAGIC = b"\x89MCAP0\r\n"
PREFIX = 9  # opcode (1) + length (8, little-endian)


class Decoder:
    """Records of one file as its bytes arrive, in any span sizes."""

    def __init__(self) -> None:
        self._buf = b""
        self._schemas: dict[int, Schema] = {}
        self._channels: dict[int, Channel] = {}
        self.dropped = 0

    def feed(self, data: bytes) -> list[BaseModel]:
        """Every message the buffer now completes, as its model. Bytes short
        of a whole record wait for the next feed."""
        self._buf += data
        out: list[BaseModel] = []
        while self._buf:
            if self._buf.startswith(MAGIC):  # the file's first and last bytes
                self._buf = self._buf[len(MAGIC):]
                continue
            if MAGIC.startswith(self._buf) or len(self._buf) < PREFIX:
                break
            end = PREFIX + int.from_bytes(self._buf[1:PREFIX], "little")
            if len(self._buf) < end:
                break
            record, self._buf = self._buf[:end], self._buf[end:]
            model = self._decode(record)
            if model is not None:
                out.append(model)
        return out

    def _decode(self, record: bytes) -> BaseModel | None:
        parsed = next(StreamReader(io.BytesIO(record), skip_magic=True).records)
        if isinstance(parsed, Schema):
            self._schemas[parsed.id] = parsed
        elif isinstance(parsed, Channel):
            self._channels[parsed.id] = parsed
        elif isinstance(parsed, Message):
            schema = self._schemas[self._channels[parsed.channel_id].schema_id]
            try:
                return BY_NAME[schema.name].model_validate_json(parsed.data)
            except ValidationError as e:
                # One garbled record is one garbled record; the file around
                # it is still the hull's log, so it is kept and counted.
                self.dropped += 1
                print(f"mirror: dropped malformed {schema.name} {parsed.data!r}: {e.errors()[0]['msg']}")
        return None


class Mirror:
    """Copies on disk, one directory per hull, one file per hull run."""

    def __init__(self, root: Path) -> None:
        root.mkdir(parents=True, exist_ok=True)
        self.root = root
        self._decoders: dict[Path, Decoder] = {}

    def path(self, span: Span) -> Path:
        return self.root / span.id / span.file

    def size(self, hull: str) -> int:
        """Bytes mirrored for a hull across all its runs."""
        d = self.root / hull
        return sum(p.stat().st_size for p in d.iterdir()) if d.is_dir() else 0

    def append(self, span: Span, data: bytes) -> list[BaseModel]:
        """Append what is new in this span and return the records it completes.
        A span already held (the hull re-sent after a crash) is skipped; one
        that overlaps is trimmed; one past the end is a gap, which means the
        stream buffer expired before shore read it, and is dropped rather
        than leave a hole in the copy."""
        path = self.path(span)
        have = path.stat().st_size if path.exists() else 0
        if span.offset + len(data) <= have:
            print(f"mirror: {span.id} {span.file}@{span.offset} already held, skipped")
            return []
        if span.offset > have:
            print(f"mirror: {span.id} {span.file}@{span.offset} but copy ends at {have}: gap, dropped")
            return []
        new = data[have - span.offset:]
        decoder = self._decoder(path)
        path.parent.mkdir(exist_ok=True)
        with path.open("ab") as f:
            f.write(new)
        return decoder.feed(new)

    def replay(self, path: Path) -> list[BaseModel]:
        """Every record in a copy, decoded from the bytes on disk. Also primes
        the decoder, so a span arriving after a shore restart is decoded
        against schemas that landed before it."""
        decoder = self._decoders[path] = Decoder()
        return decoder.feed(path.read_bytes()) if path.exists() else []

    def files(self) -> list[Path]:
        return sorted(p for p in self.root.glob("*/*.mcap"))

    def _decoder(self, path: Path) -> Decoder:
        if path not in self._decoders:
            self.replay(path)
        return self._decoders[path]
