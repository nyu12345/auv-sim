import asyncio
import json
import os
import random
import signal
import sys
import time
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import nats
from log import HullLog, Tail, slice, unsent
from messages import Heartbeat, Intent, Offload, Snapshot, Span
from streams import ensure_stream

# One span per docked tick, so this bounds both the NATS payload (1 MB cap)
# and the drain rate: about 512 KB/s at 2 Hz.
SPAN_BYTES = 256 * 1024

Publish = Callable[[str, bytes, dict[str, str]], Awaitable[None]]


class Auv:
    def __init__(self, auv_id: str, version: str, boats: list[tuple[int, int]]) -> None:
        self.id = auv_id
        self.version = version
        self.boats = boats
        self.x: int | None = None
        self.y: int | None = None
        self.dir: str | None = None
        self.tile: str = "empty"
        # Unsent tails of earlier runs' files, oldest first. The live file's
        # tail is what lies past `log.through`.
        self.pending: list[Tail] = []

    def observe(self, state: dict[str, Any]) -> None:
        self.x, self.y, self.dir = state["x"], state["y"], state["dir"]
        self.tile = state.get("tile", "empty")

    def decide(self) -> str:
        choice = random.randint(0, 2)
        if choice == 0:
            return random.choice(["turn_left", "turn_right"])
        if choice == 1:
            return "plant" if self.tile == "empty" else "move"
        return "move"

    def snapshot(self, seq: int) -> Snapshot:
        return Snapshot(
            id=self.id,
            x=self.x,
            y=self.y,
            dir=self.dir,
            seq=seq,
            ts=time.time(),
            version=self.version,
        )


# offload either one pending file or current log file. Handle one at a time to simulate bandwidth requirements
async def offload(auv: Auv, publish: Publish, log: HullLog) -> None:
    tail = auv.pending[0] if auv.pending else None
    # if we have pending logs that need offloaded from before auv start
    if tail:
        path, start, limit = (
            tail.path,
            tail.start,
            min(SPAN_BYTES, tail.end - tail.start),
        )
    # if the current log is all we need to offload
    elif log.through < log.size:
        path, start, limit = log.path, log.through, SPAN_BYTES
    else:
        return

    data = slice(path, start, limit)

    # write metadata for stitching for the boat server
    span = Span(id=auv.id, file=path.name, offset=start)

    await publish(f"fleet.{auv.id}.offload", data, span.headers())
    end = start + len(data)

    log.write(Offload(id=auv.id, ts=time.time(), file=path.name, through=end))

    if tail:
        tail.start = end
        if tail.start >= tail.end:
            auv.pending.pop(0)
    else:
        log.through = end


async def run(auv: Auv, host: str, port: int, log: HullLog) -> None:
    # `docker stop` sends SIGTERM; handles the cancel by closing the log instead of the process dying in the middle
    asyncio.get_running_loop().add_signal_handler(
        signal.SIGTERM, asyncio.current_task().cancel
    )
    try:
        await _drive(auv, host, port, log)
    finally:
        log.close()
        print(f"log closed: {log.path}")


async def _drive(auv: Auv, host: str, port: int, log: HullLog) -> None:
    reader, writer = await asyncio.open_connection(host, port)

    register = {"type": "register", "id": auv.id, "version": auv.version}
    writer.write(json.dumps(register).encode() + b"\n")
    await writer.drain()

    nats_url = os.environ.get("NATS_URL", "nats://localhost:4222")
    nc = await nats.connect(nats_url)

    js = nc.jetstream()
    stream_ready = False

    async def publish_acked(subject: str, data: bytes, headers: dict[str, str]) -> None:
        await js.publish(subject, data, timeout=0.3, headers=headers)

    while True:
        line = await reader.readline()
        if not line:
            break
        msg = json.loads(line)

        # previous intent was rejected, do not update internal state
        if msg["type"] == "reject":
            print(f"rejected: {msg['reason']}")
            break

        # sim tells AUV what it's true position is
        if msg["type"] == "tick":
            auv.observe(msg["you"])

            snap = auv.snapshot(msg["seq"])
            log.write(snap)

            heartbeat = (
                Heartbeat(id=auv.id, ts=snap.ts, version=auv.version)
                if msg["seq"] % 10 == 0
                else None
            )
            if heartbeat:
                log.write(heartbeat)

            try:
                # send a hearbeat message over NATs intended for boat server. In real world this would be an acoustic com
                if heartbeat:
                    await nc.publish(
                        f"fleet.{auv.id}.heartbeat",
                        heartbeat.model_dump_json().encode(),
                    )
                # if within one tile of a boat, offload mcap log to boat
                if any(
                    abs(auv.x - bx) <= 1 and abs(auv.y - by) <= 1
                    for bx, by in auv.boats
                ):
                    if not stream_ready:
                        await ensure_stream(js)
                        stream_ready = True
                    await offload(auv, publish_acked, log)
            except Exception:
                pass  # NATS down — AUV keeps running, the unsent tail grows

            intent = Intent(id=auv.id, seq=msg["seq"], action=auv.decide())

            log.write(intent)

            writer.write(intent.model_dump_json().encode() + b"\n")
            await writer.drain()


def main(auv_cls: type[Auv], label: str) -> None:
    # argv first so the process is self-describing: pkill -f "auv.py auv-2"
    cli_id = sys.argv[1] if len(sys.argv) > 1 else None

    fleet_data = json.loads(Path(__file__).parent.joinpath("fleet.json").read_text())
    auv = auv_cls(
        cli_id or os.environ.get("AUV_ID") or str(uuid.uuid4()),
        os.environ.get("AUV_VERSION", "dev"),
        [(b["x"], b["y"]) for b in fleet_data.get("boats", [])],
    )

    host = os.environ.get("WORLD_HOST", "localhost")
    port = int(os.environ.get("WORLD_PORT", "9000"))

    log_dir = Path(os.environ.get("LOG_DIR", "logs"))

    earlier = sorted(log_dir.glob(f"{auv.id}_*.mcap"))

    auv.pending = unsent(earlier)

    now = time.time()
    stamp = (
        time.strftime("%Y%m%dT%H%M%S", time.localtime(now))
        + f"_{int(now * 1e6) % 1_000_000:06d}"
    )
    log = HullLog(log_dir / f"{auv.id}_{stamp}.mcap")
    print(
        f"AUV {auv.id} [{auv.version}] ({label}) connecting to {host}:{port}, logging to {log.path}"
    )
    print(
        f"restored {sum(t.end - t.start for t in auv.pending)} unsent bytes from {len(earlier)} earlier run(s)"
    )
    try:
        asyncio.run(run(auv, host, port, log))
    except asyncio.CancelledError:
        pass  # SIGTERM: log already closed by run()


if __name__ == "__main__":
    main(Auv, "random")
