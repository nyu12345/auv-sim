import asyncio
import json
import os
import time
from pathlib import Path

import nats
from messages import Heartbeat, Snapshot, Span
from mirror import Mirror
from nats.aio.msg import Msg
from pydantic import BaseModel, ValidationError
from streams import MIRROR, consume, ensure_stream, reader
from websockets.asyncio.server import ServerConnection, serve

FLEET_PATH = Path(__file__).parent / "fleet.json"


def load_names() -> dict[str, str]:
    """id -> name mapping from fleet manifest."""
    data = json.loads(FLEET_PATH.read_text())
    return {a["id"]: a["name"] for a in data["auvs"]}


# Shore POV of AUV activity. Has faults and message drops compared to simulation source of truth
class Shore:
    def __init__(self, names: dict[str, str], mirror: Mirror | None = None) -> None:
        self.names = names
        self.mirror = mirror
        self.fleet: dict[str, dict] = {}

    def apply_offload(self, records: list[BaseModel]) -> None:
        for r in records:
            if not isinstance(r, Snapshot):
                continue
            known = self.fleet.get(r.id, {})
            if r.seq <= known.get("last_seq", -1):
                continue
            self.fleet[r.id] = {
                "x": r.x,
                "y": r.y,
                "dir": r.dir,
                "version": r.version,
                "last_seq": r.seq,
                "last_ts": r.ts,
                "pos_ts": r.ts,
            }

    def apply_heartbeat(self, hb: Heartbeat) -> None:
        if hb.id in self.fleet:
            # Keep the position from the last offload, just update alive timestamp.
            self.fleet[hb.id]["last_ts"] = hb.ts
            self.fleet[hb.id]["version"] = hb.version
        else:
            # First contact: alive but position unknown until it docks.
            self.fleet[hb.id] = {"version": hb.version, "last_ts": hb.ts}

    def picture(self, now: float) -> dict:
        return {
            "auvs": [
                {
                    "id": auv_id,
                    "name": self.names.get(auv_id, auv_id),
                    "x": v.get("x"),
                    "y": v.get("y"),
                    "dir": v.get("dir"),
                    "version": v["version"],
                    "last_seq": v.get("last_seq"),
                    "last_heard": round(now - v["last_ts"], 1),
                    "pos_age": round(now - v["pos_ts"], 1) if "pos_ts" in v else None,
                    "has_position": "x" in v,
                    "log_bytes": self.mirror.size(auv_id) if self.mirror else None,
                }
                for auv_id, v in self.fleet.items()
            ],
        }


def receive(shore: Shore, mirror: Mirror, msg: Msg) -> list[BaseModel]:
    """One offload message: append its span to the mirror, then let whatever
    records it completed move the picture. Malformed headers drop the
    message; redelivery could not mend them."""
    try:
        span = Span.from_headers(msg.headers)
    except ValidationError as e:
        print(
            f"offload: dropped message with bad headers {msg.headers!r}: {e.errors()[0]['msg']}"
        )
        return []
    records = mirror.append(span, msg.data)
    shore.apply_offload(records)
    return records


async def main():
    nats_url = os.environ.get("NATS_URL", "nats://localhost:4222")
    nc = await nats.connect(nats_url)
    js = nc.jetstream()
    # Shore's OFFLOAD is a mirror of the ship's, fed over the leafnode. It is
    # created here, on shore's server, whether or not the ship is reachable.
    await ensure_stream(js, MIRROR)

    # The mirror is shore's archive and, after a restart, the source of its
    # picture: every hull's last known position is read back from disk.
    mirror = Mirror(Path(os.environ.get("SHORE_DIR", "shore_data")))
    shore = Shore(load_names(), mirror)
    for path in mirror.files():
        shore.apply_offload(mirror.replay(path))
    print(
        f"mirror: {len(mirror.files())} file(s) replayed from {mirror.root}, {len(shore.fleet)} AUVs known"
    )
    viewers: set[ServerConnection] = set()

    async def push():
        if not viewers:
            return
        msg = json.dumps(shore.picture(time.time()))
        for ws in list(viewers):
            try:
                await ws.send(msg)
            except Exception:
                viewers.discard(ws)

    async def on_offload(msg):
        records = receive(shore, mirror, msg)
        print(
            f"offload: {len(msg.data)} bytes, {len(records)} records — fleet has {len(shore.fleet)} AUVs"
        )
        await push()

    async def on_heartbeat(msg):
        try:
            hb = Heartbeat.model_validate_json(msg.data)
        except ValidationError as e:
            print(
                f"heartbeat: dropped malformed message {msg.data!r}: {e.errors()[0]['msg']}"
            )
            return
        shore.apply_heartbeat(hb)

    # Core NATS interest crosses the leafnode by itself: while the link is up
    # a heartbeat published on the ship arrives here; while it is down, it
    # does not, which is exactly what a heartbeat should do.
    await nc.subscribe("fleet.*.heartbeat", cb=on_heartbeat)
    # Durable, on the mirror: the server remembers what shore has acked, so a
    # restart resumes from the first unread span instead of starting blind.
    offloads = await reader(js, "fleet.*.offload", durable="shore")
    asyncio.create_task(consume(offloads, on_offload))

    async def viewer(ws: ServerConnection):
        viewers.add(ws)
        try:
            async for _ in ws:
                pass
        finally:
            viewers.discard(ws)

    ws_server = await serve(viewer, "0.0.0.0", 8766)
    print("shore up — ws:8766")

    # Push periodically so staleness ticks up even when no new data arrives.
    while True:
        await push()
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
