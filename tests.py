"""Run with: python3 tests.py [unit|integration]"""

# Harness builds its servers in __aenter__ because they need a running event loop,
# so they cannot be assigned in __init__.
# pyright: reportUninitializedInstanceVariable=false

import asyncio
import contextlib
import io
import json
import os
import random
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from collections.abc import Callable
from typing import Any

import websockets
from pydantic import BaseModel, ValidationError
from websockets.asyncio.server import Server, ServerConnection, serve

SIM = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sim")
sys.path.insert(0, SIM)

import dashboard  # noqa: E402
import server  # noqa: E402
from auv import Auv  # noqa: E402
from auv_homing import DOCK_TICKS, RANDOM_TICKS, HomingAuv  # noqa: E402
from auv_straight import StraightAuv  # noqa: E402
from deploy import deploy_command  # noqa: E402
from log import HullLog, Tail, unsent  # noqa: E402
from log import read as read_log  # noqa: E402
from log import slice as slice_log  # noqa: E402
from messages import Heartbeat, Intent, Offload, Snapshot, Span  # noqa: E402
from server import CardinalDirection as C  # noqa: E402
from server import Boat, FLEET_PATH, Vehicle, World, load_fleet  # noqa: E402
from mirror import Decoder, Mirror  # noqa: E402
from shore import Shore  # noqa: E402
from streams import consume, reader  # noqa: E402

Test = Callable[[], Any]
UNIT: list[Test] = []
INTEGRATION: list[Test] = []


def unit(fn):
    UNIT.append(fn)
    return fn


def integration(fn):
    INTEGRATION.append(fn)
    return fn


def world_of(spec: dict[str, tuple[int, int, C]]) -> World:
    return World({i: Vehicle(i, i, x, y, d) for i, (x, y, d) in spec.items()}, {})


def step(world: World, intents: dict[str, str]) -> dict[str, tuple[int, int]]:
    world.intents = dict(intents)
    world._apply()
    return {v.id: (v.x, v.y) for v in world.vehicles.values()}


# ---------------------------------------------------------------- unit


@unit
def load_fleet_reads_spawns():
    fleet, boats = load_fleet(FLEET_PATH)
    assert len(fleet) == 3
    assert (fleet["auv-1"].x, fleet["auv-1"].y) == (10, 10)
    assert (fleet["auv-2"].x, fleet["auv-2"].y) == (50, 50)
    assert fleet["auv-3"].dir is C.WEST
    assert all(not v.connected for v in fleet.values())
    assert len(boats) == 1
    assert boats["boat-1"].name == "Leviathan"


@unit
def move_goes_the_right_way():
    for d, want in [(C.NORTH, (5, 6)), (C.SOUTH, (5, 4)), (C.EAST, (6, 5)), (C.WEST, (4, 5))]:
        got = step(world_of({"a": (5, 5, d)}), {"a": "move"})["a"]
        assert got == want, f"{d.value}: got {got}, want {want}"


@unit
def turns_are_not_mirrored():
    # Enum is declared clockwise, so rotate(+1) must be a right turn.
    for start, left, right in [
        (C.NORTH, C.WEST, C.EAST),
        (C.EAST, C.NORTH, C.SOUTH),
        (C.SOUTH, C.EAST, C.WEST),
        (C.WEST, C.SOUTH, C.NORTH),
    ]:
        w = world_of({"a": (5, 5, start)})
        step(w, {"a": "turn_left"})
        assert w.vehicles["a"].dir is left, f"left from {start.value}"
        w = world_of({"a": (5, 5, start)})
        step(w, {"a": "turn_right"})
        assert w.vehicles["a"].dir is right, f"right from {start.value}"


@unit
def edges_clamp():
    for pos, d in [((0, 0), C.SOUTH), ((0, 0), C.WEST), ((99, 99), C.NORTH), ((99, 99), C.EAST)]:
        assert step(world_of({"a": (*pos, d)}), {"a": "move"})["a"] == pos


@unit
def intents_are_drained():
    w = world_of({"a": (5, 5, C.NORTH)})
    step(w, {"a": "move"})
    assert w.intents == {}


@unit
def swap_is_refused():
    # a and b face each other; neither may pass through the other
    r = step(world_of({"a": (1, 1, C.NORTH), "b": (1, 2, C.SOUTH)}), {"a": "move", "b": "move"})
    assert r == {"a": (1, 1), "b": (1, 2)}


@unit
def contested_cell_has_one_winner():
    r = step(world_of({"a": (5, 4, C.NORTH), "b": (4, 5, C.EAST)}), {"a": "move", "b": "move"})
    assert len(set(r.values())) == 2, f"overlap: {r}"


@unit
def stationary_vehicle_blocks():
    r = step(world_of({"a": (1, 1, C.NORTH), "b": (1, 2, C.NORTH)}), {"a": "move"})
    assert r["a"] == (1, 1)


@unit
def downed_vehicle_still_blocks():
    # A rebooting AUV is still physically in the water.
    w = world_of({"a": (1, 1, C.NORTH), "b": (1, 2, C.NORTH)})
    assert not w.vehicles["b"].connected
    assert step(w, {"a": "move"})["a"] == (1, 1)


@unit
def chain_never_overlaps():
    r = step(world_of({"a": (1, 1, C.NORTH), "b": (1, 2, C.NORTH)}), {"a": "move", "b": "move"})
    assert len(set(r.values())) == 2, f"overlap: {r}"


@unit
def no_overlap_under_contention():
    random.seed(7)
    w = World({
        f"v{i}": Vehicle(f"v{i}", f"v{i}", 50 + i % 3, 50 + i // 3, random.choice(list(C)))
        for i in range(9)
    }, {})
    for tick in range(3000):
        w.intents = {i: random.choice(["move", "move", "turn_left", "turn_right"]) for i in w.vehicles}
        w._apply()
        pos = [(v.x, v.y) for v in w.vehicles.values()]
        assert len(pos) == len(set(pos)), f"tick {tick}: overlap"
        assert all(0 <= x < 100 and 0 <= y < 100 for x, y in pos), f"tick {tick}: out of bounds"


@unit
def identical_runs_are_identical():
    def run():
        random.seed(99)
        w = World({
            f"v{i}": Vehicle(f"v{i}", f"v{i}", 50 + i % 3, 50 + i // 3, C.NORTH) for i in range(9)
        }, {})
        for _ in range(200):
            w.intents = {i: random.choice(["move", "turn_left", "turn_right"]) for i in w.vehicles}
            w._apply()
        return sorted((v.id, v.x, v.y, v.dir.value) for v in w.vehicles.values())

    assert run() == run()


# ------------------------------------------------------- unit: messages


SNAP = {"id": "auv-1", "x": 4, "y": 9, "dir": "E", "seq": 40, "ts": 1700000000.5, "version": "v2"}


@unit
def messages_round_trip_through_json():
    for model, fields in [
        (Snapshot, SNAP),
        (Heartbeat, {"id": "auv-1", "ts": 1.5, "version": "v1"}),
        (Intent, {"type": "intent", "id": "auv-1", "seq": 3, "action": "turn_left"}),
        (Offload, {"id": "auv-1", "ts": 2.0, "file": "auv-1_run1.mcap", "through": 4096}),
    ]:
        msg = model(**fields)
        assert model.model_validate_json(msg.model_dump_json()) == msg
        assert msg.model_dump() == fields


@unit
def span_round_trips_through_headers_and_refuses_a_path():
    span = Span(id="auv-1", file="auv-1_run1.mcap", offset=4096)
    headers = span.headers()
    assert all(isinstance(v, str) for v in headers.values()), "NATS headers are strings"
    assert Span.from_headers(headers) == span
    for bad in [
        {**headers, "file": "../shore.py"},
        {**headers, "file": "/etc/passwd"},
        {**headers, "id": "../auv-2"},
        {**headers, "file": ""},
        {**headers, "offset": "-1"},
        {k: v for k, v in headers.items() if k != "offset"},
        None,  # a message with no headers at all
    ]:
        try:
            Span.from_headers(bad)
        except ValidationError:
            continue
        raise AssertionError(f"accepted {bad}")


@unit
def messages_reject_bad_fields():
    for bad in [
        {**SNAP, "dir": "X"},
        {**SNAP, "x": "four"},
        {k: v for k, v in SNAP.items() if k != "seq"},
    ]:
        try:
            Snapshot(**bad)
        except ValidationError:
            continue
        raise AssertionError(f"accepted {bad}")
    try:
        Intent(id="a", seq=1, action="fly")
    except ValidationError:
        pass
    else:
        raise AssertionError("accepted an unknown action")


@unit
def snapshot_schema_requires_every_field():
    schema = Snapshot.model_json_schema()
    assert set(schema["required"]) == set(SNAP)
    assert schema["properties"]["dir"]["enum"] == ["N", "E", "S", "W"]


def hull_file(d: str, name: str = "auv-1_run1.mcap") -> tuple[Path, list[BaseModel]]:
    """A closed hull file holding every model, and the records it holds."""
    written: list[BaseModel] = [snaps(1)[0], Intent(id="auv-1", seq=0, action="move"),
                                Heartbeat(id="auv-1", ts=1.0, version="v2"), *snaps(4, start=1),
                                Offload(id="auv-1", ts=2.0, file=name, through=4096), *snaps(2, start=5)]
    return journal(Path(d) / name, *written), written


@unit
def decoder_yields_the_same_records_however_the_bytes_are_cut():
    with tempfile.TemporaryDirectory() as d:
        path, written = hull_file(d)
        raw = path.read_bytes()
        assert Decoder().feed(raw) == written, "whole file at once"
        one_at_a_time = Decoder()
        got = [r for b in raw for r in one_at_a_time.feed(bytes([b]))]
        assert got == written, "one byte at a time"
        assert one_at_a_time._buf == b"", "footer and closing magic fully consumed"
        random.seed(4)
        cuts = sorted(random.sample(range(1, len(raw)), 20))
        chunks = [raw[a:b] for a, b in zip([0, *cuts], [*cuts, len(raw)])]
        ragged = Decoder()
        assert [r for c in chunks for r in ragged.feed(c)] == written, "random spans"


@unit
def decoder_drops_a_malformed_record_and_keeps_the_rest():
    with tempfile.TemporaryDirectory() as d:
        log = HullLog(Path(d) / "hull.mcap")
        good = snaps(3)
        log.write(good[0])
        # A snapshot record whose body does not satisfy the Snapshot schema.
        log._writer.add_message(channel_id=log._channels[Snapshot], log_time=0, publish_time=0,
                                data=json.dumps({**SNAP, "dir": "up"}).encode())
        log.write(good[1])
        log._writer.add_message(channel_id=log._channels[Snapshot], log_time=0, publish_time=0, data=b'{"id": 1}')
        log.write(good[2])
        log.close()
        decoder = Decoder()
        assert decoder.feed(log.path.read_bytes()) == good
        assert decoder.dropped == 2


@unit
def mirror_appends_contiguous_spans_and_skips_what_it_already_holds():
    with tempfile.TemporaryDirectory() as d:
        path, written = hull_file(d)
        raw = path.read_bytes()
        mirror = Mirror(Path(d) / "shore")
        span = lambda offset: Span(id="auv-1", file=path.name, offset=offset)  # noqa: E731
        cut = len(raw) // 2
        first = mirror.append(span(0), raw[:cut])
        assert mirror.size("auv-1") == cut and mirror.size("auv-2") == 0
        assert mirror.append(span(0), raw[:cut]) == [], "re-sent span is skipped"
        assert mirror.append(span(10), raw[10:cut - 5]) == [], "a span inside what is held is skipped"
        assert mirror.size("auv-1") == cut, "skips write nothing"
        assert mirror.append(span(cut + 1), raw[cut + 1:]) == [], "a gap is refused"
        assert mirror.size("auv-1") == cut
        overlap = mirror.append(span(cut - 7), raw[cut - 7:])  # hull crashed after the ack: re-sends from earlier
        assert first + overlap == written and mirror.path(span(0)).read_bytes() == raw, "byte for byte"
        assert mirror.files() == [mirror.path(span(0))]


@unit
def shore_drops_a_message_with_bad_headers_and_mirrors_a_good_one():
    from shore import receive

    class Msg:
        def __init__(self, data, headers):
            self.data, self.headers = data, headers

    with tempfile.TemporaryDirectory() as d:
        path, written = hull_file(d)
        mirror = Mirror(Path(d) / "shore")
        s = Shore({}, mirror)
        raw = path.read_bytes()
        assert receive(s, mirror, Msg(raw, None)) == [], "old JSON batches had no headers"
        assert receive(s, mirror, Msg(raw, {"id": "auv-1", "file": "../x.mcap", "offset": "0"})) == []
        assert mirror.files() == [] and s.fleet == {}
        assert receive(s, mirror, Msg(raw, Span(id="auv-1", file=path.name, offset=0).headers())) == written
        assert s.fleet["auv-1"]["last_seq"] == 6 and s.picture(0.0)["auvs"][0]["log_bytes"] == len(raw)


@unit
def mirror_decodes_a_late_span_against_schemas_that_landed_before_a_restart():
    with tempfile.TemporaryDirectory() as d:
        path, written = hull_file(d)
        raw = path.read_bytes()
        cut = len(raw) - 300  # mid-record, well after the schemas and channels
        span = lambda offset: Span(id="auv-1", file=path.name, offset=offset)  # noqa: E731
        before = Mirror(Path(d) / "shore").append(span(0), raw[:cut])
        after = Mirror(Path(d) / "shore")  # shore restarted: fresh decoders, same disk
        assert after.replay(after.path(span(0))) == before, "the picture is rebuilt from disk"
        assert before + after.append(span(cut), raw[cut:]) == written


@unit
def shore_picture_is_only_what_got_through():
    s = Shore({"auv-1": "Mako-01"})
    s.apply_heartbeat(Heartbeat(id="auv-2", ts=100.0, version="v1"))
    s.apply_offload([Snapshot(**{**SNAP, "ts": 90.0}), Snapshot(**{**SNAP, "seq": 41, "ts": 95.0, "x": 5})])
    by = {a["id"]: a for a in s.picture(now=100.0)["auvs"]}
    a1, a2 = by["auv-1"], by["auv-2"]
    assert a1["name"] == "Mako-01" and a1["has_position"] and (a1["x"], a1["last_seq"]) == (5, 41), "last record wins"
    assert (a1["last_heard"], a1["pos_age"], a1["version"]) == (5.0, 5.0, "v2")
    assert a2["name"] == "auv-2" and not a2["has_position"] and a2["pos_age"] is None and a2["last_heard"] == 0.0
    # A later heartbeat refreshes liveness and version but never touches position.
    s.apply_heartbeat(Heartbeat(id="auv-1", ts=110.0, version="v3"))
    a1 = next(a for a in s.picture(now=110.0)["auvs"] if a["id"] == "auv-1")
    assert (a1["x"], a1["version"], a1["last_heard"], a1["pos_age"]) == (5, "v3", 0.0, 15.0)


@unit
def shore_never_moves_a_hull_backwards():
    """A re-sent batch (hull crashed after publishing) or one delivered out of
    order must not overwrite a newer position."""
    s = Shore({})
    s.apply_offload([Snapshot(**{**SNAP, "seq": 50, "x": 9, "ts": 100.0})])
    s.apply_offload([Snapshot(**{**SNAP, "seq": 40, "x": 4, "ts": 90.0}), Snapshot(**{**SNAP, "seq": 50, "x": 9, "ts": 100.0})])
    a = s.picture(now=100.0)["auvs"][0]
    assert (a["x"], a["last_seq"], a["pos_age"]) == (9, 50, 0.0)
    s.apply_offload([Snapshot(**{**SNAP, "seq": 51, "x": 10, "ts": 101.0})])
    assert s.picture(now=101.0)["auvs"][0]["x"] == 10, "newer still applies"


# ------------------------------------------------------------ unit: log


def snaps(n: int, start: int = 0) -> list[Snapshot]:
    return [Snapshot(**{**SNAP, "seq": start + i, "x": i}) for i in range(n)]


@unit
def log_round_trips_every_model_in_order():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "hull.mcap"
        written = [snaps(1)[0], Intent(id="auv-1", seq=0, action="move"),
                   Heartbeat(id="auv-1", ts=1.0, version="v2"), *snaps(4, start=1),
                   Offload(id="auv-1", ts=2.0, file="hull.mcap", through=4096)]
        log = HullLog(path)
        for m in written:
            log.write(m)
        log.close()
        assert list(read_log(path)) == written


@unit
def log_survives_a_crash_without_close():
    """The flight-recorder property: a hull killed mid-tick leaves every
    flushed record readable. No close(), no finish(), no summary."""
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "hull.mcap"
        log = HullLog(path)
        for m in snaps(50):
            log.write(m)
        # Drop the writer on the floor; only what reached the OS counts.
        got = list(read_log(path))
        assert [s.seq for s in got] == list(range(50))
        del log


@unit
def log_never_overwrites_an_existing_file():
    with tempfile.TemporaryDirectory() as d:
        path = journal(Path(d) / "hull.mcap", *snaps(3))
        try:
            HullLog(path)
        except FileExistsError:
            pass
        else:
            raise AssertionError("opened over an existing log")
        assert [s.seq for s in read_log(path)] == [0, 1, 2], "earlier run untouched"


@unit
def log_attaches_the_pydantic_schema_to_each_channel():
    from mcap.reader import make_reader
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "hull.mcap"
        log = HullLog(path)
        log.write(snaps(1)[0])
        log.close()
        with path.open("rb") as f:
            summary = make_reader(f).get_summary()
        assert summary is not None, "a closed file must be indexed"
        by_name = {s.name: s for s in summary.schemas.values()}
        assert set(by_name) == {"Snapshot", "Intent", "Heartbeat", "Offload"}
        assert json.loads(by_name["Snapshot"].data) == Snapshot.model_json_schema()
        assert {c.topic for c in summary.channels.values()} == {"/snapshot", "/intent", "/heartbeat", "/offload"}


@unit
def log_size_is_the_end_of_the_last_flushed_record_and_slice_reads_it():
    with tempfile.TemporaryDirectory() as d:
        log = HullLog(Path(d) / "hull.mcap")
        assert log.through == 0
        header_end = log.size
        assert header_end > 8, "magic, header, schemas and channels are already on disk"
        marks = []
        for s in snaps(3):
            log.write(s)
            marks.append(log.size)
        assert marks == sorted(set(marks)) and marks[0] > header_end
        # The live file is readable by a second handle while the writer holds it.
        whole = slice_log(log.path, 0, 10**9)
        assert len(whole) == log.size and whole[:8] == b"\x89MCAP0\r\n"
        assert slice_log(log.path, marks[0], marks[1] - marks[0]) == whole[marks[0]:marks[1]]
        assert slice_log(log.path, marks[1], 5) == whole[marks[1]:marks[1] + 5], "limit caps a span"
        assert slice_log(log.path, log.size, 100) == b"", "nothing past the end"
        log.close()
        assert log.size > marks[-1], "close appends the summary"


def journal(path: Path, *records: BaseModel, close: bool = True) -> Path:
    log = HullLog(path)
    for r in records:
        log.write(r)
    if close:
        log.close()
    return path


def acked(file: str, through: int) -> Offload:
    return Offload(id="auv-1", ts=0.0, file=file, through=through)


@unit
def unsent_is_every_byte_no_offload_record_has_claimed():
    with tempfile.TemporaryDirectory() as d:
        run1 = HullLog(Path(d) / "auv-1_run1.mcap")
        for s in snaps(5):
            run1.write(s)
        docked = run1.size
        run1.write(acked(run1.path.name, docked))
        for s in snaps(3, start=5):
            run1.write(s)
        crashed = run1.size  # no close(): no footer
        run2 = journal(Path(d) / "auv-1_run2.mcap", *snaps(2, start=8))  # never docked
        assert unsent([run1.path, run2]) == [Tail(run1.path, docked, crashed), Tail(run2, 0, run2.stat().st_size)]
        assert unsent([run2]) == [Tail(run2, 0, run2.stat().st_size)], "one file on its own"
        # A later run ships its predecessors' tails and journals that in its own file.
        run3 = journal(Path(d) / "auv-1_run3.mcap", acked(run1.path.name, crashed), acked(run2.name, 100), *snaps(1, start=10))
        tails = unsent([run1.path, run2, run3])
        assert [(t.path.name, t.start) for t in tails] == [("auv-1_run2.mcap", 100), ("auv-1_run3.mcap", 0)]
        assert tails[1].end == run3.stat().st_size, "a closed file's tail runs through its footer"
        run4 = journal(Path(d) / "auv-1_run4.mcap", acked(run2.name, 50), acked(run3.name, run3.stat().st_size))
        assert [(t.path.name, t.start) for t in unsent([run1.path, run2, run3, run4])] == [
            ("auv-1_run2.mcap", 100), ("auv-1_run4.mcap", 0)], "a smaller claim never moves a file backwards"
        assert unsent([]) == []


@unit
async def offload_ships_earlier_runs_first_and_journals_only_after_the_ack():
    import auv as hull

    async def deliver(subject, data, headers):
        sent.append((subject, data, Span.from_headers(headers)))

    async def drop(subject, data, headers):
        raise ConnectionError("NATS down")

    with tempfile.TemporaryDirectory() as d:
        sent = []
        a = Auv("auv-1", "v1", [BOAT])
        old = journal(Path(d) / "auv-1_run0.mcap", *snaps(3))  # an earlier run that never docked
        a.pending = unsent([old])
        log = HullLog(Path(d) / "auv-1_run1.mcap")
        for s in snaps(2, start=3):
            log.write(s)

        try:
            await hull.offload(a, drop, log)
        except ConnectionError:
            pass
        assert a.pending == unsent([old]) and log.through == 0, "a failed publish moves nothing"
        assert not any(isinstance(r, Offload) for r in read_log(log.path)), "and journals nothing"

        await hull.offload(a, deliver, log)
        subject, data, span = sent[0]
        assert subject == "fleet.auv-1.offload" and data == old.read_bytes()
        assert span == Span(id="auv-1", file=old.name, offset=0)
        assert a.pending == [], "the earlier file went out whole"
        marks = [r for r in read_log(log.path) if isinstance(r, Offload)]
        assert [(m.file, m.through) for m in marks] == [(old.name, len(data))], "journaled in the live file"

        live = log.size  # includes the Offload record just journaled
        await hull.offload(a, deliver, log)
        _, data, span = sent[1]
        assert span == Span(id="auv-1", file=log.path.name, offset=0) and len(data) == live
        assert log.through == live, "the live file's offset moves on the ack"
        assert log.through < log.size, "the journal record for that span is itself unsent"

        await hull.offload(a, deliver, log)
        _, data, span = sent[2]
        assert span.offset == live and log.through == live + len(data), "next span is just that record"
        assert log.through < log.size, "and it too gets journaled, so the file never fully catches up while open"
        log.close()
        tails = unsent([old, log.path])  # old's delivery is journaled in run1, so old is not pending
        assert [(t.path.name, t.start) for t in tails] == [(log.path.name, log.through)], "only the footer is left"


@unit
async def offload_caps_a_span_and_the_mirror_reassembles_the_file():
    import auv as hull

    with tempfile.TemporaryDirectory() as d:
        mirror = Mirror(Path(d) / "shore")
        got: list[BaseModel] = []

        async def deliver(subject, data, headers):
            got.extend(mirror.append(Span.from_headers(headers), data))

        a = Auv("auv-1", "v1", [BOAT])
        old, written = hull_file(d, "auv-1_run0.mcap")
        # The fixture's journal says its first 4096 bytes got out, so shore holds them already.
        got.extend(mirror.append(Span(id="auv-1", file=old.name, offset=0), old.read_bytes()[:4096]))
        a.pending = unsent([old])
        assert a.pending[0].start == 4096
        log = HullLog(Path(d) / "auv-1_run1.mcap")
        hull.SPAN_BYTES = 100
        try:
            for _ in range(200):
                await hull.offload(a, deliver, log)
                if not a.pending:
                    break
            assert not a.pending, "200 spans of 100 bytes should cover a small file"
            assert mirror.path(Span(id="auv-1", file=old.name, offset=0)).read_bytes() == old.read_bytes()
            assert got == written, "shore decoded every record across the cuts"
        finally:
            hull.SPAN_BYTES = 256 * 1024
            log.close()


# ----------------------------------------------------- unit: behaviours


BOAT = (50, 50)


def homing_at(x: int, y: int, d: str) -> HomingAuv:
    a = HomingAuv("a", "dev", [BOAT])
    a.observe({"x": x, "y": y, "dir": d})
    a.phase = "homing"
    return a


def drive(auv: Auv, world: World, ticks: int) -> None:
    """Close the loop: the behaviour decides, the real world applies it."""
    v = world.vehicles[auv.id]
    for _ in range(ticks):
        auv.observe({"x": v.x, "y": v.y, "dir": v.dir.value})
        step(world, {auv.id: auv.decide()})


@unit
def random_auv_emits_only_known_actions():
    a = Auv("a", "dev", [BOAT])
    a.observe({"x": 5, "y": 5, "dir": "N"})
    assert {a.decide() for _ in range(200)} == {"move", "turn_left", "turn_right"}


@unit
def snapshot_carries_what_was_observed():
    a = Auv("auv-1", "v2", [BOAT])
    a.observe({"x": 7, "y": 3, "dir": "W"})
    s = a.snapshot(12)
    assert isinstance(s, Snapshot)
    assert (s.id, s.x, s.y, s.dir, s.seq, s.version) == ("auv-1", 7, 3, "W", 12, "v2")
    assert abs(s.ts - time.time()) < 1


@unit
def straight_auv_never_turns():
    a = StraightAuv("a", "dev", [BOAT])
    a.observe({"x": 5, "y": 5, "dir": "N"})
    assert all(a.decide() == "move" for _ in range(50))


@unit
def homing_wanders_for_exactly_random_ticks():
    a = HomingAuv("a", "dev", [BOAT])
    a.observe({"x": 5, "y": 5, "dir": "N"})
    for _ in range(RANDOM_TICKS - 1):
        assert a.decide() in {"move", "turn_left", "turn_right"}
        assert a.phase == "random"
    a.decide()
    assert a.phase == "homing"


@unit
def homing_prefers_the_farther_axis():
    for pos, d, want in [
        ((10, 30), "N", "E"),   # 40 east, 20 north -> east
        ((30, 10), "E", "N"),   # 20 east, 40 north -> north
        ((90, 60), "N", "W"),   # 40 west, 10 south -> west
        ((60, 90), "W", "S"),   # 10 west, 40 south -> south
        ((50, 10), "E", "N"),   # aligned on x -> only y matters
        ((10, 50), "N", "E"),   # aligned on y -> only x matters
    ]:
        got = homing_at(*pos, d)._desired_direction()
        assert got == want, f"at {pos}: got {got}, want {want}"


@unit
def homing_turns_the_short_way():
    for start, target, want in [
        ("N", "N", "move"),
        ("N", "E", "turn_right"),
        ("N", "W", "turn_left"),
        ("N", "S", "turn_right"),  # 180: either way; must be deterministic
        ("W", "N", "turn_right"),
        ("E", "N", "turn_left"),
    ]:
        got = homing_at(5, 5, start)._turn_toward(target)
        assert got == want, f"{start}->{target}: got {got}, want {want}"


@unit
def homing_reaches_the_boat_through_real_physics():
    for start in [(10, 30, C.NORTH), (90, 60, C.SOUTH), (50, 10, C.WEST), (10, 50, C.EAST), (49, 49, C.NORTH)]:
        w = world_of({"a": start})
        a = homing_at(start[0], start[1], start[2].value)
        # Farther-axis-first zigzags along the diagonal (up to one turn per
        # move once the gaps are near-equal), so the budget is 2x Manhattan.
        budget = 2 * (abs(BOAT[0] - start[0]) + abs(BOAT[1] - start[1])) + 4
        for _ in range(budget):
            drive(a, w, 1)
            if a.phase == "docked":
                break
        v = w.vehicles["a"]
        assert (v.x, v.y) == BOAT and a.phase == "docked", f"from {start}: ended at {(v.x, v.y)}"


@unit
def homing_holds_on_the_boat_then_resumes():
    w = world_of({"a": (*BOAT, C.NORTH)})
    a = homing_at(*BOAT, "N")
    drive(a, w, 1)
    assert a.phase == "docked"
    v = w.vehicles["a"]
    for _ in range(DOCK_TICKS - 1):
        drive(a, w, 1)
        assert (v.x, v.y, v.dir) == (*BOAT, C.NORTH), "must not move while docked"
        assert a.phase == "docked"
    drive(a, w, 1)
    assert a.phase == "random"


@unit
def homing_cycle_repeats():
    w = world_of({"a": (10, 10, C.NORTH)})
    a = HomingAuv("a", "dev", [BOAT])
    seen = []
    for _ in range(2 * (RANDOM_TICKS + 100 + DOCK_TICKS)):
        drive(a, w, 1)
        if not seen or seen[-1] != a.phase:
            seen.append(a.phase)
    assert seen[:6] == ["random", "homing", "docked", "random", "homing", "docked"], seen


# --------------------------------------------------------- unit: deploy


@unit
def deploy_builds_the_documented_compose_command():
    argv, env = deploy_command("auv-2", "v3", {"auv-1", "auv-2"}, ["v1", "v2", "v3"])
    assert argv == ["docker", "compose", "up", "-d", "--no-deps", "auv-2"]
    assert env["AUV2_VERSION"] == "v3"
    assert "AUV1_VERSION" not in env or env["AUV1_VERSION"] == os.environ.get("AUV1_VERSION")


@unit
def deploy_refuses_unknown_targets():
    ids, versions = {"auv-1"}, ["v1"]
    for auv_id, version in [("auv-9", "v1"), ("auv-1", "v9"), ("auv-1; rm -rf /", "v1"), ("auv-1", "v1 && true")]:
        try:
            deploy_command(auv_id, version, ids, versions)
        except ValueError:
            continue
        raise AssertionError(f"accepted {auv_id!r} {version!r}")


# ------------------------------------------------------ unit: dashboard


async def until(cond, what: str, timeout: float = 5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


@unit
async def subscribe_survives_a_missing_server_and_reconnects_after_drops():
    """Each connection serves exactly one message then hangs up, so three
    messages received means two reconnects happened. The task is started
    before the server exists, so a refused connection must also be survived."""
    got: list[dict] = []

    async def one_shot(ws: ServerConnection):
        await ws.send(json.dumps({"n": len(got)}))

    # Bind first to learn the port, then close, so the subscriber sees ECONNREFUSED.
    probe = await serve(one_shot, "127.0.0.1", 0)
    port = probe.sockets[0].getsockname()[1]
    probe.close()
    await probe.wait_closed()

    task = asyncio.create_task(dashboard.subscribe(f"ws://127.0.0.1:{port}", got.append, backoff=0.05))
    await asyncio.sleep(0.15)
    assert got == [], "nothing to receive while the server is down"

    srv = await serve(one_shot, "127.0.0.1", port)
    try:
        await until(lambda: len(got) >= 3, "three messages across reconnects")
    finally:
        task.cancel()
        srv.close()
    assert got[:3] == [{"n": 0}, {"n": 1}, {"n": 2}]


def auv_at(x, y, d, connected=True, name="Mako", version="v1"):
    return {"id": "a", "name": name, "x": x, "y": y, "dir": d, "connected": connected, "version": version}


@unit
def heading_is_clockwise_from_north_and_passes_numbers_through():
    assert [dashboard.heading_deg(d) for d in "NESW"] == [0, 90, 180, 270]
    assert dashboard.heading_deg(37.5) == 37.5


@unit
def grid_has_one_line_per_cell_edge_each_way():
    svg = dashboard.grid_svg(800)
    assert svg.count("<line ") == 2 * (dashboard.GRID + 1)
    assert 'x1="800"' in svg and 'y1="800"' in svg, "last edge sits on the far side"


@unit
def scene_places_and_rotates_each_vehicle():
    svg = dashboard.scene_svg([auv_at(0, 0, "N"), auv_at(99, 99, "E")], [], size=800)
    assert svg.count("<polygon ") == 2
    # (0,0) is the bottom-left cell; with 8px cells its centre is (4, 796).
    assert 'rotate(0 4 796)' in svg
    assert 'rotate(90 796 4)' in svg


@unit
def scene_marks_downed_vehicles_red_and_boats_yellow():
    live = dashboard.scene_svg([auv_at(5, 5, "N")], [], size=800)
    down = dashboard.scene_svg([auv_at(5, 5, "N", connected=False)], [], size=800)
    assert f'fill="{dashboard.LIVE}"' in live and dashboard.DOWN not in live
    assert f'fill="{dashboard.DOWN}"' in down and f'fill="{dashboard.LIVE}"' not in down
    boat = dashboard.scene_svg([], [{"id": "b", "name": "Leviathan", "x": 50, "y": 50}], size=800)
    assert boat.count("<polygon ") == 1 and f'fill="{dashboard.BOAT}"' in boat
    assert ">Leviathan<" in boat


@unit
def truth_rows_list_boats_first_and_flag_downed_vehicles():
    snap = {
        "seq": 7,
        "auvs": [auv_at(47, 41, "E", connected=False, name="Mako-01", version="v3"),
                 auv_at(50, 56, "S", name="Mako-02", version="v1")],
        "boats": [{"id": "boat-1", "name": "Leviathan", "x": 50, "y": 50}],
    }
    rows = dashboard.truth_rows(snap)
    assert [r["kind"] for r in rows] == ["boat", "auv", "auv"]
    assert rows[0]["pos"] == "(50, 50)" and rows[0]["id"] == "boat-1"
    down, live = rows[1], rows[2]
    assert down["status"] == "down" and down["down"] and down["pos"] == "(47, 41) E" and down["version"] == "v3"
    assert live["status"] == "live" and not live["down"] and live["pos"] == "(50, 56) S"
    assert dashboard.truth_rows({"seq": 0, "auvs": [], "boats": []}) == []


@unit
def staleness_colors_match_the_old_page_thresholds():
    hc, pc = dashboard.heard_class, dashboard.pos_class
    assert [hc(a) for a in (0, 2.9, 3, 14.9, 15, 100)] == [
        "dash-fresh", "dash-fresh", "dash-warn", "dash-warn", "dash-lost", "dash-lost"]
    assert [pc(a) for a in (0, 4.9, 5, 29.9, 30, 1000)] == [
        "dash-fresh", "dash-fresh", "dash-warn", "dash-warn", "dash-lost", "dash-lost"]
    assert pc(None) == "dash-dim", "never offloaded is dim, not lost"


@unit
def shore_rows_report_only_what_shore_believes():
    snap = {"auvs": [
        {"id": "auv-1", "name": "Mako-01", "x": 4, "y": 9, "dir": "E", "version": "v2",
         "last_seq": 40, "last_heard": 1.2, "pos_age": 7.5, "has_position": True, "log_bytes": 548472},
        {"id": "auv-2", "name": "Mako-02", "x": None, "y": None, "dir": None, "version": "v1",
         "last_seq": None, "last_heard": 20.0, "pos_age": None, "has_position": False},
    ]}
    a, b = dashboard.shore_rows(snap)
    assert a["pos"] == "(4, 9) E" and a["age"] == "pos 7.5s ago" and a["age_cls"] == "dash-warn"
    assert a["heard"] == "heard 1.2s ago" and a["heard_cls"] == "dash-fresh"
    assert a["log"] == "log 536 KB" and b["log"] == "", "mirrored bytes shown only when shore reports them"
    assert b["pos"] == "position unknown" and b["age"] == "no offload yet" and b["age_cls"] == "dash-dim"
    assert b["heard_cls"] == "dash-lost"
    assert dashboard.shore_rows({"auvs": []}) == []


def fresh_deploys(versions=("v1", "v2")):
    dashboard.deploys.update({"rev": 0, "versions": list(versions), "busy": set(), "errors": {}, "chosen": {}})
    return dashboard.deploys


@unit
async def deploy_is_busy_only_while_the_command_runs():
    d = fresh_deploys()
    seen = {}

    async def run(argv, env):
        seen["busy_during"] = set(d["busy"])
        seen["argv"] = argv
        return True, "ok\n"

    await dashboard.run_deploy("auv-2", "v2", run=run, list_versions=lambda: ["v1", "v2", "v3"])
    assert seen["busy_during"] == {"auv-2"}
    assert seen["argv"][-1] == "auv-2"
    assert d["busy"] == set() and d["errors"] == {}
    assert d["versions"] == ["v1", "v2", "v3"], "image list is re-read after a deploy"
    assert d["rev"] >= 2, "panel told to redraw at start and end"


@unit
async def failed_deploy_keeps_its_output_until_the_next_attempt():
    d = fresh_deploys()

    async def fail(argv, env):
        return False, "  boom: no such service  \n"

    async def succeed(argv, env):
        return True, ""

    await dashboard.run_deploy("auv-1", "v1", run=fail, list_versions=lambda: ["v1"])
    assert d["errors"] == {"auv-1": "boom: no such service"} and d["busy"] == set()
    await dashboard.run_deploy("auv-1", "v1", run=succeed, list_versions=lambda: ["v1"])
    assert d["errors"] == {}


@unit
async def deploy_refuses_unknown_versions_without_running_anything():
    d = fresh_deploys(["v1"])
    calls = []

    async def run(argv, env):
        calls.append(argv)
        return True, ""

    await dashboard.run_deploy("auv-1", "v9", run=run, list_versions=lambda: ["v1"])
    assert calls == [] and "no image auv:v9" in d["errors"]["auv-1"]


@unit
async def second_deploy_of_a_busy_vehicle_is_ignored():
    fresh_deploys()
    calls = []
    gate = asyncio.Event()

    async def run(argv, env):
        calls.append(argv)
        await gate.wait()
        return True, ""

    first = asyncio.create_task(dashboard.run_deploy("auv-3", "v1", run=run, list_versions=lambda: ["v1"]))
    await asyncio.sleep(0)
    await dashboard.run_deploy("auv-3", "v2", run=run, list_versions=lambda: ["v1"])
    gate.set()
    await first
    assert len(calls) == 1


@unit
def scene_escapes_names():
    svg = dashboard.scene_svg([auv_at(1, 1, "N", name="<b>x</b>")], [], size=800)
    assert "<b>" not in svg and "&lt;b&gt;" in svg


# --------------------------------------------------------- integration


class Harness:
    """A live world on OS-assigned ports, so tests never collide with a stray server."""

    world: World
    tcp: asyncio.Server
    ws: Server
    port: int
    ws_port: int
    task: asyncio.Task[None]
    procs: list[subprocess.Popen[bytes]]
    log_dir: tempfile.TemporaryDirectory[str]

    async def __aenter__(self):
        # Hulls log here rather than into sim/logs, and it vanishes with the test.
        self.log_dir = tempfile.TemporaryDirectory()
        vehicles, boats = load_fleet(FLEET_PATH)
        self.world = World(vehicles, boats)
        self.tcp = await asyncio.start_server(self.world.handle_auv, "127.0.0.1", 0)
        self.port = self.tcp.sockets[0].getsockname()[1]

        async def viewer(ws: ServerConnection):
            self.world.viewers.add(ws)
            try:
                async for _ in ws:
                    pass
            finally:
                self.world.viewers.discard(ws)

        self.ws = await serve(viewer, "127.0.0.1", 0)
        self.ws_port = self.ws.sockets[0].getsockname()[1]
        self.task = asyncio.create_task(self.world.run())
        self.procs = []
        return self

    async def __aexit__(self, *exc):
        for p in self.procs:
            p.terminate()
            p.wait()
        self.task.cancel()
        self.tcp.close()
        self.ws.close()
        self.log_dir.cleanup()

    def spawn(self, auv_id: str, entry: str = "auv.py", capture: bool = False, **env: str) -> subprocess.Popen[bytes]:
        p = subprocess.Popen(
            [sys.executable, "-u", entry],
            env={**os.environ, "AUV_ID": auv_id, "WORLD_HOST": "127.0.0.1",
                 "WORLD_PORT": str(self.port), "LOG_DIR": self.log_dir.name, **env},
            cwd=SIM, stdout=subprocess.PIPE if capture else subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.procs.append(p)
        return p

    async def ticks(self, n: int):
        await asyncio.sleep(n * server.TICK_SECONDS)

    async def until(self, cond, what: str, timeout: float = 5.0):
        """Poll faster than a tick, so we catch a state change before it moves on."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if cond():
                return
            await asyncio.sleep(0.02)
        raise AssertionError(f"timed out waiting for {what}")


@integration
async def auv_registers_and_disconnects():
    async with Harness() as h:
        v = h.world.vehicles["auv-1"]
        p = h.spawn("auv-1")
        await h.until(lambda: v.connected, "auv-1 to register")
        p.terminate()
        p.wait()
        await h.until(lambda: not v.connected, "auv-1 to drop")


@integration
async def every_behaviour_boots_and_moves():
    """Each entry point must register and drive — catches a broken __main__."""
    async with Harness() as h:
        for auv_id, entry in [("auv-1", "auv.py"), ("auv-2", "auv_straight.py"), ("auv-3", "auv_homing.py")]:
            h.spawn(auv_id, entry)
        vs = h.world.vehicles
        await h.until(lambda: all(v.connected for v in vs.values()), "all three to register")
        spawn = {i: (v.x, v.y, v.dir) for i, v in vs.items()}
        await h.until(lambda: all((v.x, v.y, v.dir) != spawn[i] for i, v in vs.items()),
                      "all three to change state")


@integration
async def unknown_id_is_rejected():
    async with Harness() as h:
        r, w = await asyncio.open_connection("127.0.0.1", h.port)
        w.write(json.dumps({"type": "register", "id": "ghost-9"}).encode() + b"\n")
        await w.drain()
        reply = json.loads(await r.readline())
        assert reply["type"] == "reject" and "manifest" in reply["reason"]
        w.close()


@integration
async def intents_arrive_each_tick():
    async with Harness() as h:
        h.spawn("auv-1")
        h.spawn("auv-2")
        vs = h.world.vehicles
        await h.until(lambda: vs["auv-1"].connected and vs["auv-2"].connected, "both to register")
        seen = set()
        for _ in range(6):
            await asyncio.sleep(server.ACTION_WINDOW / 2)
            seen |= set(h.world.intents)
        assert {"auv-1", "auv-2"} <= seen, f"only saw {seen}"


@integration
async def vehicle_moves_in_the_live_loop():
    async with Harness() as h:
        v = h.world.vehicles["auv-1"]
        h.spawn("auv-1")
        # A random walk can return to where it started, so collect distinct
        # states over time rather than comparing two snapshots.
        seen = set()
        for _ in range(20):
            seen.add((v.x, v.y, v.dir))
            await h.ticks(1)
        assert len(seen) > 1, f"vehicle never changed state: {seen}"
        assert any((x, y) != (10, 10) for x, y, _ in seen), "never left its spawn cell"


@integration
async def viewer_gets_every_vehicle():
    async with Harness() as h:
        h.spawn("auv-1")
        await h.until(lambda: h.world.vehicles["auv-1"].connected, "auv-1 to register")
        async with websockets.connect(f"ws://127.0.0.1:{h.ws_port}") as ws:
            msg = json.loads(await ws.recv())
            assert set(msg) == {"seq", "t", "auvs", "boats"}
            assert len(msg["auvs"]) == 3, "downed vehicles must be sent too"
            assert set(msg["auvs"][0]) == {"id", "name", "x", "y", "dir", "connected", "version"}
            live = next(a for a in msg["auvs"] if a["id"] == "auv-1")
            down = next(a for a in msg["auvs"] if a["id"] == "auv-2")
            assert live["version"] == "dev", f"registered version not surfaced: {live['version']}"
            assert down["version"] == "unknown", "a vehicle that never connected has no build"
            states = {a["id"]: a["connected"] for a in msg["auvs"]}
            assert states["auv-1"] and not states["auv-2"]


@integration
async def viewers_join_midrun_and_are_cleaned_up():
    async with Harness() as h:
        await h.ticks(2)
        assert len(h.world.viewers) == 0
        a = await websockets.connect(f"ws://127.0.0.1:{h.ws_port}")
        b = await websockets.connect(f"ws://127.0.0.1:{h.ws_port}")
        assert json.loads(await a.recv())["auvs"]
        assert json.loads(await b.recv())["auvs"]
        assert len(h.world.viewers) == 2
        await a.close()
        await h.ticks(3)
        assert len(h.world.viewers) == 1, "closed viewer must be dropped"
        assert json.loads(await b.recv())["auvs"], "survivor still receives"
        await b.close()
        await h.ticks(3)
        assert len(h.world.viewers) == 0


@integration
async def position_survives_a_restart():
    """The deploy story: new software, same vehicle, same place."""
    async with Harness() as h:
        v = h.world.vehicles["auv-2"]
        spawn_pos = (v.x, v.y)

        p = h.spawn("auv-2")
        await h.until(lambda: (v.x, v.y) != spawn_pos, "auv-2 to leave its spawn")

        p.terminate()
        p.wait()
        await h.until(lambda: not v.connected, "auv-2 to drop")
        held = (v.x, v.y)
        for _ in range(3):
            await h.ticks(1)
            assert (v.x, v.y) == held, "must hold position while its software is down"

        h.spawn("auv-2")
        # Catch it the instant it reconnects, before it can move again.
        await h.until(lambda: v.connected, "auv-2 to come back")
        assert (v.x, v.y) == held, f"resumed at {(v.x, v.y)}, expected to hold at {held}"

        seen = set()
        for _ in range(20):
            seen.add((v.x, v.y))
            await h.ticks(1)
        assert len(seen) > 1, "should be under way again after the restart"


@integration
async def hull_logs_every_tick_and_closes_on_sigterm():
    """The flight recorder in the live loop: one snapshot and one intent per
    tick, and `docker stop` (SIGTERM) leaves a finished, indexed file."""
    from mcap.reader import make_reader
    async with Harness() as h:
        p = h.spawn("auv-1")
        await h.until(lambda: h.world.vehicles["auv-1"].connected, "auv-1 to register")
        await h.ticks(6)
        p.terminate()
        assert p.wait(timeout=5) == 0, "SIGTERM must be a clean exit"
        files = list(Path(h.log_dir.name).glob("auv-1_*.mcap"))
        assert len(files) == 1, files
        records = list(read_log(files[0]))
        seqs = [r.seq for r in records if isinstance(r, Snapshot)]
        assert len(seqs) >= 4 and seqs == sorted(seqs) and len(set(seqs)) == len(seqs), seqs
        assert [r.seq for r in records if isinstance(r, Intent)] == seqs, "one intent per tick"
        assert all(r.id == "auv-1" for r in records)
        with files[0].open("rb") as f:
            assert make_reader(f).get_summary() is not None, "closed on SIGTERM, so indexed"


@integration
async def redeploy_restores_the_unsent_backlog():
    """auv-1 spawns far from the boat, so nothing it logs ever gets out. Kill
    it, start it again: the new process must pick up every earlier snapshot."""
    async with Harness() as h:
        v = h.world.vehicles["auv-1"]
        first = h.spawn("auv-1")
        await h.until(lambda: v.connected, "first run to register")
        await h.ticks(6)
        first.terminate()
        first.wait(timeout=5)
        await h.until(lambda: not v.connected, "world to notice the first run dropped")
        [logged] = Path(h.log_dir.name).glob("auv-1_*.mcap")
        assert len([r for r in read_log(logged) if isinstance(r, Snapshot)]) >= 4

        second = h.spawn("auv-1", capture=True)
        await h.until(lambda: v.connected, "second run to register")
        second.terminate()
        out = second.communicate(timeout=5)[0].decode()
        assert f"restored {logged.stat().st_size} unsent bytes from 1 earlier run(s)" in out, out


@integration
async def a_mirror_on_the_shore_server_copies_the_ship_stream_over_the_leafnode():
    """Against the live pair: a throwaway stream on the ship's server (4222)
    and a mirror of it on shore's (4223). What is published on the ship,
    before and after the mirror exists, is read on shore in order."""
    import nats as natslib
    from nats.js.api import StreamConfig
    from streams import ensure_stream, mirror_of

    ship = await natslib.connect("nats://localhost:4222")
    shore_side = await natslib.connect("nats://localhost:4223")
    ship_js, shore_js = ship.jetstream(), shore_side.jetstream()
    name = f"TEST{os.getpid()}"
    subject = f"test.{name}.offload"
    got: list[int] = []

    async def handle(msg):
        got.append(json.loads(msg.data)["i"])

    try:
        await ship_js.add_stream(StreamConfig(name=name, subjects=[subject]))
        for i in range(3):
            await ship_js.publish(subject, json.dumps({"i": i}).encode())
        await ensure_stream(shore_js, mirror_of(name))
        await ensure_stream(shore_js, mirror_of(name))  # idempotent
        info = await shore_js.stream_info(name)
        assert info.config.mirror is not None and info.config.mirror.external.api == "$JS.boat.API"
        task = asyncio.create_task(consume(await reader(shore_js, subject, durable="shore", stream=name), handle))
        await until(lambda: len(got) == 3, "the mirror to catch up on what preceded it")
        for i in range(3, 5):
            await ship_js.publish(subject, json.dumps({"i": i}).encode())
        await until(lambda: len(got) == 5, "the mirror to follow the origin")
        assert got == [0, 1, 2, 3, 4], got
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    finally:
        await shore_js.delete_stream(name)
        await ship_js.delete_stream(name)
        await shore_side.close()
        await ship.close()


@integration
async def spans_cross_both_hops_and_shore_mirrors_the_hull_file_byte_for_byte():
    """The whole offload path on the real server pair, on a throwaway stream:
    the hull publishes spans with headers on the ship's server, the mirror
    carries them over the leafnode, shore reads them from its own server and
    appends them. Afterwards shore's copy is the hull's file."""
    import auv as hull
    import nats as natslib
    import shore as shore_station
    from nats.js.api import StreamConfig
    from streams import ensure_stream, mirror_of

    ship = await natslib.connect("nats://localhost:4222")
    shore_side = await natslib.connect("nats://localhost:4223")
    js, shore_js = ship.jetstream(), shore_side.jetstream()
    name = f"TEST{os.getpid()}"
    fleet_hop = f"test.{name}.fleet"
    await js.add_stream(StreamConfig(name=name, subjects=[fleet_hop]))
    await ensure_stream(shore_js, mirror_of(name))

    async def publish_acked(subject, data, headers):
        await js.publish(subject, data, timeout=1, headers=headers)

    with tempfile.TemporaryDirectory() as d:
        mirror = Mirror(Path(d) / "shore")
        picture = shore_station.Shore({}, mirror)
        a = Auv("auv-1", "v1", [BOAT])
        old, _ = hull_file(d, "auv-1_run0.mcap")
        mirror.append(Span(id="auv-1", file=old.name, offset=0), old.read_bytes()[:4096])  # journaled as delivered
        a.pending = unsent([old])
        log = HullLog(Path(d) / "auv-1_run1.mcap")
        for s in snaps(3, start=7):
            log.write(s)
        hull.SPAN_BYTES = 1500

        async def to_fleet_hop(_subject, data, headers):  # the hull's `fleet.<id>.offload`, on the test stream
            await publish_acked(fleet_hop, data, headers)

        async def land(msg):
            shore_station.receive(picture, mirror, msg)

        def copied(path: Path) -> bool:
            copy = mirror.root / "auv-1" / path.name
            return copy.exists() and copy.read_bytes() == path.read_bytes()

        try:
            landed = asyncio.create_task(consume(await reader(shore_js, fleet_hop, durable="shore", stream=name), land))
            for _ in range(50):
                await hull.offload(a, to_fleet_hop, log)
                if not a.pending:
                    break
            log.close()  # run 1 over; its footer is shipped by run 2 as an earlier file's tail
            second = Auv("auv-1", "v1", [BOAT])
            second.pending = unsent([old, log.path])
            assert [t.path for t in second.pending] == [log.path], "run 1 journaled old as delivered"
            log2 = HullLog(Path(d) / "auv-1_run2.mcap")
            while second.pending:
                await hull.offload(second, to_fleet_hop, log2)
            await until(lambda: copied(old) and copied(log.path), "every span of both files to land on shore")
            assert picture.fleet["auv-1"]["last_seq"] == 9, "the picture came from the mirrored bytes"
            assert picture.picture(0.0)["auvs"][0]["log_bytes"] == mirror.size("auv-1")
            log2.close()
        finally:
            hull.SPAN_BYTES = 256 * 1024
            landed.cancel()
            await asyncio.gather(landed, return_exceptions=True)
            await shore_js.delete_stream(name)
            await js.delete_stream(name)
            await shore_side.close()
            await ship.close()


@integration
async def durable_reader_catches_up_and_retries_after_it_was_away():
    """The shore-restart story, against the real JetStream server on a
    throwaway stream: batches published while no reader is attached are
    delivered when one attaches, nothing is lost or repeated across the
    reader going away, and a batch whose handler fails comes back."""
    import nats as natslib
    from nats.js.api import StorageType, StreamConfig

    nc = await natslib.connect("nats://localhost:4222")
    js = nc.jetstream()
    name = f"TEST{os.getpid()}"
    subject = f"test.{name}.offload"
    await js.add_stream(StreamConfig(name=name, subjects=[subject], storage=StorageType.MEMORY))
    got: list[int] = []
    failed_once = set()

    async def handle(msg):
        i = json.loads(msg.data)["i"]
        if i == 3 and i not in failed_once:
            failed_once.add(i)
            raise RuntimeError("downstream hiccup")
        got.append(i)

    async def attach():
        return asyncio.create_task(consume(await reader(js, subject, durable="shore", stream=name), handle))

    try:
        for i in range(3):
            await js.publish(subject, json.dumps({"i": i}).encode())
        task = await attach()  # arrives after the batches did
        await until(lambda: len(got) == 3, "late reader to receive the backlog")
        task.cancel()  # reader dies...
        await asyncio.sleep(1.5)  # ...and stays down past its last pull request's expiry
        for i in range(3, 5):
            await js.publish(subject, json.dumps({"i": i}).encode())
        task = await attach()  # reader returns
        await until(lambda: len(got) == 5, "returning reader to pick up what it missed")
        task.cancel()
        assert got == [0, 1, 2, 3, 4], got
        assert failed_once == {3}, "the failing batch was redelivered, not dropped"
    finally:
        await js.delete_stream(name)
        await nc.close()


# ---------------------------------------------------------------- runner


def main():
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    # Integration tests run the real loop, so shorten the tick to keep them quick.
    server.TICK_SECONDS = 0.2
    server.ACTION_WINDOW = 0.15

    groups = []
    if which in ("all", "unit"):
        groups.append(("unit", UNIT))
    if which in ("all", "integration"):
        groups.append(("integration", INTEGRATION))

    passed = failed = 0
    start = time.time()
    for label, tests in groups:
        print(f"\n{label}")
        for fn in tests:
            name = fn.__name__.replace("_", " ")
            captured = io.StringIO()
            try:
                with contextlib.redirect_stdout(captured):
                    asyncio.run(fn()) if asyncio.iscoroutinefunction(fn) else fn()
            except Exception as e:
                failed += 1
                print(f"  {name:.<52} FAIL")
                print(f"      {type(e).__name__}: {e}")
                for line in captured.getvalue().splitlines():
                    print(f"      | {line}")
            else:
                passed += 1
                print(f"  {name:.<52} pass")

    print(f"\n{passed} passed, {failed} failed in {time.time() - start:.1f}s")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
