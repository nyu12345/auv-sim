"""Operator dashboard: world truth, shore picture, and per-AUV deploys in one
page. Bare-metal only — it is a client of the world server and the shore
station, and it needs the host's Docker CLI to deploy."""

import asyncio
import json
import os
from collections import deque
from collections.abc import Callable
from html import escape
from typing import Any

import websockets
from deploy import FLEET_PATH, ROOT, available_versions, deploy_command, fleet_ids
from nicegui import app, background_tasks, ui

WORLD_WS = "ws://localhost:8765"
SHORE_WS = "ws://localhost:8766"
GRID = 100  # server.py GRID
SIZE = 800  # px; the map is square

# Same palette as the old canvas: black hull, red when its software is down.
LIVE, DOWN, DIM, BOAT, GRASS = "#000", "#c33", "#456", "#fc0", "#165"

# One copy of each upstream's latest message, shared by every browser tab.
# The world stamps its own seq; shore pushes have none, so we count them.
world: dict = {"seq": 0, "auvs": [], "boats": []}
shore: dict = {"rev": 0, "auvs": []}
# Deployer state. `rev` bumps on every change so tabs redraw the controls only
# then — never mid-tick, so an open dropdown is not torn down under the cursor.
deploys: dict = {"rev": 0, "versions": [], "busy": set(), "errors": {}, "chosen": {}}

LOG_SIZE = 10
world_log: dict[str, deque] = {}
shore_log: dict[str, deque] = {}
world_log_open: set[str] = set()
shore_log_open: set[str] = set()

# Prefixed so nothing collides with Quasar's own .row/.column utilities.
CSS = """
body { background: #87ceeb; }
.dash-panel { width: 300px; box-sizing: border-box; padding: 10px; background: #111; color: #ddd;
              font: 12px/1.5 monospace; border: 1px solid #333; overflow-y: auto; }
.dash-hdr { display: flex; justify-content: space-between; color: #6cf; letter-spacing: 0.1em;
            border-bottom: 1px solid #333; padding-bottom: 6px; margin-bottom: 8px; }
.dash-line { display: flex; justify-content: space-between; }
.dash-name { color: #fff; }
.dash-status { color: #4c8; }
.dash-dim { color: #789; }
.dash-auv { border-left: 2px solid #4c8; padding-left: 8px; margin-bottom: 10px; }
.dash-auv.down { border-left-color: #c33; }
.dash-auv.down .dash-name, .dash-auv.down .dash-status { color: #c33; }
.dash-auv.down .dash-dim { color: #944; }
.dash-boat { border-left: 2px solid #fc0; padding-left: 8px; margin-bottom: 10px; }
.dash-boat .dash-name { color: #fc0; }
.dash-boat .dash-dim { color: #a86; }
.dash-fresh { color: #4c8; }
.dash-warn { color: #ca0; }
.dash-lost { color: #c33; }
.dash-deploy { display: flex; gap: 6px; align-items: center; margin-bottom: 6px; }
.dash-deploy .dash-name { flex: 1; }
.dash-deploy .q-field, .dash-deploy .q-btn { font: 11px monospace; color: #ddd; background: #222; }
.dash-err { color: #c33; white-space: pre-wrap; margin-bottom: 6px; }
.dash-log-toggle { color: #6cf; cursor: pointer; font-size: 11px; user-select: none; }
.dash-log-toggle:hover { text-decoration: underline; }
.dash-log-entry { color: #789; font-size: 11px; white-space: pre; }
"""


def heading_deg(dir: str | float) -> float:
    """Clockwise degrees from north. Accepts a cardinal letter today and a raw
    heading tomorrow, so the map needs no change when the world goes freeform."""
    if isinstance(dir, str):
        return {"N": 0, "E": 90, "S": 180, "W": 270}[dir]
    return float(dir)


def _f(n: float) -> str:
    return f"{n:g}"


def _center(x: float, y: float, cell: float) -> tuple[float, float]:
    # World y points north; screen y points down.
    return x * cell + cell / 2, (GRID - 1 - y) * cell + cell / 2


def grid_svg(size: float = SIZE) -> str:
    """Static grid layer: one line per cell edge, drawn once."""
    cell = size / GRID
    lines = []
    for i in range(GRID + 1):
        p = _f(i * cell)
        lines.append(f'<line x1="{p}" y1="0" x2="{p}" y2="{_f(size)}"/>')
        lines.append(f'<line x1="0" y1="{p}" x2="{_f(size)}" y2="{p}"/>')
    return (
        f'<svg width="{_f(size)}" height="{_f(size)}" style="position:absolute;top:0;left:0">'
        f'<g stroke="#222" stroke-width="0.5">{"".join(lines)}</g></svg>'
    )


def scene_svg(auvs: list[dict], boats: list[dict], size: float = SIZE,
              seagrass: list[dict] | None = None) -> str:
    """Live layer, rebuilt every tick: seagrass tiles, a triangle per AUV, a diamond per boat."""
    cell = size / GRID
    parts = []
    for g in seagrass or []:
        sx = g["x"] * cell
        sy = (GRID - 1 - g["y"]) * cell
        parts.append(f'<rect x="{_f(sx)}" y="{_f(sy)}" width="{_f(cell)}" height="{_f(cell)}" fill="{GRASS}" opacity="0.4"/>')
    for a in auvs:
        cx, cy = _center(a["x"], a["y"], cell)
        h = cell * 3 / 2  # half the triangle's bounding box
        color = LIVE if a["connected"] else DOWN
        pts = f"{_f(cx)},{_f(cy - h)} {_f(cx + h)},{_f(cy + h)} {_f(cx - h)},{_f(cy + h)}"
        parts.append(
            f'<polygon points="{pts}" fill="{color}" '
            f'transform="rotate({_f(heading_deg(a["dir"]))} {_f(cx)} {_f(cy)})"/>'
            f'<text x="{_f(cx + h + 3)}" y="{_f(cy)}" font-size="11" fill="{color}">{escape(a["name"])}</text>'
            f'<text x="{_f(cx + h + 3)}" y="{_f(cy + 11)}" font-size="9" '
            f'fill="{DIM if a["connected"] else DOWN}">{escape(a["version"])}</text>'
        )
    for b in boats:  # drawn last, on top of AUVs
        cx, cy = _center(b["x"], b["y"], cell)
        r = cell * 2
        pts = f"{_f(cx)},{_f(cy - r)} {_f(cx + r)},{_f(cy)} {_f(cx)},{_f(cy + r)} {_f(cx - r)},{_f(cy)}"
        parts.append(
            f'<polygon points="{pts}" fill="{BOAT}"/>'
            f'<text x="{_f(cx + r + 3)}" y="{_f(cy + 4)}" font-size="11" fill="{LIVE}">{escape(b["name"])}</text>'
        )
    return (
        f'<svg width="{_f(size)}" height="{_f(size)}" style="position:absolute;top:0;left:0" '
        f'font-family="monospace">{"".join(parts)}</svg>'
    )


async def subscribe(url: str, on_msg: Callable[[dict], None], backoff: float = 1.0) -> None:
    """Feed every JSON message from `url` to `on_msg`, forever.

    Each upstream gets its own loop so one dead service never stalls the
    others: a refused connection or a drop just waits `backoff` and retries.
    """
    while True:
        try:
            async with websockets.connect(url) as ws:
                async for raw in ws:
                    on_msg(json.loads(raw))
        except (OSError, websockets.WebSocketException):
            pass
        await asyncio.sleep(backoff)


def truth_rows(snapshot: dict) -> list[dict]:
    """Boats first, then vehicles, each flattened to the strings the panel shows.
    Deliberately dumb: it reports what the last tick said, nothing inferred,
    so the shore panel beside it can diverge visibly when messages drop."""
    rows = [
        {"kind": "boat", "name": f"⬥ {b['name']}", "pos": f"({b['x']}, {b['y']})", "id": b["id"]}
        for b in snapshot["boats"]
    ]
    rows += [
        {
            "kind": "auv",
            "name": a["name"],
            "status": "live" if a["connected"] else "down",
            "down": not a["connected"],
            "pos": f"({a['x']}, {a['y']}) {a['dir']}",
            "version": a["version"],
            "id": a["id"],
        }
        for a in snapshot["auvs"]
    ]
    return rows


def heard_class(age: float) -> str:
    """Seconds since the last heartbeat: green under 3, amber under 15, red after."""
    return "dash-fresh" if age < 3 else "dash-warn" if age < 15 else "dash-lost"


def pos_class(age: float | None) -> str:
    """Seconds since the last offloaded position: dim if never, green under 5,
    amber under 30, red after. Looser than heard_class because positions only
    arrive when a vehicle docks."""
    if age is None:
        return "dash-dim"
    return "dash-fresh" if age < 5 else "dash-warn" if age < 30 else "dash-lost"


def shore_rows(snapshot: dict) -> list[dict]:
    """What shore believes about each vehicle, flattened to the strings the
    panel shows. Every field is as-reported: staleness is shore's own clock."""
    rows = []
    for a in snapshot["auvs"]:
        pos_age = a.get("pos_age")
        rows.append({
            "name": a["name"],
            "heard": f"heard {a['last_heard']}s ago",
            "heard_cls": heard_class(a["last_heard"]),
            "pos": f"({a['x']}, {a['y']}) {a['dir']}" if a["has_position"] else "position unknown",
            "version": a["version"],
            "age": f"pos {pos_age}s ago" if pos_age is not None else "no offload yet",
            "age_cls": pos_class(pos_age),
            "log": f"log {a['log_bytes'] / 1024:.0f} KB" if a.get("log_bytes") is not None else "",
        })
    return rows


def _line(left: str, right: str = "", cls: str = "") -> None:
    with ui.element("div").classes(f"dash-line {cls}"):
        ui.label(left)
        ui.label(right)


def _log_expansion(auv_id: str, entries: deque | None, open_set: set[str]) -> None:
    if not entries:
        return
    is_open = auv_id in open_set

    def toggle(e, aid=auv_id, oset=open_set):
        if e.value:
            oset.add(aid)
        else:
            oset.discard(aid)

    with ui.expansion("log", icon="expand_more", value=is_open, on_value_change=toggle).classes("dash-log-toggle").props("dense"):
        for entry in reversed(entries):
            ui.label(entry).classes("dash-log-entry")


def truth_panel() -> None:
    with ui.element("div").classes("dash-hdr"):
        ui.label("WORLD TRUTH")
        ui.label(f"seq {world['seq']}")
    for r in truth_rows(world):
        if r["kind"] == "boat":
            with ui.element("div").classes("dash-boat"):
                with ui.element("div").classes("dash-line"):
                    ui.label(r["name"]).classes("dash-name")
                _line(r["pos"], r["id"], "dash-dim")
        else:
            with ui.element("div").classes("dash-auv" + (" down" if r["down"] else "")):
                with ui.element("div").classes("dash-line"):
                    ui.label(r["name"]).classes("dash-name")
                    ui.label(r["status"]).classes("dash-status")
                _line(r["pos"], r["version"], "dash-dim")
                _line(r["id"], cls="dash-dim")
                _log_expansion(r["id"], world_log.get(r["id"]), world_log_open)


def shore_panel() -> None:
    with ui.element("div").classes("dash-hdr"):
        ui.label("SHORE PICTURE")
    rows = shore_rows(shore)
    if not rows:
        ui.label("no data yet").classes("dash-dim")
    for a, r in zip(shore.get("auvs", []), rows):
        with ui.element("div").classes("dash-auv"):
            with ui.element("div").classes("dash-line"):
                ui.label(r["name"]).classes("dash-name")
                ui.label(r["heard"]).classes(r["heard_cls"])
            _line(r["pos"], r["version"], "dash-dim")
            _line(r["age"], r["log"], r["age_cls"])
            _log_expansion(a.get("id", ""), shore_log.get(a.get("id", "")), shore_log_open)


def fleet_names() -> dict[str, str]:
    """id -> name from the manifest, so the controls exist before any tick."""
    return {a["id"]: a["name"] for a in json.loads(FLEET_PATH.read_text())["auvs"]}


async def _run_compose(argv: list[str], env: dict[str, str]) -> tuple[bool, str]:
    proc = await asyncio.create_subprocess_exec(
        *argv, env=env, cwd=ROOT,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    out, _ = await proc.communicate()
    return proc.returncode == 0, out.decode()


async def refresh_versions(list_versions: Callable[[], list[str]] = available_versions) -> None:
    deploys["versions"] = await asyncio.to_thread(list_versions)
    deploys["rev"] += 1


async def run_deploy(
    auv_id: str,
    version: str,
    run: Callable[..., Any] = _run_compose,
    list_versions: Callable[[], list[str]] = available_versions,
) -> None:
    """One deploy, start to finish. A vehicle already deploying is left alone;
    a failure leaves its output on the panel until the next attempt."""
    if auv_id in deploys["busy"]:
        return
    deploys["busy"].add(auv_id)
    deploys["errors"].pop(auv_id, None)
    deploys["rev"] += 1
    try:
        argv, env = deploy_command(auv_id, version, fleet_ids(), deploys["versions"])
        ok, output = await run(argv, env)
    except ValueError as e:
        ok, output = False, str(e)
    print(f"deploy {auv_id} -> {version}: {'ok' if ok else 'FAILED'}")
    if not ok:
        deploys["errors"][auv_id] = output.strip()
    deploys["busy"].discard(auv_id)
    await refresh_versions(list_versions)


def deploy_panel() -> None:
    with ui.element("div").classes("dash-hdr").style("margin-top:12px"):
        ui.label("DEPLOY")
    versions = deploys["versions"]
    if not versions:
        ui.label("no auv images").classes("dash-dim")
        return
    for auv_id, name in fleet_names().items():
        busy = auv_id in deploys["busy"]
        with ui.element("div").classes("dash-deploy"):
            ui.label(name).classes("dash-name")
            ui.select(
                versions,
                value=deploys["chosen"].get(auv_id, versions[0]),
                on_change=lambda e, i=auv_id: deploys["chosen"].__setitem__(i, e.value),
            ).props("dense outlined dark options-dense").style("min-width:70px")
            ui.button(
                "deploying…" if busy else "deploy",
                on_click=lambda i=auv_id: run_deploy(i, deploys["chosen"].get(i, deploys["versions"][0])),
            ).props("dense flat no-caps").set_enabled(not busy)
        if auv_id in deploys["errors"]:
            ui.label(deploys["errors"][auv_id]).classes("dash-err")


def on_world(msg: dict) -> None:
    world.update(msg)
    seq = msg.get("seq", "?")
    for a in msg.get("auvs", []):
        buf = world_log.setdefault(a["id"], deque(maxlen=LOG_SIZE))
        buf.append(f"seq {seq}  ({a['x']}, {a['y']}) {a['dir']}  {a['version']}  {'live' if a['connected'] else 'DOWN'}")


def on_shore(msg: dict) -> None:
    shore.update(msg)
    shore["rev"] += 1
    for a in msg.get("auvs", []):
        buf = shore_log.setdefault(a["id"], deque(maxlen=LOG_SIZE))
        pos = f"({a['x']}, {a['y']}) {a['dir']}" if a.get("has_position") else "pos unknown"
        buf.append(f"heard {a['last_heard']}s  {pos}  {a['version']}")


def build_page() -> None:
    """Built once per browser tab (NiceGUI's root function). The upstream
    feeds live in module state; each tab polls that faster than a tick and
    re-renders a panel only when its feed has moved."""
    ui.add_css(CSS)
    truth = ui.refreshable(truth_panel)
    picture = ui.refreshable(shore_panel)
    controls = ui.refreshable(deploy_panel)
    with ui.row().classes("items-start"):
        with ui.element("div").style(f"position:relative;width:{SIZE}px;height:{SIZE}px;border:1px solid #333"):
            ui.html(grid_svg(), sanitize=False)
            scene = ui.html("", sanitize=False)
        with ui.element("div").classes("dash-panel").style(f"height:{SIZE}px"):
            truth()
            controls()
        with ui.element("div").classes("dash-panel").style(f"height:{SIZE}px"):
            picture()

    shown = {"seq": None, "shore": None, "deploys": None}

    def sync() -> None:
        if world["seq"] != shown["seq"]:
            shown["seq"] = world["seq"]
            scene.content = scene_svg(world["auvs"], world["boats"],
                                      seagrass=world.get("seagrass"))
            truth.refresh()
        if shore["rev"] != shown["shore"]:
            shown["shore"] = shore["rev"]
            picture.refresh()
        if deploys["rev"] != shown["deploys"]:
            shown["deploys"] = deploys["rev"]
            controls.refresh()

    ui.timer(0.2, sync)


if __name__ == "__main__":
    app.on_startup(lambda: background_tasks.create(subscribe(WORLD_WS, on_world), name="world"))
    app.on_startup(lambda: background_tasks.create(subscribe(SHORE_WS, on_shore), name="shore"))
    app.on_startup(refresh_versions)
    # Not 8080: on this machine Chrome loads the page there but never opens
    # NiceGUI's socket, so the map sits frozen at its first frame.
    port = int(os.environ.get("DASH_PORT", "8081"))
    ui.run(root=build_page, port=port, reload=False, show=False, title="AUV Sim")
