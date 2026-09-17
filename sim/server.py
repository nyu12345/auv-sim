import asyncio
import json
import time
from enum import Enum
from pathlib import Path

from websockets.asyncio.server import ServerConnection, serve

GRID = 100
TICK_SECONDS = 0.5
ACTION_WINDOW = 0.4
FLEET_PATH = Path(__file__).parent / "fleet.json"


class CardinalDirection(Enum):
    # Declared clockwise so rotate(+1) is a right turn.
    NORTH = "N"
    EAST = "E"
    SOUTH = "S"
    WEST = "W"

    def rotate(self, n: int) -> "CardinalDirection":
        values = list(CardinalDirection)
        return values[(values.index(self) + n) % len(values)]

    @property
    def delta(self) -> tuple[int, int]:
        match self:
            case CardinalDirection.NORTH:
                return (0, 1)
            case CardinalDirection.WEST:
                return (-1, 0)
            case CardinalDirection.SOUTH:
                return (0, -1)
            case CardinalDirection.EAST:
                return (1, 0)


class Boat:
    def __init__(self, id: str, name: str, x: int, y: int):
        self.id = id
        self.name = name
        self.x = x
        self.y = y


class Vehicle:
    def __init__(self, id: str, name: str, x: int, y: int, dir: CardinalDirection):
        self.id = id
        self.name = name
        self.x = x
        self.y = y
        self.dir = dir
        self.writer: asyncio.StreamWriter | None = None
        # Last build this hull reported. Survives disconnect on purpose: a
        # powered-down vehicle still has that software sitting on it.
        self.version = "unknown"

    @property
    def connected(self) -> bool:
        return self.writer is not None


class World:
    def __init__(self, vehicles: dict[str, Vehicle], boats: dict[str, Boat]):
        self.vehicles = vehicles
        self.boats = boats
        self.seq = 0
        self.intents: dict[str, str] = {}
        self.viewers: set[ServerConnection] = set()
        self.seagrass: set[tuple[int, int]] = set()

    # TCP connection to listen to AUV intents and to map each AUV to it's TCP writer. Used to get AUV actions to update the real world simulation
    async def handle_auv(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        line = await reader.readline()
        if not line:
            return
        register = json.loads(line)
        auv_id = register.get("id")
        vehicle = self.vehicles.get(auv_id)

        # if the tcp connection is not in our fleet
        if vehicle is None:
            writer.write(
                json.dumps(
                    {"type": "reject", "reason": f"{auv_id} not in fleet manifest"}
                ).encode()
                + b"\n"
            )
            await writer.drain()
            writer.close()
            return

        vehicle.version = register.get("version", "unknown")
        vehicle.writer = writer
        print(f"connected: {auv_id} [{vehicle.version}] at ({vehicle.x}, {vehicle.y})")
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                msg = json.loads(line)

                # world server stores the AUV's intents for its understanding of what the AUVs are doing. each auv gets one, last write wins
                if msg["type"] == "intent":
                    self.intents[auv_id] = msg["action"]
        finally:
            vehicle.writer = None
            print(f"disconnected: {auv_id}")

    # For each vehicle, have the world server teach them their true position in the sim.
    async def _broadcast_tick(self):
        for vehicle in self.vehicles.values():
            writer = vehicle.writer
            if writer is None:
                continue
            tile = "seagrass" if (vehicle.x, vehicle.y) in self.seagrass else "empty"
            msg = (
                json.dumps(
                    {
                        "type": "tick",
                        "seq": self.seq,
                        "you": {
                            "x": vehicle.x,
                            "y": vehicle.y,
                            "dir": vehicle.dir.value,
                            "tile": tile,
                        },
                    }
                ).encode()
                + b"\n"
            )
            try:
                writer.write(msg)
                await writer.drain()
            except Exception:
                vehicle.writer = None

    def _apply(self):
        # Disconnected vehicles still hold their cell — a rebooting AUV is still in the water.
        # An occupied set to account for collisions
        occupied = {(v.x, v.y) for v in self.vehicles.values()}

        for auv_id in sorted(self.intents):
            vehicle = self.vehicles[auv_id]
            match self.intents[auv_id]:
                case "turn_left":
                    vehicle.dir = vehicle.dir.rotate(-1)
                case "turn_right":
                    vehicle.dir = vehicle.dir.rotate(1)
                case "move":
                    dx, dy = vehicle.dir.delta
                    dest = (
                        max(0, min(GRID - 1, vehicle.x + dx)),
                        max(0, min(GRID - 1, vehicle.y + dy)),
                    )
                    # Only move if not occupied,
                    if dest not in occupied:
                        occupied.discard((vehicle.x, vehicle.y))
                        occupied.add(dest)
                        vehicle.x, vehicle.y = dest
                case "plant":
                    pos = (vehicle.x, vehicle.y)
                    if pos not in self.seagrass:
                        self.seagrass.add(pos)

        self.intents.clear()

    async def _push_viewers(self):
        msg = json.dumps(
            {
                "seq": self.seq,
                "t": time.time(),
                "auvs": [
                    {
                        "id": v.id,
                        "name": v.name,
                        "x": v.x,
                        "y": v.y,
                        "dir": v.dir.value,
                        "connected": v.connected,
                        "version": v.version,
                    }
                    for v in self.vehicles.values()
                ],
                "boats": [
                    {"id": b.id, "name": b.name, "x": b.x, "y": b.y}
                    for b in self.boats.values()
                ],
                "seagrass": [{"x": x, "y": y} for x, y in self.seagrass],
            }
        )
        for viewer in list(self.viewers):
            try:
                await viewer.send(msg)
            except Exception:
                self.viewers.discard(viewer)

    async def run(self):
        while True:
            self.seq += 1
            await self._broadcast_tick()
            await asyncio.sleep(ACTION_WINDOW)

            self._apply()
            await self._push_viewers()
            await asyncio.sleep(TICK_SECONDS - ACTION_WINDOW)


# Instantiates Vehicle and Boat instances give fleet.json data
def load_fleet(path: Path) -> tuple[dict[str, Vehicle], dict[str, Boat]]:
    data = json.loads(path.read_text())

    # for each json fleet vehicle instantiate a Vehicle
    vehicles = {
        a["id"]: Vehicle(
            a["id"],
            a["name"],
            a["spawn"]["x"],
            a["spawn"]["y"],
            CardinalDirection(a["spawn"]["dir"]),
        )
        for a in data["auvs"]
    }

    # for each json boat instantiate a Boat
    boats = {
        b["id"]: Boat(b["id"], b["name"], b["x"], b["y"]) for b in data.get("boats", [])
    }
    return vehicles, boats


async def main():
    # loads fleet
    vehicles, boats = load_fleet(FLEET_PATH)

    # initiates world sim
    world = World(vehicles, boats)

    async def viewer(websocket: ServerConnection):
        world.viewers.add(websocket)
        try:
            async for _ in websocket:
                pass
        finally:
            world.viewers.discard(websocket)

    # TCP connection for letting AUV update world source of truth
    auv_server = await asyncio.start_server(world.handle_auv, "0.0.0.0", 9000)

    # Websocket for the NiceGUI server
    viewer_server = await serve(viewer, "localhost", 8765)
    print(f"world sim up — {len(world.vehicles)} AUVs in manifest, tcp:9000 ws:8765")
    try:
        await world.run()
    finally:
        auv_server.close()
        viewer_server.close()


if __name__ == "__main__":
    asyncio.run(main())
