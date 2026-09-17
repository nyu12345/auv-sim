# AUV sim

<img width="1294" height="727" alt="Screenshot 2026-09-17 at 2 28 11 AM" src="https://github.com/user-attachments/assets/633cd1cf-ed19-40d1-993e-bf1482a216ad" />


A simulation of the shore-side fleet management problem: deploying code to AUVs, monitoring them over an unreliable connection, and operating from a degraded picture when the link drops. AUVs run as independent Docker containers, offload telemetry through NATS/JetStream, and shore reassembles their logs and tracks their positions across restarts, crashes, and network outages.

**Built with:** Python (Pydantic), NATS/JetStream, Docker, MCAP, WebSockets, NiceGUI

## Architecture

- **World server** (`sim/server.py`, host): source of truth. Runs a 100x100 grid at 0.5s ticks. Sends each AUV its position over TCP, collects intents, applies movement/planting, pushes state to the dashboard over WebSocket.

- **AUV containers** (`sim/auv.py` + variants, Docker): each AUV is its own container with independent versioning. Decides an action each tick, writes MCAP flight-recorder logs to a persistent volume, sends heartbeats over NATS, and offloads log chunks over JetStream when within range of a boat. If NATS is down the AUV keeps running; unsent data drains on reconnect.

- **NATS servers** (Docker): two servers model the ship-to-shore link. The boat server holds the OFFLOAD stream; shore holds a JetStream mirror pulled over a leafnode. Cutting the `uplink` network simulates a satellite outage, and reconnecting catches up automatically.

- **Shore station** (`sim/shore.py`, Docker): receives heartbeats and offloads, reassembles MCAP files on disk, tracks a fleet picture (last known position, staleness timers, version). Replays archived files on restart so it never starts blind. Serves the picture over WebSocket.

- **Dashboard** (`sim/dashboard.py`, host): NiceGUI app with an SVG map, two side panels, and deploy controls.
  - _World truth_: live location, direction, status, code version, last 10 log entries per AUV
  - _Shore picture_: the operator's degraded view with inferred positions, staleness timers, heartbeat status
  - _Deploy console_: pick a version from a dropdown, deploy to any vehicle with one click (rollback the same way)

## How to run

Prerequisites: Docker Desktop, Python 3.11+, pipenv

```bash
# 1. Install Python dependencies
pipenv install

# 2. Start the infrastructure (NATS servers, shore, AUVs)
docker compose up -d

# 3. Start the world server (runs on host, not in Docker)
pipenv run python sim/server.py

# 4. Start the dashboard (runs on host, needs Docker CLI for deploys)
pipenv run python sim/dashboard.py
```

Dashboard is at http://localhost:8081

### Deploying a different AUV version

```bash
# Build a version with a specific behavior
docker build -t auv:v3 --build-arg AUV_VERSION=v3 --build-arg AUV_ENTRY=auv_homing.py .

# Deploy it to one AUV
AUV1_VERSION=v3 docker compose up -d --no-deps auv-1

# Or deploy to all three
AUV1_VERSION=v3 AUV2_VERSION=v3 AUV3_VERSION=v3 docker compose up -d --no-deps auv-1 auv-2 auv-3
```

Available behaviors:

- `auv.py` (v1): random movement + planting
- `auv_straight.py` (v2): drive straight, alternating move and plant
- `auv_homing.py` (v3): random phase with planting, then navigate home to dock

### Simulating a satellite outage

```bash
docker network disconnect auv-sim_uplink nats-boat   # satellite down
docker network connect    auv-sim_uplink nats-boat   # satellite back
```
