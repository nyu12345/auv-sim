"""Stand-in for shore-side fleet-management tooling: turns a (vehicle, version)
pick into the compose command that deploys it. The dashboard runs these;
this module never runs on its own and never ships in an image."""

import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).parent.parent
FLEET_PATH = Path(__file__).parent / "fleet.json"


def fleet_ids() -> set[str]:
    return {a["id"] for a in json.loads(FLEET_PATH.read_text())["auvs"]}


def available_versions() -> list[str]:
    out = subprocess.run(
        ["docker", "image", "ls", "auv", "--format", "{{.Tag}}"],
        capture_output=True, text=True, check=True,
    ).stdout
    return sorted(out.split())


def deploy_command(auv_id: str, version: str, ids: set[str], versions: list[str]) -> tuple[list[str], dict[str, str]]:
    if auv_id not in ids:
        raise ValueError(f"{auv_id} not in fleet manifest")
    if version not in versions:
        raise ValueError(f"no image auv:{version}")
    # auv-2 -> AUV2_VERSION, matching the variable names in docker-compose.yml.
    n = auv_id.split("-")[1]
    argv = ["docker", "compose", "up", "-d", "--no-deps", auv_id]
    env = {**os.environ, f"AUV{n}_VERSION": version}
    return argv, env
