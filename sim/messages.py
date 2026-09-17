"""The messages a hull produces. Shared by the hull, the boat, and shore, so
one definition decides what a record looks like on every side of the link.
The JSON schema of each model doubles as its MCAP channel schema."""

from pathlib import Path
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, Field

Direction = Literal["N", "E", "S", "W"]
Action = Literal["move", "turn_left", "turn_right", "wait", "plant"]


class Snapshot(BaseModel):
    """One per tick: where the hull was and what build it ran. Logged and offloaded."""

    id: str
    x: int
    y: int
    dir: Direction
    seq: int
    ts: float
    version: str


class Heartbeat(BaseModel):
    """Every 10 ticks: alive signal only, no position."""

    id: str
    ts: float
    version: str


def bare_name(name: str) -> str:
    """A file name as it appears on the hull, never a path: shore writes its
    mirror under this name, so a separator here would let a hull escape the
    mirror directory."""
    if not name or name != Path(name).name:
        raise ValueError(f"not a bare file name: {name!r}")
    return name


class Offload(BaseModel):
    """Logged only, never sent: bytes [0, through) of `file` reached the boat.
    Names the file because a run journals the delivery of earlier runs' tails
    too, so the journal for one file can live in a later one."""

    id: str
    ts: float
    # file is a string that needs to be validated with bare_name after it's validated
    file: Annotated[str, AfterValidator(bare_name)]
    through: int = Field(ge=0)


class Span(BaseModel):
    """The headers on an offload message: the payload is bytes of `file`,
    starting at `offset`. Validated on receipt; the payload itself is opaque
    until shore's mirror decodes it. `id` names the mirror's directory for the
    hull, so it is held to a bare name too."""

    id: Annotated[str, AfterValidator(bare_name)]
    file: Annotated[str, AfterValidator(bare_name)]
    offset: int = Field(ge=0)

    def headers(self) -> dict[str, str]:
        return {"id": self.id, "file": self.file, "offset": str(self.offset)}

    @classmethod
    def from_headers(cls, headers: dict[str, str] | None) -> "Span":
        return cls.model_validate(headers or {})


class Intent(BaseModel):
    """What the hull asked the world to do in answer to tick `seq`.
    `type` is the world protocol's line discriminator; it is fixed here so the
    model serialises straight onto the wire."""

    type: Literal["intent"] = "intent"
    id: str
    seq: int
    action: Action
