"""v2 behaviour: no wandering. Drive straight until the world stops us.

Same transport and same server contract as auv.py — only decide() differs, so
this is a genuine behaviour change rather than a different client.
"""

from auv import Auv, main


class StraightAuv(Auv):
    def __init__(self, auv_id: str, version: str, boats: list[tuple[int, int]]) -> None:
        super().__init__(auv_id, version, boats)
        self._last_planted = False

    def decide(self) -> str:
        if not self._last_planted and self.tile == "empty":
            self._last_planted = True
            return "plant"
        self._last_planted = False
        return "move"


if __name__ == "__main__":
    main(StraightAuv, "straight")
