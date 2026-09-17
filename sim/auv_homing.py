"""v3 behaviour: homing cycle. Wander randomly for 50s, navigate to the
boat, stay on it for 3s, repeat."""

from auv import Auv, main

TICK_SECONDS = 0.5  # server.py TICK_SECONDS
RANDOM_TICKS = int(50 / TICK_SECONDS)
DOCK_TICKS = int(3 / TICK_SECONDS)

# Clockwise, matching server.py CardinalDirection.
DIR_ORDER = ["N", "E", "S", "W"]


class HomingAuv(Auv):
    def __init__(self, auv_id: str, version: str, boats: list[tuple[int, int]]) -> None:
        super().__init__(auv_id, version, boats)
        self.boat_x, self.boat_y = boats[0]
        self.phase = "random"
        self.phase_ticks = 0

    def _desired_direction(self) -> str:
        dx = self.boat_x - self.x
        dy = self.boat_y - self.y
        if abs(dx) >= abs(dy):
            return "E" if dx > 0 else "W"
        return "N" if dy > 0 else "S"

    def _turn_toward(self, target_dir: str) -> str:
        cw_dist = (DIR_ORDER.index(target_dir) - DIR_ORDER.index(self.dir)) % 4
        if cw_dist == 0:
            return "move"
        return "turn_right" if cw_dist <= 2 else "turn_left"

    def decide(self) -> str:
        self.phase_ticks += 1

        if self.phase == "random":
            if self.phase_ticks >= RANDOM_TICKS:
                self.phase = "homing"
                self.phase_ticks = 0
            return super().decide()  # base Auv.decide() already weights plant equally

        if self.phase == "homing":
            if self.x == self.boat_x and self.y == self.boat_y:
                self.phase = "docked"
                self.phase_ticks = 0
                return "wait"
            return self._turn_toward(self._desired_direction())

        if self.phase_ticks >= DOCK_TICKS:
            self.phase = "random"
            self.phase_ticks = 0
            return super().decide()
        return "wait"


if __name__ == "__main__":
    main(HomingAuv, "homing")
