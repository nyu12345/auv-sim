"""v4 behaviour: seagrass planter. Wanders randomly and plants seagrass
on any empty tile it lands on."""

from auv import Auv, main


class PlanterAuv(Auv):
    def decide(self) -> str:
        if self.tile == "empty":
            return "plant"
        return super().decide()


if __name__ == "__main__":
    main(PlanterAuv, "planter")
