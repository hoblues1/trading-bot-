from core.module import Module
from core.events import Events


class CoinScanner(Module):

    def __init__(self):

        super().__init__()

        self.active_symbols = set()

    def set_event_bus(self, bus):

        super().set_event_bus(bus)

        bus.subscribe("orderbook_signal", self.on_signal)

    async def on_signal(self, data):

        imbalance = data["imbalance"]

        if imbalance > 0.65:

            await self.event_bus.publish(
                "scanner_long",
                {
                    "symbol": data["symbol"],
                    "strength": imbalance
                }
            )

        elif imbalance < 0.35:

            await self.event_bus.publish(
                "scanner_short",
                {
                    "symbol": data["symbol"],
                    "strength": imbalance
                }
            )
