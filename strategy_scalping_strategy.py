from core.module import Module


class ScalpingStrategy(Module):

    def __init__(self):

        super().__init__()

    def set_event_bus(self, bus):

        super().set_event_bus(bus)

        bus.subscribe("scanner_long", self.on_long_signal)

    async def on_long_signal(self, data):

        symbol = data["symbol"]

        order = {
            "symbol": symbol,
            "side": "BUY",
            "type": "MARKET",
            "qty": 50
        }

        await self.event_bus.publish("order_request", order)
