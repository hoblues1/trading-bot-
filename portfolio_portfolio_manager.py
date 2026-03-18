from core.module import Module


class PortfolioManager(Module):

    def __init__(self):

        super().__init__()

        self.positions = {}

    def set_event_bus(self, bus):

        super().set_event_bus(bus)

        bus.subscribe("order_filled", self.on_fill)

    async def on_fill(self, data):

        symbol = data["symbol"]

        qty = float(data["executedQty"])

        self.positions[symbol] = self.positions.get(symbol, 0) + qty

        print("Portfolio:", self.positions)
