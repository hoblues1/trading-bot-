from core.module import Module
from core.events import Events


class OrderbookAnalyzer(Module):

    def __init__(self):

        super().__init__()

    def set_event_bus(self, bus):

        super().set_event_bus(bus)

        bus.subscribe(Events.ORDERBOOK, self.on_orderbook)

    async def on_orderbook(self, data):

        bids = data["bids"]
        asks = data["asks"]

        bid_volume = 0
        ask_volume = 0

        for price, qty in bids:

            bid_volume += float(qty)

        for price, qty in asks:

            ask_volume += float(qty)

        total = bid_volume + ask_volume

        if total == 0:
            return

        imbalance = bid_volume / total

        signal = {
            "symbol": data["symbol"],
            "imbalance": imbalance,
            "bid_volume": bid_volume,
            "ask_volume": ask_volume
        }

        await self.event_bus.publish("orderbook_signal", signal)
