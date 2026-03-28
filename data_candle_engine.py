from core_module import Module
import time


class CandleEngine(Module):

    def __init__(self, interval=1):

        super().__init__()

        self.interval = interval
        self.current = {}

    def set_event_bus(self, bus):

        super().set_event_bus(bus)

        bus.subscribe("trade", self.on_trade)

    async def on_trade(self, trade):

        symbol = trade["symbol"]
        price = float(trade["price"])
        qty = float(trade["qty"])
        ts = int(trade["time"] / 1000)

        bucket = ts - (ts % self.interval)

        if symbol not in self.current:

            self.current[symbol] = {
                "time": bucket,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": qty
            }

            return

        candle = self.current[symbol]

        if candle["time"] != bucket:

            await self.event_bus.publish("candle", {
                "symbol": symbol,
                **candle
            })

            self.current[symbol] = {
                "time": bucket,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": qty
            }

            return

        candle["high"] = max(candle["high"], price)
        candle["low"] = min(candle["low"], price)
        candle["close"] = price
        candle["volume"] += qty
