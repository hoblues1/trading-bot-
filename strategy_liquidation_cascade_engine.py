import time


class LiquidationCascadeEngine:

    def __init__(self):

        self.liquidation_volume = {}
        self.liquidation_count = {}
        self.last_reset = {}

        self.window = 4
        self.liq_threshold = 5
        self.volume_threshold = 10000

    def update(self, liquidation):

        symbol = liquidation["symbol"]
        qty = liquidation["qty"]

        now = time.time()

        if symbol not in self.liquidation_volume:

            self.liquidation_volume[symbol] = 0
            self.liquidation_count[symbol] = 0
            self.last_reset[symbol] = now

        self.liquidation_volume[symbol] += qty
        self.liquidation_count[symbol] += 1

        if now - self.last_reset[symbol] > self.window:

            self.liquidation_volume[symbol] = 0
            self.liquidation_count[symbol] = 0
            self.last_reset[symbol] = now

    def signal(self, symbol):

        count = self.liquidation_count.get(symbol, 0)
        volume = self.liquidation_volume.get(symbol, 0)

        if count > self.liq_threshold:

            if volume > self.volume_threshold:

                return {
                    "symbol": symbol,
                    "cascade": True,
                    "count": count,
                    "volume": volume
                }

        return None
