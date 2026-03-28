class VolatilityBreakout:

    def __init__(self):

        self.high = {}
        self.low = {}

    def update(self, symbol, price):

        if symbol not in self.high:

            self.high[symbol] = price
            self.low[symbol] = price
            return None

        if price > self.high[symbol]:

            self.high[symbol] = price

            return {
                "symbol": symbol,
                "side": "BUY"
            }

        if price < self.low[symbol]:

            self.low[symbol] = price

            return {
                "symbol": symbol,
                "side": "SELL"
            }

        return None
