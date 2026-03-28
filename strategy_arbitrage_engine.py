class ArbitrageEngine:

    def __init__(self):

        self.prices = {}

    def update(self, exchange, symbol, price):

        if symbol not in self.prices:

            self.prices[symbol] = {}

        self.prices[symbol][exchange] = price

    def opportunity(self, symbol):

        exchanges = self.prices.get(symbol)

        if not exchanges or len(exchanges) < 2:
            return None

        prices = list(exchanges.values())

        low = min(prices)
        high = max(prices)

        diff = (high - low) / low

        if diff > 0.003:

            return {
                "symbol": symbol,
                "spread": diff
            }

        return None
