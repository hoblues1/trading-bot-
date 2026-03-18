class MultiTimeframeMomentum:

    def __init__(self):

        self.prices = {}

    def update(self, symbol, price):

        if symbol not in self.prices:

            self.prices[symbol] = []

        self.prices[symbol].append(price)

        if len(self.prices[symbol]) > 60:

            self.prices[symbol].pop(0)

    def signal(self, symbol):

        p = self.prices.get(symbol)

        if not p or len(p) < 20:

            return None

        short = p[-1] - p[-5]
        mid = p[-1] - p[-20]

        if short > 0 and mid > 0:

            return {
                "symbol": symbol,
                "side": "BUY"
            }

        if short < 0 and mid < 0:

            return {
                "symbol": symbol,
                "side": "SELL"
            }

        return None
