class VolumeScanner:

    def __init__(self):

        self.last_price = {}

    def update(self, symbol, price):

        if symbol not in self.last_price:

            self.last_price[symbol] = price
            return None

        change = (price - self.last_price[symbol]) / self.last_price[symbol]

        self.last_price[symbol] = price

        if abs(change) > 0.002:

            return {
                "symbol": symbol,
                "momentum": change
            }

        return None
