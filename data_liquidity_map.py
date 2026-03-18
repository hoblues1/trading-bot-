class LiquidityMap:

    def __init__(self):

        self.map = {}

    def update(self, orderbook):

        symbol = orderbook["symbol"]

        bids = orderbook["bids"]
        asks = orderbook["asks"]

        levels = {}

        for price, qty in bids:

            levels[float(price)] = float(qty)

        for price, qty in asks:

            levels[float(price)] = float(qty)

        self.map[symbol] = levels

    def strongest_level(self, symbol):

        levels = self.map.get(symbol)

        if not levels:

            return None

        price = max(levels, key=levels.get)

        return {
            "price": price,
            "liquidity": levels[price]
        }
