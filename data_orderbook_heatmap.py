class OrderbookHeatmap:

    def __init__(self):

        self.levels = {}

    def update(self, orderbook):

        symbol = orderbook["symbol"]

        bids = orderbook["bids"]
        asks = orderbook["asks"]

        heat = []

        for price, qty in bids:

            if float(qty) > 20:

                heat.append(
                    {
                        "side": "bid",
                        "price": float(price),
                        "qty": float(qty)
                    }
                )

        for price, qty in asks:

            if float(qty) > 20:

                heat.append(
                    {
                        "side": "ask",
                        "price": float(price),
                        "qty": float(qty)
                    }
                )

        self.levels[symbol] = heat

        return heat
