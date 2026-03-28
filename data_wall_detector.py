class WallDetector:

    def __init__(self):

        self.wall_threshold = 50

    def detect(self, orderbook):

        bids = orderbook["bids"]
        asks = orderbook["asks"]

        for price, qty in bids:

            if float(qty) > self.wall_threshold:

                return {
                    "symbol": orderbook["symbol"],
                    "type": "BID_WALL",
                    "price": float(price)
                }

        for price, qty in asks:

            if float(qty) > self.wall_threshold:

                return {
                    "symbol": orderbook["symbol"],
                    "type": "ASK_WALL",
                    "price": float(price)
                }

        return None
