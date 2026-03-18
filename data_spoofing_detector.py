import time


class SpoofingDetector:

    def __init__(self):

        self.previous = {}

    def detect(self, orderbook):

        symbol = orderbook["symbol"]

        bids = orderbook["bids"]
        asks = orderbook["asks"]

        bid_volume = sum(float(q) for p, q in bids)
        ask_volume = sum(float(q) for p, q in asks)

        now = time.time()

        if symbol not in self.previous:

            self.previous[symbol] = {
                "bid": bid_volume,
                "ask": ask_volume,
                "time": now
            }

            return None

        prev = self.previous[symbol]

        bid_change = bid_volume - prev["bid"]
        ask_change = ask_volume - prev["ask"]

        self.previous[symbol] = {
            "bid": bid_volume,
            "ask": ask_volume,
            "time": now
        }

        if abs(bid_change) > prev["bid"] * 0.5:

            return {
                "symbol": symbol,
                "type": "BID_SPOOF"
            }

        if abs(ask_change) > prev["ask"] * 0.5:

            return {
                "symbol": symbol,
                "type": "ASK_SPOOF"
            }

        return None
