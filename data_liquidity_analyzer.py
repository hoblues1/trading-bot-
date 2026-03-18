class LiquidityAnalyzer:

    def __init__(self):

        pass

    async def analyze(self, data):

        bids = data["bid_volume"]
        asks = data["ask_volume"]

        if bids > asks * 2:

            return {
                "symbol": data["symbol"],
                "signal": "BUY_PRESSURE"
            }

        elif asks > bids * 2:

            return {
                "symbol": data["symbol"],
                "signal": "SELL_PRESSURE"
            }

        return None
