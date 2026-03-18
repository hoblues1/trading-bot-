class TopUniverseScanner:

    def __init__(self):

        self.universe = []

    def update(self, market_data):

        sorted_coins = sorted(
            market_data,
            key=lambda x: x["volume"],
            reverse=True
        )

        self.universe = sorted_coins[:300]

    def symbols(self):

        return [c["symbol"] for c in self.universe]
