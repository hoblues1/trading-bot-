class UniverseScanner:

    def __init__(self):

        self.volume = {}

    def update(self, symbol, volume):

        if symbol not in self.volume:

            self.volume[symbol] = volume
            return None

        change = volume - self.volume[symbol]

        self.volume[symbol] = volume

        if change > 100000:

            return {
                "symbol": symbol,
                "type": "VOLUME_SURGE"
            }

        return None
