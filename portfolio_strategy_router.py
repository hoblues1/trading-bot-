class StrategyRouter:

    def __init__(self):

        self.strategies = []

    def add(self, strategy):

        self.strategies.append(strategy)

    async def route(self, data):

        signals = []

        for s in self.strategies:

            sig = await s.on_data(data)

            if sig:
                signals.append(sig)

        return signals
