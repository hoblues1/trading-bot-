class RiskAllocator:

    def __init__(self, capital):

        self.capital = capital

        self.allocations = {
            "scalp": 0.4,
            "momentum": 0.3,
            "breakout": 0.3
        }

    def capital_for(self, strategy):

        weight = self.allocations.get(strategy, 0.1)

        return self.capital * weight
