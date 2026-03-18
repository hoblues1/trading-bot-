class StrategyWeightEngine:

    def __init__(self):

        self.performance = {
            "scalp": 1.0,
            "momentum": 1.0,
            "breakout": 1.0
        }

    def update(self, strategy, pnl):

        self.performance[strategy] += pnl

    def best_strategy(self):

        return max(self.performance, key=self.performance.get)
