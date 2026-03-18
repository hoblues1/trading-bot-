class StrategyOptimizer:

    def __init__(self):

        self.stats = {}

    def record(self, strategy, pnl):

        if strategy not in self.stats:

            self.stats[strategy] = []

        self.stats[strategy].append(pnl)

    def best(self):

        best = None
        best_score = -999

        for s, results in self.stats.items():

            score = sum(results)

            if score > best_score:

                best = s
                best_score = score

        return best
