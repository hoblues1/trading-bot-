class RegimeDetector:

    def __init__(self):

        self.prices = []

    def update(self, price):

        self.prices.append(price)

        if len(self.prices) > 50:
            self.prices.pop(0)

    def regime(self):

        if len(self.prices) < 20:
            return "UNKNOWN"

        change = (self.prices[-1] - self.prices[0]) / self.prices[0]

        if abs(change) > 0.02:
            return "TREND"

        return "RANGE"
