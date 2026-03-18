class PositionSizer:

    def __init__(self, balance):

        self.balance = balance

        self.risk_per_trade = 0.01

    def size(self, price, stop_distance):

        risk_amount = self.balance * self.risk_per_trade

        position_size = risk_amount / stop_distance

        qty = position_size / price

        return round(qty, 3)
