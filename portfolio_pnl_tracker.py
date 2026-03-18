class PnLTracker:

    def __init__(self):

        self.positions = {}

    def update(self, trade):

        symbol = trade["symbol"]
        price = trade["price"]
        qty = trade["qty"]

        if symbol not in self.positions:

            self.positions[symbol] = {
                "qty": qty,
                "avg": price
            }

            return

        pos = self.positions[symbol]

        pos["qty"] += qty

        pos["avg"] = (pos["avg"] + price) / 2

    def pnl(self, symbol, current_price):

        if symbol not in self.positions:
            return 0

        pos = self.positions[symbol]

        return (current_price - pos["avg"]) * pos["qty"]
