class AdaptiveSelector:

    def __init__(self):

        self.current = "scalp"

    def select(self, regime):

        if regime == "TREND":

            self.current = "momentum"

        elif regime == "RANGE":

            self.current = "mean_reversion"

        else:

            self.current = "scalp"

        return self.current
