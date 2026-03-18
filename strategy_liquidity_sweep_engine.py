import numpy as np


class LiquiditySweepEngine:

    def __init__(self):

        self.lookback = 5
        self.sweep_threshold = 0.003
        self.recovery_threshold = 0.0015

    def detect_long_sweep(self, prices):

        if len(prices) < self.lookback:
            return False

        recent = prices[-self.lookback:]

        high = max(recent)
        low = min(recent)
        last = recent[-1]

        drop = (high - low) / high
        recovery = (last - low) / low

        if drop > self.sweep_threshold and recovery > self.recovery_threshold:
            return True

        return False

    def detect_short_sweep(self, prices):

        if len(prices) < self.lookback:
            return False

        recent = prices[-self.lookback:]

        high = max(recent)
        low = min(recent)
        last = recent[-1]

        pump = (high - low) / low
        pullback = (high - last) / high

        if pump > self.sweep_threshold and pullback > self.recovery_threshold:
            return True

        return False

    def get_signal(self, prices):

        if self.detect_long_sweep(prices):
            return "LONG"

        if self.detect_short_sweep(prices):
            return "SHORT"

        return None
