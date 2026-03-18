import numpy as np


class AlphaScoreEngine:

    def __init__(self):

        self.weights = {
            "momentum": 0.25,
            "volume": 0.20,
            "orderbook": 0.20,
            "liquidity": 0.15,
            "microstructure": 0.10,
            "regime": 0.10
        }

        self.threshold = 0.65


    def compute_alpha(self, data):

        momentum = self.momentum_score(data)
        volume = self.volume_score(data)
        orderbook = self.orderbook_score(data)
        liquidity = self.liquidity_score(data)
        micro = self.microstructure_score(data)
        regime = self.regime_score(data)

        score = (
            momentum * self.weights["momentum"] +
            volume * self.weights["volume"] +
            orderbook * self.weights["orderbook"] +
            liquidity * self.weights["liquidity"] +
            micro * self.weights["microstructure"] +
            regime * self.weights["regime"]
        )

        return score


    def momentum_score(self, data):

        price_now = data["price_now"]
        price_5s = data["price_5s"]

        change = (price_now - price_5s) / price_5s

        score = np.clip(change * 50, 0, 1)

        return score


    def volume_score(self, data):

        vol_now = data["volume_now"]
        vol_avg = data["volume_avg"]

        if vol_avg == 0:
            return 0

        ratio = vol_now / vol_avg

        score = np.clip((ratio - 1) / 3, 0, 1)

        return score


    def orderbook_score(self, data):

        bid = data["bid_volume"]
        ask = data["ask_volume"]

        total = bid + ask

        if total == 0:
            return 0

        imbalance = bid / total

        score = np.clip((imbalance - 0.5) * 3, 0, 1)

        return score


    def liquidity_score(self, data):

        sweep = data["liquidity_sweep"]

        if sweep:
            return 1

        return 0.2


    def microstructure_score(self, data):

        spoofing = data["spoofing_detected"]

        if spoofing:
            return 0

        return 0.8


    def regime_score(self, data):

        btc_trend = data["btc_trend"]

        if btc_trend == "up":
            return 1

        if btc_trend == "sideways":
            return 0.4

        return 0.1


    def should_trade(self, data):

        score = self.compute_alpha(data)

        if score > self.threshold:
            return True, score

        return False, score
