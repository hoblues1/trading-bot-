import numpy as np


class MarketAIBrain:
    def __init__(self):
        self.weights = {
            "alpha": 0.34,
            "volume": 0.18,
            "volatility": 0.16,
            "liquidity": 0.17,
            "btc": 0.15,
        }

        # 기본 진입 기준
        self.base_trade_threshold = 0.57

        # 동적 threshold 범위
        self.min_trade_threshold = 0.50
        self.max_trade_threshold = 0.66

        # 리스크 패널티 강도
        self.overheat_vol_penalty = 0.10
        self.low_volume_penalty = 0.08
        self.bad_btc_penalty = 0.06
        self.no_liquidity_penalty = 0.05

    def compute_market_score(self, data):
        alpha = self.alpha_score(data)
        volume = self.volume_score(data)
        volatility = self.volatility_score(data)
        liquidity = self.liquidity_score(data)
        btc = self.btc_score(data)

        raw_score = (
            alpha * self.weights["alpha"] +
            volume * self.weights["volume"] +
            volatility * self.weights["volatility"] +
            liquidity * self.weights["liquidity"] +
            btc * self.weights["btc"]
        )

        penalty = self.risk_penalty(data, volume, volatility, liquidity, btc)

        final_score = raw_score - penalty
        return float(np.clip(final_score, 0.0, 1.0))

    def alpha_score(self, data):
        """
        alpha_score는 핵심 신호이므로 비선형 스케일 적용.
        약한 신호는 더 약하게, 강한 신호는 더 명확하게 반영.
        """
        a = float(data.get("alpha_score", 0.0))
        a = float(np.clip(a, 0.0, 1.0))

        # 약한 알파는 과대평가하지 않고, 강한 알파는 살려줌
        return float(np.clip(a ** 0.85, 0.0, 1.0))

    def volume_score(self, data):
        """
        volume_ratio:
        1.0 = 평균 수준
        1.5 이상부터 유의미
        너무 낮으면 거래 빈도만 늘고 질이 떨어짐
        """
        v = float(data.get("volume_ratio", 1.0))

        # 완만하게 반응하게 조정
        score = (v - 0.9) / 1.6
        return float(np.clip(score, 0.0, 1.0))

    def volatility_score(self, data):
        """
        volatility는 무조건 높다고 좋은 게 아님.
        적당한 변동성이 가장 좋고, 과열은 오히려 감점.
        """
        vol = float(data.get("volatility", 0.0))

        # sweet spot 중심 점수
        # 0.015 ~ 0.035 근처가 가장 좋고 너무 낮거나 너무 높으면 감점
        center = 0.024
        width = 0.020

        score = 1.0 - abs(vol - center) / width
        return float(np.clip(score, 0.0, 1.0))

    def liquidity_score(self, data):
        """
        유동성 sweep / 오더북 흡수 흔적이 있으면 높게,
        없더라도 완전 0은 아니게 설정.
        """
        if bool(data.get("liquidity_sweep", False)):
            return 0.95

        orderbook_depth_ok = bool(data.get("orderbook_depth_ok", False))
        spread_ok = bool(data.get("spread_ok", True))

        if orderbook_depth_ok and spread_ok:
            return 0.55

        if spread_ok:
            return 0.40

        return 0.22

    def btc_score(self, data):
        """
        시장 전체 방향성 반영.
        추세 역행은 막되, 횡보장에서는 완전 차단하지 않음.
        """
        trend = str(data.get("btc_trend", "sideways")).lower()

        if trend == "up":
            return 0.95
        if trend == "sideways":
            return 0.62
        if trend == "down":
            return 0.28

        return 0.50

    def risk_penalty(self, data, volume, volatility, liquidity, btc):
        penalty = 0.0

        raw_vol = float(data.get("volatility", 0.0))
        raw_volume_ratio = float(data.get("volume_ratio", 1.0))
        liquidity_sweep = bool(data.get("liquidity_sweep", False))
        btc_trend = str(data.get("btc_trend", "sideways")).lower()

        # 1) 과열 변동성 패널티
        if raw_vol > 0.045:
            penalty += self.overheat_vol_penalty

        # 2) 거래량 부실 패널티
        if raw_volume_ratio < 1.0:
            penalty += self.low_volume_penalty

        # 3) 유동성 취약 패널티
        if not liquidity_sweep and liquidity < 0.45:
            penalty += self.no_liquidity_penalty

        # 4) BTC 역풍 패널티
        if btc_trend == "down" and btc < 0.35:
            penalty += self.bad_btc_penalty

        # 5) 조합 패널티: 거래량도 약하고 유동성도 약하면 추가 감점
        if volume < 0.25 and liquidity < 0.35:
            penalty += 0.04

        return float(np.clip(penalty, 0.0, 0.35))

    def dynamic_trade_threshold(self, data):
        """
        거래를 자주 하되, 장이 안 좋을 땐 threshold를 높여서 계좌 보호.
        장이 좋으면 threshold를 살짝 낮춰 진입 빈도 확보.
        """
        threshold = self.base_trade_threshold

        alpha = self.alpha_score(data)
        volume = self.volume_score(data)
        volatility = self.volatility_score(data)
        liquidity = self.liquidity_score(data)
        btc = self.btc_score(data)

        # 좋은 조건이면 threshold 완화 → 거래 빈도 증가
        if alpha > 0.70:
            threshold -= 0.03
        if volume > 0.60:
            threshold -= 0.02
        if liquidity > 0.75:
            threshold -= 0.02
        if btc > 0.80:
            threshold -= 0.01

        # 나쁜 조건이면 threshold 강화 → 계좌 보호
        if volatility < 0.30:
            threshold += 0.02
        if volume < 0.20:
            threshold += 0.03
        if liquidity < 0.30:
            threshold += 0.03
        if btc < 0.35:
            threshold += 0.02

        return float(np.clip(
            threshold,
            self.min_trade_threshold,
            self.max_trade_threshold
        ))

    def rank_markets(self, market_data):
        ranking = []

        for symbol, data in market_data.items():
            score = self.compute_market_score(data)
            ranking.append((score, symbol))

        ranking.sort(reverse=True)
        return ranking

    def get_best_trades(self, market_data, top_n=3):
        ranked = self.rank_markets(market_data)
        candidates = []

        for score, symbol in ranked[:top_n]:
            data = market_data[symbol]
            threshold = self.dynamic_trade_threshold(data)

            # 최소 안전조건
            alpha = self.alpha_score(data)
            liquidity = self.liquidity_score(data)
            btc = self.btc_score(data)

            # 거래를 자주 하되, 너무 허접한 조건은 차단
            if alpha < 0.18:
                continue
            if liquidity < 0.22:
                continue
            if btc < 0.20:
                continue

            if score >= threshold:
                candidates.append({
                    "symbol": symbol,
                    "score": float(score),
                    "threshold": float(threshold),
                })

        return candidates
