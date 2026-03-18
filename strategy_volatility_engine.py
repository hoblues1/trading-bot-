import time
from collections import deque
from typing import Dict, Deque, Any, Optional


class VolatilityEngine:
    """
    High-end volatility engine

    목표:
    1) symbol별 최근 가격 변동성 저장
    2) allow_trade() 유지해서 이전 코드와 호환
    3) signal(symbol) 제공해서 AlphaFusionEngine과 연결
    4) 너무 조용한 장 / 너무 과열된 장 필터링 가능
    5) 간단하지만 실전에서 바로 쓸 수 있는 구조
    """

    def __init__(
        self,
        window_seconds: float = 20.0,
        min_samples: int = 8,
        min_volatility: float = 0.0008,
        max_volatility: float = 0.03,
        shock_volatility: float = 0.06,
        max_stored_points_per_symbol: int = 4000,
    ):
        self.window_seconds = float(window_seconds)
        self.min_samples = int(min_samples)
        self.min_volatility = float(min_volatility)
        self.max_volatility = float(max_volatility)
        self.shock_volatility = float(shock_volatility)
        self.max_stored_points_per_symbol = int(max_stored_points_per_symbol)

        # symbol -> deque[(timestamp, price)]
        self.prices: Dict[str, Deque] = {}

    def _get_deque(self, symbol: str) -> Deque:
        if symbol not in self.prices:
            self.prices[symbol] = deque()
        return self.prices[symbol]

    def _prune(self, symbol: str, now: float) -> None:
        dq = self._get_deque(symbol)
        cutoff = now - self.window_seconds

        while dq and dq[0][0] < cutoff:
            dq.popleft()

        while len(dq) > self.max_stored_points_per_symbol:
            dq.popleft()

    def update(self, trade: Dict[str, Any]) -> None:
        symbol = trade.get("symbol")
        price = float(trade.get("price", 0.0))
        now = float(trade.get("timestamp", time.time()))

        if not symbol or price <= 0:
            return

        dq = self._get_deque(symbol)
        dq.append((now, price))
        self._prune(symbol, now)

    def _compute_volatility(self, symbol: str) -> Optional[Dict[str, Any]]:
        now = time.time()
        self._prune(symbol, now)
        dq = self._get_deque(symbol)

        if len(dq) < self.min_samples:
            return None

        prices = [p for _, p in dq]
        first_price = prices[0]
        last_price = prices[-1]

        if first_price <= 0 or last_price <= 0:
            return None

        # 단순 range 기반 실전형 변동성
        high_price = max(prices)
        low_price = min(prices)

        range_ratio = (high_price - low_price) / max(last_price, 1e-9)
        move_ratio = abs(last_price - first_price) / max(first_price, 1e-9)

        # 변동성 value는 range 중심 + 방향 이동 반영
        volatility_value = (range_ratio * 0.7) + (move_ratio * 0.3)

        if volatility_value < self.min_volatility:
            regime = "flat"
            allow_trade = False
        elif volatility_value > self.shock_volatility:
            regime = "shock"
            allow_trade = False
        elif volatility_value > self.max_volatility:
            regime = "overheated"
            allow_trade = False
        else:
            regime = "tradable"
            allow_trade = True

        return {
            "symbol": symbol,
            "value": float(volatility_value),
            "range_ratio": float(range_ratio),
            "move_ratio": float(move_ratio),
            "samples": len(prices),
            "window_seconds": self.window_seconds,
            "regime": regime,
            "allow_trade": allow_trade,
            "source": "volatility_engine",
            "ts": now,
        }

    def signal(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        AlphaFusionEngine 호환용
        """
        return self._compute_volatility(symbol)

    def allow_trade(self, atr) -> bool:
        """
        이전 코드 호환용
        숫자 하나를 넣어도 동작하게 유지
        """
        try:
            atr = float(atr)
        except Exception:
            return False

        if atr < self.min_volatility:
            return False

        if atr > self.max_volatility:
            return False

        return True

    def allow_trade_for_symbol(self, symbol: str) -> bool:
        stats = self._compute_volatility(symbol)
        if not stats:
            return False
        return bool(stats["allow_trade"])

    def snapshot(self, symbol: str) -> Optional[Dict[str, Any]]:
        return self._compute_volatility(symbol)

    def reset_symbol(self, symbol: str) -> None:
        self.prices[symbol] = deque()

    def reset_all(self) -> None:
        self.prices.clear()
