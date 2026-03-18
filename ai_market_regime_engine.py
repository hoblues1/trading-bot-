import time
from collections import deque
from typing import Dict, Deque, Any, Optional


class MarketRegimeEngine:

    def __init__(
        self,
        window_seconds: float = 35.0,
        min_samples: int = 6,
        trend_ratio_threshold: float = 0.52,
        shock_ratio_threshold: float = 0.028,
        chop_band_threshold: float = 0.0045,
        range_trade_band_low: float = 0.0010,
        range_trade_band_high: float = 0.018,
        min_efficiency_for_trend: float = 0.06,
        min_efficiency_for_chop_trade: float = 0.15,
        max_stored_points_per_symbol: int = 4000,
        high_volatility_ratio: float = 0.03,
        low_liquidity_spread_bps: float = 10.0,
        low_liquidity_depth_ratio: float = 0.60,
    ):
        self.window_seconds = float(window_seconds)
        self.min_samples = int(min_samples)
        self.trend_ratio_threshold = float(trend_ratio_threshold)
        self.shock_ratio_threshold = float(shock_ratio_threshold)
        self.chop_band_threshold = float(chop_band_threshold)
        self.range_trade_band_low = float(range_trade_band_low)
        self.range_trade_band_high = float(range_trade_band_high)
        self.min_efficiency_for_trend = float(min_efficiency_for_trend)
        self.min_efficiency_for_chop_trade = float(min_efficiency_for_chop_trade)
        self.max_stored_points_per_symbol = int(max_stored_points_per_symbol)

        self.high_volatility_ratio = float(high_volatility_ratio)
        self.low_liquidity_spread_bps = float(low_liquidity_spread_bps)
        self.low_liquidity_depth_ratio = float(low_liquidity_depth_ratio)

        self.prices: Dict[str, Deque] = {}

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return float(default)
            return float(value)
        except Exception:
            return float(default)

    def _get_deque(self, symbol: str) -> Deque:
        if symbol not in self.prices:
            self.prices[symbol] = deque()
        return self.prices[symbol]

    def _prune(self, symbol: str, now: float):
        dq = self._get_deque(symbol)
        cutoff = now - self.window_seconds

        while dq and dq[0][0] < cutoff:
            dq.popleft()

        while len(dq) > self.max_stored_points_per_symbol:
            dq.popleft()

    def update(self, symbol: str, price: float, ts: Optional[float] = None):
        if not symbol or price <= 0:
            return

        now = ts if ts else time.time()
        dq = self._get_deque(symbol)
        dq.append((now, price))
        self._prune(symbol, now)

    def _extract_market_context_flags(self, market_context: Optional[Dict[str, Any]]) -> Dict[str, float]:
        ctx = market_context or {}

        spread_bps = self._safe_float(ctx.get("spread_bps"), 0.0)
        top_book_ratio = self._safe_float(ctx.get("top_book_ratio"), 0.0)
        depth_ratio = self._safe_float(ctx.get("depth_ratio"), 0.0)
        book_pressure = self._safe_float(ctx.get("book_pressure"), 0.0)

        return {
            "spread_bps": spread_bps,
            "top_book_ratio": top_book_ratio,
            "depth_ratio": depth_ratio,
            "book_pressure": book_pressure,
        }

    def _compute(self, symbol: str, market_context: Optional[Dict[str, Any]] = None):
        now = time.time()
        self._prune(symbol, now)

        dq = self._get_deque(symbol)
        if len(dq) < self.min_samples:
            return None

        prices = [p for _, p in dq]

        first_price = prices[0]
        last_price = prices[-1]
        high_price = max(prices)
        low_price = min(prices)

        total_move_ratio = abs(last_price - first_price) / max(first_price, 1e-9)
        range_ratio = (high_price - low_price) / max(last_price, 1e-9)

        up_moves = 0
        down_moves = 0
        total_path = 0.0

        for i in range(1, len(prices)):
            delta = prices[i] - prices[i - 1]

            if delta > 0:
                up_moves += 1
            elif delta < 0:
                down_moves += 1

            total_path += abs(delta)

        directional_count = max(up_moves, down_moves)
        total_directional = max(up_moves + down_moves, 1)
        directional_ratio = directional_count / total_directional

        net_move = abs(last_price - first_price)
        efficiency_ratio = net_move / max(total_path, 1e-9)

        direction = "FLAT"
        if last_price > first_price:
            direction = "UP"
        elif last_price < first_price:
            direction = "DOWN"

        ctx_flags = self._extract_market_context_flags(market_context)
        spread_bps = ctx_flags["spread_bps"]
        top_book_ratio = ctx_flags["top_book_ratio"]
        depth_ratio = ctx_flags["depth_ratio"]
        book_pressure = ctx_flags["book_pressure"]

        regime = "CHOP"
        allow_trade = False

        if range_ratio >= self.shock_ratio_threshold:
            regime = "SHOCK"
            allow_trade = False

        elif spread_bps >= self.low_liquidity_spread_bps or (
            depth_ratio > 0 and depth_ratio < self.low_liquidity_depth_ratio
        ):
            regime = "LOW_LIQUIDITY"
            allow_trade = False

        elif range_ratio >= self.high_volatility_ratio and efficiency_ratio < self.min_efficiency_for_trend:
            regime = "HIGH_VOL"
            allow_trade = False

        elif (
            directional_ratio >= self.trend_ratio_threshold
            and efficiency_ratio >= self.min_efficiency_for_trend
        ):
            if direction == "UP":
                regime = "TREND_UP"
            elif direction == "DOWN":
                regime = "TREND_DOWN"
            else:
                regime = "TREND"
            allow_trade = True

        elif (
            self.range_trade_band_low <= range_ratio <= self.range_trade_band_high
            and efficiency_ratio >= self.min_efficiency_for_chop_trade
        ):
            regime = "RANGE"
            allow_trade = True

        else:
            regime = "CHOP"
            allow_trade = False

        confidence = 0.50
        if regime in ("TREND_UP", "TREND_DOWN", "TREND"):
            confidence = min(0.95, 0.55 + efficiency_ratio + (directional_ratio - 0.5))
        elif regime == "RANGE":
            confidence = min(0.88, 0.52 + (range_ratio * 10.0) + (efficiency_ratio * 0.25))
        elif regime in ("SHOCK", "HIGH_VOL", "LOW_LIQUIDITY"):
            confidence = min(0.98, 0.60 + (range_ratio * 8.0))
        else:
            confidence = min(0.80, 0.45 + (range_ratio * 6.0))

        return {
            "symbol": symbol,
            "regime": regime,
            "allow_trade": allow_trade,
            "samples": len(prices),
            "ts": now,
            "direction": direction,
            "total_move_ratio": total_move_ratio,
            "range_ratio": range_ratio,
            "directional_ratio": directional_ratio,
            "efficiency_ratio": efficiency_ratio,
            "spread_bps": spread_bps,
            "top_book_ratio": top_book_ratio,
            "depth_ratio": depth_ratio,
            "book_pressure": book_pressure,
            "confidence": round(confidence, 6),
            "source": "market_regime_engine",
        }

    def classify(
        self,
        symbol: str,
        trade: Optional[Dict[str, Any]] = None,
        price: float = None,
        market_context: Optional[Dict[str, Any]] = None,
    ):
        if price is not None:
            self.update(symbol, float(price), time.time())

        elif trade is not None:
            p = float(trade.get("price", trade.get("px", trade.get("close", 0))))
            ts = trade.get("timestamp", time.time())
            if p > 0:
                self.update(symbol, p, ts)

        stats = self._compute(symbol, market_context=market_context)

        if not stats:
            return {
                "symbol": symbol,
                "regime": "WARMUP",
                "allow_trade": False,
                "samples": len(self._get_deque(symbol)),
                "min_samples": self.min_samples,
                "source": "market_regime_engine",
                "ts": time.time(),
                "confidence": 0.0,
            }

        return stats

    def signal(
        self,
        symbol: str,
        trade: Optional[Dict[str, Any]] = None,
        price: float = None,
        market_context: Optional[Dict[str, Any]] = None,
    ):
        return self.classify(symbol, trade, price, market_context=market_context)

    def allow_trade(self, symbol: str, market_context: Optional[Dict[str, Any]] = None):
        stats = self._compute(symbol, market_context=market_context)
        if not stats:
            return False
        return bool(stats["allow_trade"])

    def snapshot(self, symbol: Optional[str] = None):
        if symbol:
            stats = self._compute(symbol)
            if stats:
                return stats
            return {
                "symbol": symbol,
                "regime": "WARMUP",
                "allow_trade": False,
                "samples": len(self._get_deque(symbol)),
                "min_samples": self.min_samples,
                "source": "market_regime_engine",
                "ts": time.time(),
                "confidence": 0.0,
            }

        return {
            "window_seconds": self.window_seconds,
            "min_samples": self.min_samples,
            "trend_ratio_threshold": self.trend_ratio_threshold,
            "shock_ratio_threshold": self.shock_ratio_threshold,
            "chop_band_threshold": self.chop_band_threshold,
            "range_trade_band_low": self.range_trade_band_low,
            "range_trade_band_high": self.range_trade_band_high,
            "min_efficiency_for_trend": self.min_efficiency_for_trend,
            "min_efficiency_for_chop_trade": self.min_efficiency_for_chop_trade,
            "max_stored_points_per_symbol": self.max_stored_points_per_symbol,
            "high_volatility_ratio": self.high_volatility_ratio,
            "low_liquidity_spread_bps": self.low_liquidity_spread_bps,
            "low_liquidity_depth_ratio": self.low_liquidity_depth_ratio,
        }
