import time
from typing import Any, Dict, Optional


class TradeFilterEngine:
    """
    High-end trade filter engine

    목표:
    1) 기존 allow(velocity, pressure, volatility, regime) 방식 호환
    2) 현재 main의 allow(symbol=..., signal=..., price=price, trade=trade) 방식도 호환
    3) 신호 품질(score/confidence/source) 기반 필터링
    4) symbol별 지나친 재허용/재거절 흔들림 억제
    5) regime / volatility 형식이 제각각이어도 최대한 흡수
    6) alpha / regime direction / micro timing까지 반영한 진입 품질 필터 제공
    """

    def __init__(
        self,
        min_score: float = 0.32,
        min_confidence: float = 0.30,
        signal_accept_cooldown_seconds: float = 0.90,
        reject_cooldown_seconds: float = 0.20,
        block_sources=None,
        blocked_regimes=None,
        allowed_regimes=None,
    ):
        self.min_score = float(min_score)
        self.min_confidence = float(min_confidence)
        self.signal_accept_cooldown_seconds = float(signal_accept_cooldown_seconds)
        self.reject_cooldown_seconds = float(reject_cooldown_seconds)

        self.block_sources = set(block_sources or [])
        self.blocked_regimes = set(blocked_regimes or {"NO_TRADE", "BLOCKED", "SHOCK", "LOW_LIQUIDITY"})
        self.allowed_regimes = set(allowed_regimes or [])

        self.last_accept_ts: Dict[str, float] = {}
        self.last_reject_ts: Dict[str, float] = {}

    # ================= INTERNAL =================
    def _now(self) -> float:
        return time.time()

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return default

    def _extract_regime_label(self, regime: Any) -> Optional[str]:
        if regime is None:
            return None

        if isinstance(regime, str):
            return regime.upper().strip()

        if isinstance(regime, dict):
            for key in ("regime", "label", "state", "market_regime"):
                if key in regime and regime[key] is not None:
                    return str(regime[key]).upper().strip()

        return None

    def _regime_allows(self, regime: Any) -> bool:
        if regime is None:
            return True

        if isinstance(regime, bool):
            return regime

        if isinstance(regime, dict):
            label = self._extract_regime_label(regime)
            if label in {"WARMUP", "SHOCK", "LOW_LIQUIDITY"}:
                return False

            if "allow_trade" in regime:
                if bool(regime["allow_trade"]) is False and label not in {"RANGE", "TREND_UP", "TREND_DOWN", "TREND"}:
                    return False

        label = self._extract_regime_label(regime)
        if label is None:
            return True

        if self.allowed_regimes:
            return label in self.allowed_regimes

        return label not in self.blocked_regimes

    def _volatility_allows(self, volatility: Any) -> bool:
        if volatility is None:
            return True

        if isinstance(volatility, bool):
            return volatility

        if isinstance(volatility, (int, float)):
            return volatility > 0

        if isinstance(volatility, dict):
            if "allow_trade" in volatility:
                return bool(volatility["allow_trade"])

            if "allowed" in volatility:
                return bool(volatility["allowed"])

            value = volatility.get("value", volatility.get("volatility"))
            if value is not None:
                return self._safe_float(value, 0.0) > 0

        return True

    def _source_allowed(self, source: Optional[str]) -> bool:
        if not source:
            return True
        return str(source) not in self.block_sources

    def _signal_quality_allows(self, signal: Any, side_hint: Optional[str] = None) -> bool:
        if signal is None:
            return False

        # ===== 숫자 신호 허용 =====
        if isinstance(signal, (int, float)):
            value = float(signal)

            if value == 0:
                return False

            score = abs(value)
            confidence = abs(value)

            if score < self.min_score:
                return False

            if confidence < self.min_confidence:
                return False

            return True

        # ===== dict 신호 처리 =====
        if not isinstance(signal, dict):
            return False

        side = str(signal.get("side", side_hint or "")).upper().strip()

        if side not in ("BUY", "SELL"):
            raw_signal = signal.get("signal")
            if isinstance(raw_signal, (int, float)):
                if raw_signal > 0:
                    side = "BUY"
                elif raw_signal < 0:
                    side = "SELL"

        if side not in ("BUY", "SELL"):
            return False

        source = signal.get("source")
        if not self._source_allowed(source):
            return False

        score = self._safe_float(
            signal.get("weighted_score", signal.get("score", signal.get("strength", 0.0))),
            0.0,
        )
        confidence = self._safe_float(signal.get("confidence", signal.get("quality", 0.0)), 0.0)

        alpha = self._safe_float(signal.get("alpha", signal.get("signal", 0.0)), 0.0)

        micro = self._safe_float(signal.get("micro", 0.0), 0.0)
        pressure = self._safe_float(signal.get("pressure", 0.0), 0.0)
        velocity = self._safe_float(signal.get("velocity", 0.0), 0.0)
        imbalance = self._safe_float(signal.get("imbalance", 0.0), 0.0)

        regime = signal.get("regime", None)
        regime_label = self._extract_regime_label(regime)
        regime_direction = ""

        if isinstance(regime, dict):
            regime_direction = str(regime.get("direction", "")).upper().strip()

        # ===== 1. 기본 품질 =====
        if abs(score) < self.min_score:
            return False

        if confidence < self.min_confidence:
            return False

        # ===== 2. alpha 방향성 =====
        if alpha == 0:
            return False

        if side == "BUY" and alpha < 0:
            return False

        if side == "SELL" and alpha > 0:
            return False

        # ===== 3. regime 필터 =====
        if regime_label in {"WARMUP", "SHOCK", "LOW_LIQUIDITY", "BLOCKED", "NO_TRADE"}:
            return False

        # ===== 4. regime 방향 일치 =====
        if regime_direction:
            if side == "BUY" and regime_direction not in {"UP", "LONG", ""}:
                return False
            if side == "SELL" and regime_direction not in {"DOWN", "SHORT", ""}:
                return False

        # ===== 5. micro 타이밍 품질 =====
        directional_sum = 0.0
        for v in (micro, pressure, velocity, imbalance):
            if side == "BUY" and v > 0:
                directional_sum += abs(v)
            elif side == "SELL" and v < 0:
                directional_sum += abs(v)

        if directional_sum < 1.0:
            return False

        return True

    def _symbol_accept_blocked(self, symbol: Optional[str]) -> bool:
        if not symbol:
            return False

        last = self.last_accept_ts.get(symbol, 0.0)
        return (self._now() - last) < self.signal_accept_cooldown_seconds

    def _symbol_reject_blocked(self, symbol: Optional[str]) -> bool:
        if not symbol:
            return False

        last = self.last_reject_ts.get(symbol, 0.0)
        return (self._now() - last) < self.reject_cooldown_seconds

    def _mark_accept(self, symbol: Optional[str]):
        if symbol:
            self.last_accept_ts[symbol] = self._now()

    def _mark_reject(self, symbol: Optional[str]):
        if symbol:
            self.last_reject_ts[symbol] = self._now()

    # ================= NEW STYLE =================
    def allow(
        self,
        velocity=None,
        pressure=None,
        volatility=None,
        regime=None,
        symbol=None,
        signal=None,
        price=None,
        trade=None,
        side=None,
        **kwargs,
    ):
        """
        1) 구형 호출:
           allow(velocity, pressure, volatility, regime)

        2) 신형 호출:
           allow(symbol=symbol, signal=signal, price=price, trade=trade, regime=..., volatility=...)
        """
        # ===== 신형 경로 =====
        if signal is not None or symbol is not None or price is not None or trade is not None:
            if self._symbol_accept_blocked(symbol):
                return False

            if self._symbol_reject_blocked(symbol):
                return False

            if not self._signal_quality_allows(signal, side_hint=side):
                self._mark_reject(symbol)
                return False

            if not self._volatility_allows(volatility):
                self._mark_reject(symbol)
                return False

            if not self._regime_allows(regime):
                self._mark_reject(symbol)
                return False

            self._mark_accept(symbol)
            return True

        # ===== 구형 경로 =====
        if not velocity:
            return False

        if not pressure:
            return False

        if not self._volatility_allows(volatility):
            return False

        if not self._regime_allows(regime):
            return False

        return True

    # ================= OPTIONAL HELPERS =================
    def filter(self, *args, **kwargs):
        """
        main에서 allow 대신 filter를 호출해도 호환되도록 유지
        """
        return self.allow(*args, **kwargs)

    def snapshot(self):
        return {
            "last_accept_ts": self.last_accept_ts,
            "last_reject_ts": self.last_reject_ts,
            "min_score": self.min_score,
            "min_confidence": self.min_confidence,
            "signal_accept_cooldown_seconds": self.signal_accept_cooldown_seconds,
            "reject_cooldown_seconds": self.reject_cooldown_seconds,
            "blocked_regimes": list(self.blocked_regimes),
            "allowed_regimes": list(self.allowed_regimes),
            "block_sources": list(self.block_sources),
        }

    def reset_symbol(self, symbol: str):
        self.last_accept_ts.pop(symbol, None)
        self.last_reject_ts.pop(symbol, None)

    def reset_all(self):
        self.last_accept_ts.clear()
        self.last_reject_ts.clear()