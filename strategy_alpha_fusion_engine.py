import time
from typing import Any, Dict, Optional, List


class AlphaFusionEngine:
    """
    High-end alpha fusion engine

    목표:
    1) 여러 전략 신호를 단순 합산하지 않고 품질 기반으로 합성
    2) 같은 방향 연속 발사 억제
    3) 반대 방향 즉시 뒤집기 억제
    4) 최소 신호 개수 / 최소 확신도 / 최소 강도 기준 적용
    5) 전략별 dict / str / None 반환을 모두 안전하게 처리
    6) 디버깅 가능한 상세 메타데이터 제공
    7) regime / execution quality / drawdown 환경까지 반영 가능한 구조
    8) 기존 호출부 수정 최소화
    """

    def __init__(
        self,
        micro,
        pressure,
        velocity,
        volatility,
        buy_threshold: float = 0.95,
        sell_threshold: float = 0.95,
        min_agree_count: int = 1,
        signal_cooldown_seconds: float = 2.5,
        same_side_rearm_seconds: float = 5.0,
        flip_block_seconds: float = 8.0,
        strong_flip_multiplier: float = 1.15,
        use_volatility_filter: bool = True,
        min_volatility: float = 0.00035,
        max_volatility: float = 0.040,
        debug: bool = False,
    ):
        self.micro = micro
        self.pressure = pressure
        self.velocity = velocity
        self.volatility = volatility

        self.buy_threshold = float(buy_threshold)
        self.sell_threshold = float(sell_threshold)
        self.min_agree_count = int(min_agree_count)

        self.signal_cooldown_seconds = float(signal_cooldown_seconds)
        self.same_side_rearm_seconds = float(same_side_rearm_seconds)
        self.flip_block_seconds = float(flip_block_seconds)
        self.strong_flip_multiplier = float(strong_flip_multiplier)

        self.use_volatility_filter = bool(use_volatility_filter)
        self.min_volatility = float(min_volatility)
        self.max_volatility = float(max_volatility)

        self.debug = bool(debug)

        self.last_signal_ts: Dict[str, float] = {}
        self.last_signal_side: Dict[str, str] = {}

        self.weights = {
            "micro": 1.20,
            "pressure": 1.12,
            "velocity": 0.82,
        }

        self.regime_multipliers = {
            "TREND_UP": {"BUY": 0.8, "SELL": 0.9},
            "TREND_DOWN": {"BUY": 0.9, "SELL": 0.8},
            "TREND": {"BUY": 0.85, "SELL": 0.85},
            "MOMENTUM": {"BUY": 0.85, "SELL": 0.85},
            "BALANCED": {"BUY": 1.1, "SELL": 1.1},
            "NEUTRAL": {"BUY": 1.15, "SELL": 1.15},

            "MEAN_REVERSION": {"BUY": 1.2, "SELL": 1.2},
            "RANGE": {"BUY": 1.25, "SELL": 1.25},
            "CHOPPY": {"BUY": 1.35, "SELL": 1.35},
            "NOISE": {"BUY": 1.4, "SELL": 1.4},

            "PANIC": {"BUY": 1.2, "SELL": 1.2},
            "HIGH_VOL": {"BUY": 1.2, "SELL": 1.2},
            "LOW_LIQUIDITY": {"BUY": 1.3, "SELL": 1.3},
            "LIQUIDITY_CRISIS": {"BUY": 1.4, "SELL": 1.4},

            "RISK_OFF": {"BUY": 1.5, "SELL": 1.5},
            "DEFENSIVE": {"BUY": 1.3, "SELL": 1.3},

            "UNKNOWN": {"BUY": 1.0, "SELL": 1.0},
        }

    def _log(self, msg: str) -> None:
        if self.debug:
            print(msg)

    def _safe_call(self, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except TypeError:
            try:
                return fn(*args)
            except Exception:
                return None
        except Exception:
            return None

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return float(default)
            return float(value)
        except Exception:
            return float(default)

    def _clamp(self, value: float, low: float, high: float) -> float:
        return max(low, min(value, high))

    def _call_velocity_signal(self, symbol: str, qty: float, trade: Dict[str, Any]):
        raw = None

        if qty > 0:
            raw = self._safe_call(self.velocity.signal, symbol, qty)
            if raw is not None:
                return raw

        raw = self._safe_call(self.velocity.signal, symbol)
        if raw is not None:
            return raw

        raw = self._safe_call(self.velocity.signal, symbol, trade)
        if raw is not None:
            return raw

        raw = self._safe_call(self.velocity.signal, symbol, qty)
        return raw

    def _normalize_signal(self, raw: Any, source: str) -> Optional[Dict[str, Any]]:
        if raw is None:
            return None

        if isinstance(raw, str):
            side = raw.upper().strip()
            if side not in ("BUY", "SELL"):
                return None
            return {
                "source": source,
                "side": side,
                "strength": 1.0,
                "quality": 0.55,
                "raw": raw,
            }

        if isinstance(raw, dict):
            side = str(raw.get("side", "")).upper().strip()
            if side not in ("BUY", "SELL"):
                return None

            strength = self._safe_float(raw.get("strength", 1.0), 1.0)
            if strength <= 0:
                strength = 1.0

            quality = self._safe_float(raw.get("quality", raw.get("confidence", 0.6)), 0.6)
            quality = self._clamp(quality, 0.40, 1.0)

            normalized = dict(raw)
            normalized["source"] = source
            normalized["side"] = side
            normalized["strength"] = strength
            normalized["quality"] = quality
            return normalized

        return None

    def _clamp_strength(self, source: str, strength: float) -> float:
        if source == "micro":
            return max(0.55, min(strength, 2.8))
        if source == "pressure":
            return max(0.55, min(strength, 2.4))
        if source == "velocity":
            return max(0.55, min(strength, 2.1))
        return max(0.5, min(strength, 2.0))

    def _extract_volatility_value(self, raw: Any) -> Optional[float]:
        if raw is None:
            return None

        if isinstance(raw, (int, float)):
            return float(raw)

        if isinstance(raw, dict):
            for key in ("value", "volatility", "vol", "current_volatility", "atr_ratio"):
                if key in raw:
                    try:
                        return float(raw[key])
                    except Exception:
                        pass

        return None

    def _extract_regime_state(self, regime: Any) -> str:
        if regime is None:
            return "UNKNOWN"

        if isinstance(regime, str):
            return regime.upper().strip()

        if isinstance(regime, dict):
            for key in ("regime", "state", "market_regime", "name", "label"):
                if key in regime:
                    try:
                        return str(regime[key]).upper().strip()
                    except Exception:
                        pass

        return "UNKNOWN"

    def _regime_multiplier(self, regime: Any, side: str) -> float:
        state = self._extract_regime_state(regime)
        side_map = self.regime_multipliers.get(state, self.regime_multipliers["UNKNOWN"])
        return float(side_map.get(side, 1.0))

    def _extract_execution_penalty(self, execution_quality: Any) -> float:
        if execution_quality is None:
            return 1.0

        if isinstance(execution_quality, dict):
            severity = str(execution_quality.get("severity", "")).upper().strip()
            if severity == "CRITICAL":
                return 0.80
            if severity == "WARNING":
                return 0.92
            if severity == "NORMAL":
                return 1.0

            degradation_score = self._safe_float(execution_quality.get("degradation_score"), 0.0)
            if degradation_score >= 4:
                return 0.80
            if degradation_score >= 2:
                return 0.92

        return 1.0

    def _extract_drawdown_penalty(self, account_context: Optional[Dict[str, Any]]) -> float:
        ctx = account_context or {}
        dd = self._safe_float(ctx.get("drawdown_ratio"), 0.0)

        if dd <= 0.01:
            return 1.00
        if dd <= 0.03:
            return 0.96
        if dd <= 0.05:
            return 0.90
        if dd <= 0.08:
            return 0.82
        return 0.74

    def _quality_adjusted_strength(self, sig: Dict[str, Any]) -> float:
        raw_strength = self._clamp_strength(sig["source"], float(sig.get("strength", 1.0)))
        quality = self._clamp(float(sig.get("quality", 0.6)), 0.35, 1.0)
        adjusted = raw_strength * (0.72 + 0.45 * quality)
        return float(adjusted)

    def _blocked_by_timing(self, symbol: str, side: str, abs_score: float, dominant_quality: float) -> bool:
        now = time.time()
        last_ts = self.last_signal_ts.get(symbol)
        last_side = self.last_signal_side.get(symbol)

        if last_ts is None or last_side is None:
            return False

        elapsed = now - last_ts

        if elapsed < self.signal_cooldown_seconds:
            self._log(
                f"[ALPHA BLOCK] {symbol} cooldown elapsed={elapsed:.3f} < {self.signal_cooldown_seconds:.3f}"
            )
            return True

        same_side_rearm = self.same_side_rearm_seconds
        if dominant_quality >= 0.82:
            same_side_rearm *= 0.72
        elif dominant_quality >= 0.72:
            same_side_rearm *= 0.85

        if side == last_side and elapsed < same_side_rearm:
            self._log(
                f"[ALPHA BLOCK] {symbol} same_side_rearm elapsed={elapsed:.3f} < {same_side_rearm:.3f}"
            )
            return True

        if side != last_side and elapsed < self.flip_block_seconds:
            needed = max(self.buy_threshold, self.sell_threshold) * self.strong_flip_multiplier
            if dominant_quality >= 0.88:
                needed *= 0.96
            if abs_score < needed:
                self._log(
                    f"[ALPHA BLOCK] {symbol} flip_block elapsed={elapsed:.3f} < {self.flip_block_seconds:.3f} "
                    f"abs_score={abs_score:.4f} needed={needed:.4f}"
                )
                return True

        return False

    def _build_component_score(self, sig: Dict[str, Any]) -> float:
        source = sig["source"]
        side = sig["side"]
        adj_strength = self._quality_adjusted_strength(sig)
        weight = self.weights.get(source, 1.0)

        signed = adj_strength * weight
        if side == "SELL":
            signed *= -1.0
        return signed

    def _majority_side(self, components: List[Dict[str, Any]]):
        buy_count = sum(1 for s in components if s["side"] == "BUY")
        sell_count = sum(1 for s in components if s["side"] == "SELL")

        if buy_count > sell_count:
            return "BUY", buy_count, sell_count
        if sell_count > buy_count:
            return "SELL", buy_count, sell_count
        return None, buy_count, sell_count

    def signal(
        self,
        symbol,
        trade,
        regime: Any = None,
        execution_quality: Optional[Dict[str, Any]] = None,
        account_context: Optional[Dict[str, Any]] = None,
    ):
        try:
            trade = trade or {}

            qty = self._safe_float(trade.get("qty", 0.0), 0.0)

            raw_micro = self._safe_call(self.micro.signal, symbol)
            raw_pressure = self._safe_call(self.pressure.signal, symbol)
            raw_velocity = self._call_velocity_signal(symbol, qty, trade)
            raw_volatility = self._safe_call(self.volatility.signal, symbol)

            micro_sig = self._normalize_signal(raw_micro, "micro")
            pressure_sig = self._normalize_signal(raw_pressure, "pressure")
            velocity_sig = self._normalize_signal(raw_velocity, "velocity")

            self._log(
                f"[ALPHA DEBUG] {symbol} qty={qty} "
                f"raw_micro={raw_micro} raw_pressure={raw_pressure} "
                f"raw_velocity={raw_velocity} raw_volatility={raw_volatility}"
            )

            components = [s for s in (micro_sig, pressure_sig, velocity_sig) if s is not None]
            self._log(f"[ALPHA DEBUG] {symbol} components={components}")

            if len(components) < self.min_agree_count:
                self._log(
                    f"[ALPHA SKIP] {symbol} insufficient_components "
                    f"count={len(components)} required={self.min_agree_count}"
                )
                return None

            majority_side, buy_count, sell_count = self._majority_side(components)
            if majority_side is None:
                self._log(
                    f"[ALPHA SKIP] {symbol} tied_components buy_count={buy_count} sell_count={sell_count}"
                )
                return None

            weighted_score = 0.0
            component_details = []

            majority_quality_sum = 0.0
            majority_quality_count = 0

            for sig in components:
                comp_score = self._build_component_score(sig)
                weighted_score += comp_score

                if sig["side"] == majority_side:
                    majority_quality_sum += float(sig.get("quality", 0.6))
                    majority_quality_count += 1

                component_details.append({
                    "source": sig["source"],
                    "side": sig["side"],
                    "strength": round(float(sig.get("strength", 1.0)), 6),
                    "quality": round(float(sig.get("quality", 0.6)), 6),
                    "adjusted_strength": round(self._quality_adjusted_strength(sig), 6),
                    "weight": round(float(self.weights.get(sig["source"], 1.0)), 6),
                    "weighted_score": round(comp_score, 6),
                })

            abs_score = abs(weighted_score)
            dominant_quality = (
                majority_quality_sum / majority_quality_count
                if majority_quality_count > 0 else 0.6
            )

            if weighted_score >= self.buy_threshold:
                final_side = "BUY"
                threshold = self.buy_threshold
            elif weighted_score <= -self.sell_threshold:
                final_side = "SELL"
                threshold = self.sell_threshold
            else:
                self._log(
                    f"[ALPHA SKIP] {symbol} below_threshold "
                    f"weighted_score={weighted_score:.6f} "
                    f"buy_threshold={self.buy_threshold:.6f} sell_threshold={self.sell_threshold:.6f}"
                )
                return None

            if final_side != majority_side:
                self._log(
                    f"[ALPHA SKIP] {symbol} side_mismatch final_side={final_side} majority_side={majority_side}"
                )
                return None

            agree_count = buy_count if final_side == "BUY" else sell_count
            oppose_count = sell_count if final_side == "BUY" else buy_count

            if agree_count < self.min_agree_count:
                self._log(
                    f"[ALPHA SKIP] {symbol} weak_majority agree_count={agree_count} required={self.min_agree_count}"
                )
                return None

            if oppose_count > 0:
                if abs_score < (threshold * 1.01):
                    self._log(
                        f"[ALPHA SKIP] {symbol} mixed_signal_weak_edge "
                        f"abs_score={abs_score:.6f} needed={(threshold * 1.01):.6f}"
                    )
                    return None
                if dominant_quality < 0.54:
                    self._log(
                        f"[ALPHA SKIP] {symbol} mixed_signal_low_quality quality={dominant_quality:.6f}"
                    )
                    return None

            vol_value = self._extract_volatility_value(raw_volatility)
            volatility_filter_blocked = False

            if self.use_volatility_filter and vol_value is not None:
                if vol_value < self.min_volatility or vol_value > self.max_volatility:
                    volatility_filter_blocked = True
                    self._log(
                        f"[ALPHA SKIP] {symbol} volatility_block "
                        f"vol_value={vol_value:.8f} min={self.min_volatility:.8f} max={self.max_volatility:.8f}"
                    )
                    return None

            total_count = max(1, buy_count + sell_count)
            dominance_ratio = agree_count / total_count

            if dominance_ratio < 0.55:
                self._log(
                    f"[ALPHA SKIP] {symbol} low_dominance dominance_ratio={dominance_ratio:.6f}"
                )
                return None

            if dominant_quality < 0.50:
                self._log(
                    f"[ALPHA SKIP] {symbol} low_quality dominant_quality={dominant_quality:.6f}"
                )
                return None

            edge_ratio = abs_score / max(threshold, 1e-9)
            if edge_ratio < 1.0:
                self._log(
                    f"[ALPHA SKIP] {symbol} low_edge edge_ratio={edge_ratio:.6f}"
                )
                return None

            regime_multiplier = self._regime_multiplier(regime, final_side)
            execution_penalty = self._extract_execution_penalty(execution_quality)
            drawdown_penalty = self._extract_drawdown_penalty(account_context)

            adjusted_edge_ratio = edge_ratio * regime_multiplier * execution_penalty * drawdown_penalty
            adjusted_score = abs_score * regime_multiplier * execution_penalty * drawdown_penalty

            if adjusted_edge_ratio < 0.92:
                self._log(
                    f"[ALPHA SKIP] {symbol} adjusted_edge_too_low "
                    f"adjusted_edge_ratio={adjusted_edge_ratio:.6f}"
                )
                return None

            if self._blocked_by_timing(symbol, final_side, adjusted_score, dominant_quality):
                return None

            now = time.time()
            self.last_signal_ts[symbol] = now
            self.last_signal_side[symbol] = final_side

            result = {
                "symbol": symbol,
                "side": final_side,
                "score": round(adjusted_score, 6),
                "raw_score": round(abs_score, 6),
                "weighted_score": round(weighted_score, 6),
                "edge_ratio": round(edge_ratio, 6),
                "adjusted_edge_ratio": round(adjusted_edge_ratio, 6),
                "quality": round(float(dominant_quality), 6),
                "agree_count": int(agree_count),
                "oppose_count": int(oppose_count),
                "buy_count": int(buy_count),
                "sell_count": int(sell_count),
                "dominance_ratio": round(dominance_ratio, 6),
                "components": component_details,
                "volatility_value": vol_value,
                "volatility_filter_blocked": volatility_filter_blocked,
                "regime": self._extract_regime_state(regime),
                "regime_multiplier": round(regime_multiplier, 6),
                "execution_penalty": round(execution_penalty, 6),
                "drawdown_penalty": round(drawdown_penalty, 6),
                "source": "alpha_fusion",
                "ts": now,
            }

            self._log(
                f"[ALPHA FIRE] {symbol} side={final_side} score={adjusted_score:.6f} "
                f"weighted_score={weighted_score:.6f} edge_ratio={edge_ratio:.6f} "
                f"adjusted_edge_ratio={adjusted_edge_ratio:.6f} quality={dominant_quality:.6f} "
                f"agree={agree_count} oppose={oppose_count}"
            )

            return result

        except Exception as e:
            print("AlphaFusion error:", e)
            return None

    def snapshot(self) -> Dict[str, Any]:
        return {
            "buy_threshold": self.buy_threshold,
            "sell_threshold": self.sell_threshold,
            "min_agree_count": self.min_agree_count,
            "signal_cooldown_seconds": self.signal_cooldown_seconds,
            "same_side_rearm_seconds": self.same_side_rearm_seconds,
            "flip_block_seconds": self.flip_block_seconds,
            "strong_flip_multiplier": self.strong_flip_multiplier,
            "use_volatility_filter": self.use_volatility_filter,
            "min_volatility": self.min_volatility,
            "max_volatility": self.max_volatility,
            "weights": dict(self.weights),
            "regime_multipliers": dict(self.regime_multipliers),
        }
