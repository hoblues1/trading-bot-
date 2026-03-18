import copy
import logging
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, Optional

from config import (
    CAPITAL_PER_TRADE,
    MAX_POSITION_SIZE,
    SIZING_MIN_SIGNAL_MULTIPLIER,
    SIZING_MAX_SIGNAL_MULTIPLIER,
    SIZING_MIN_VOLATILITY_FLOOR,
    SIZING_MAX_VOLATILITY_CAP,
    SIZING_MAX_MARGIN_USAGE_RATIO,
    SIZING_FREE_BALANCE_BUFFER_RATIO,
    SIZING_EXCHANGE_MIN_NOTIONAL,
    LEVERAGE,
)


class DynamicSizing:
    """
    Institutional / hedge-fund grade dynamic sizing engine

    설계 목표:
    1) 신호 강도 / 변동성 / 레짐 / 드로우다운 / 체결품질 / confidence를 동시에 반영
    2) account context 기반 equity / free balance / available balance / margin ratio 반영
    3) OPEN / CLOSE / PARTIAL_CLOSE sizing 모두 지원
    4) 심볼별 리스크 캡 / step / min_qty 반영
    5) exchange min notional / free balance buffer / margin usage 방어
    6) current position 기반 symbol capacity 계산
    7) partial close 시 지나치게 작은 찌꺼기 수량 방지
    8) long-term 운영용 debug metadata 제공
    9) meta 응답과 raw size 응답 모두 지원
    10) 잘못된 입력값이나 누락된 market context에도 방어적으로 동작
    11) 장기 운용 중 size drift가 커지지 않도록 precision 정규화
    12) over-sizing 보다 under-sizing 을 우선하는 보수적 구조 유지
    13) router / executor / position / pnl / kill switch 와 쉽게 연결 가능한 구조 유지
    14) 심볼별 risk budget과 global cap을 동시에 적용
    15) leverage 적용 후 실제 required margin 기준으로 재검증
    16) 수년간 튜닝 없이 굴려도 안정적인 sizing layer 유지
    """

    def __init__(self):
        self.base_risk = float(CAPITAL_PER_TRADE)
        self.exchange_min_notional = 2.0

        self.min_signal_multiplier = 0.80
        self.max_signal_multiplier = 1.80

        self.min_volatility_floor = 0.0005
        self.max_volatility_cap = 0.08

        self.max_margin_usage_ratio = 0.85
        self.free_balance_buffer_ratio = 0.05

        self.min_regime_multiplier = 0.68
        self.max_regime_multiplier = 1.22

        self.min_drawdown_multiplier = 0.50
        self.max_drawdown_multiplier = 1.00

        self.min_execution_multiplier = 0.60
        self.max_execution_multiplier = 1.10

        self.min_confidence_multiplier = 0.72
        self.max_confidence_multiplier = 1.28

        self.max_position_value_cap_ratio = 1.00
        self.partial_close_size_floor_ratio = 0.40
        self.min_partial_close_notional_ratio = 0.40

        self.symbol_risk_caps = {
            "BTCUSDT": 1.00,
            "ETHUSDT": 1.00,
            "BNBUSDT": 0.95,
            "SOLUSDT": 0.95,
            "DOGEUSDT": 0.90,
        }

        self.step_map = {
            "BTCUSDT": 0.001,
            "ETHUSDT": 0.001,
            "SOLUSDT": 0.01,
            "BNBUSDT": 0.001,
            "DOGEUSDT": 1.0,
        }

        self.min_qty_map = {
            "BTCUSDT": 0.001,
            "ETHUSDT": 0.01,
            "SOLUSDT": 0.1,
            "BNBUSDT": 0.01,
            "DOGEUSDT": 1.0,
        }

        self.last_debug_payload: Dict[str, Any] = {}

    # ================= INTERNAL =================
    def _precision_floor(self, qty, step):
        step_dec = Decimal(str(step))
        qty_dec = Decimal(str(qty))
        adj = (qty_dec // step_dec) * step_dec
        adj = adj.quantize(step_dec, rounding=ROUND_DOWN)
        return float(adj)

    def _safe_float(self, value, default=0.0):
        try:
            return float(value)
        except Exception:
            return default

    def _safe_int(self, value, default=0):
        try:
            return int(value)
        except Exception:
            return default

    def _clamp(self, value, low, high):
        return max(low, min(value, high))

    def _normalize_symbol(self, symbol: Optional[str]) -> str:
        return str(symbol or "").upper().strip()

    def _normalize_action(self, action: Optional[str]) -> str:
        a = str(action or "OPEN").upper().strip()
        if a in ("OPEN", "CLOSE", "PARTIAL_CLOSE"):
            return a
        return "OPEN"

    def _normalize_side(self, side: Optional[str]) -> Optional[str]:
        if side is None:
            return None
        s = str(side).upper().strip()
        if s in ("BUY", "LONG"):
            return "BUY"
        if s in ("SELL", "SHORT"):
            return "SELL"
        return s

    def _step_for_symbol(self, symbol: str) -> float:
        return float(self.step_map.get(symbol, 0.001))

    def _min_qty_for_symbol(self, symbol: str) -> float:
        step = self._step_for_symbol(symbol)
        return float(self.min_qty_map.get(symbol, step))

    def _extract_volatility_value(self, volatility):
        if volatility is None:
            return 0.0
        if isinstance(volatility, bool):
            return 0.01 if volatility else 0.0
        if isinstance(volatility, (int, float)):
            return float(volatility)
        if isinstance(volatility, dict):
            for key in ("value", "volatility", "range_ratio", "atr_ratio", "current_volatility"):
                if key in volatility:
                    try:
                        return float(volatility[key])
                    except Exception:
                        pass
        return 0.0

    def _signal_multiplier(self, signal_strength):
        s = self._safe_float(signal_strength, 1.0)
        if s <= 0:
            return self.min_signal_multiplier
        if s < 1.0:
            mult = 0.75 + (s * 0.25)
        elif s < 2.0:
            mult = 1.0 + ((s - 1.0) * 0.20)
        elif s < 3.0:
            mult = 1.2 + ((s - 2.0) * 0.10)
        else:
            mult = self.max_signal_multiplier
        return self._clamp(mult, self.min_signal_multiplier, self.max_signal_multiplier)

    def _volatility_multiplier(self, volatility_value):
        v = self._clamp(self._safe_float(volatility_value, 0.0), self.min_volatility_floor, self.max_volatility_cap)
        sweet_spot = 0.008
        if v <= sweet_spot:
            ratio = v / max(sweet_spot, 1e-9)
            return 0.60 + (ratio * 0.40)
        excess = min((v - sweet_spot) / max(self.max_volatility_cap - sweet_spot, 1e-9), 1.0)
        return 1.00 - (excess * 0.45)

    def _symbol_cap_multiplier(self, symbol):
        return float(self.symbol_risk_caps.get(symbol, 0.80))

    def _extract_regime_state(self, regime) -> str:
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

    def _regime_multiplier(self, regime) -> float:
        state = self._extract_regime_state(regime)
        mapping = {
            "TREND_UP": 1.08,
            "TREND_DOWN": 1.08,
            "TREND": 1.05,
            "MOMENTUM": 1.05,
            "BALANCED": 1.00,
            "NEUTRAL": 1.00,
            "MEAN_REVERSION": 0.90,
            "RANGE": 0.88,
            "CHOPPY": 0.72,
            "NOISE": 0.68,
            "PANIC": 0.58,
            "HIGH_VOL": 0.70,
            "LOW_LIQUIDITY": 0.62,
            "ILLIQUID": 0.60,
            "RISK_OFF": 0.55,
            "DEFENSIVE": 0.60,
            "UNKNOWN": 0.92,
        }
        mult = mapping.get(state, 0.92)
        return self._clamp(mult, self.min_regime_multiplier, self.max_regime_multiplier)

    def _drawdown_multiplier(self, drawdown_ratio: float) -> float:
        dd = max(0.0, self._safe_float(drawdown_ratio, 0.0))
        if dd <= 0.01:
            mult = 1.00
        elif dd <= 0.03:
            mult = 0.90
        elif dd <= 0.05:
            mult = 0.78
        elif dd <= 0.08:
            mult = 0.62
        else:
            mult = 0.45
        return self._clamp(mult, self.min_drawdown_multiplier, self.max_drawdown_multiplier)

    def _execution_multiplier(self, execution_quality: Any) -> float:
        if execution_quality is None:
            return 1.00

        if isinstance(execution_quality, (int, float)):
            score = self._safe_float(execution_quality, 1.0)
            score = self._clamp(score, 0.0, 1.5)
            if score >= 1.0:
                mult = 0.95 + min(score - 1.0, 0.1)
            else:
                mult = 0.45 + (score * 0.55)
            return self._clamp(mult, self.min_execution_multiplier, self.max_execution_multiplier)

        if isinstance(execution_quality, dict):
            severity = str(execution_quality.get("severity", "")).upper().strip()
            if severity == "CRITICAL":
                return 0.45
            if severity == "WARNING":
                return 0.72
            if severity == "NORMAL":
                return 1.00

            degradation_score = self._safe_float(execution_quality.get("degradation_score"), 0.0)
            if degradation_score >= 4:
                return 0.45
            if degradation_score >= 2:
                return 0.72

            snapshot = execution_quality.get("snapshot", {})
            if isinstance(snapshot, dict):
                avg_fill_ratio = self._safe_float(snapshot.get("avg_fill_ratio"), 1.0)
                avg_slippage_bps = self._safe_float(snapshot.get("avg_slippage_bps"), 0.0)
                avg_completion_ms = self._safe_float(snapshot.get("avg_completion_ms"), 0.0)
                mult = 1.00
                if avg_fill_ratio < 0.70:
                    mult *= 0.72
                elif avg_fill_ratio < 0.85:
                    mult *= 0.86
                if avg_slippage_bps > 10.0:
                    mult *= 0.72
                elif avg_slippage_bps > 6.0:
                    mult *= 0.86
                if avg_completion_ms > 3000:
                    mult *= 0.78
                elif avg_completion_ms > 1800:
                    mult *= 0.90
                return self._clamp(mult, self.min_execution_multiplier, self.max_execution_multiplier)

        return 1.00

    def _confidence_multiplier(self, confidence: float) -> float:
        c = self._clamp(self._safe_float(confidence, 0.0), 0.0, 1.0)
        mult = 0.60 + (c * 0.60)
        return self._clamp(mult, self.min_confidence_multiplier, self.max_confidence_multiplier)

    def _extract_balance_context(self, balance: float, account_context: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
        ctx = account_context or {}

        equity = self._safe_float(ctx.get("equity"), balance)
        wallet_balance = self._safe_float(ctx.get("wallet_balance"), balance)
        free_balance = self._safe_float(ctx.get("free_balance"), balance)
        available_balance = self._safe_float(ctx.get("available_balance"), free_balance)
        used_margin = self._safe_float(ctx.get("used_margin"), 0.0)
        margin_ratio = self._safe_float(ctx.get("margin_ratio"), 0.0)
        drawdown_ratio = self._safe_float(ctx.get("drawdown_ratio"), 0.0)

        if equity <= 0:
            equity = balance
        if wallet_balance <= 0:
            wallet_balance = balance
        if free_balance <= 0:
            free_balance = balance
        if available_balance <= 0:
            available_balance = free_balance
        if margin_ratio <= 0 and equity > 0:
            margin_ratio = used_margin / equity if used_margin > 0 else 0.0

        return {
            "equity": equity,
            "wallet_balance": wallet_balance,
            "free_balance": free_balance,
            "available_balance": available_balance,
            "used_margin": used_margin,
            "margin_ratio": margin_ratio,
            "drawdown_ratio": drawdown_ratio,
        }

    def _extract_position_size(self, current_position: Any) -> float:
        if current_position is None:
            return 0.0
        if isinstance(current_position, (int, float)):
            return abs(self._safe_float(current_position, 0.0))
        if isinstance(current_position, dict):
            for key in ("size", "qty", "position_size", "amount", "positionAmt"):
                if key in current_position:
                    qty = abs(self._safe_float(current_position.get(key), 0.0))
                    if qty > 0:
                        return qty
        return 0.0

    def _extract_price(self, price: Any, market_context: Optional[Dict[str, Any]] = None) -> float:
        p = self._safe_float(price, 0.0)
        if p > 0:
            return p
        ctx = market_context or {}
        for key in ("mid_price", "mark_price", "last_price", "ask_price", "bid_price"):
            p = self._safe_float(ctx.get(key), 0.0)
            if p > 0:
                return p
        return 0.0

    def _extract_requested_close_qty(self, current_position: Optional[Dict[str, Any]]) -> float:
        if not isinstance(current_position, dict):
            return 0.0
        for key in ("close_qty", "requested_qty", "target_qty", "qty"):
            val = self._safe_float(current_position.get(key), 0.0)
            if val > 0:
                return val
        return 0.0

    def _apply_precision_and_validate(self, symbol: str, raw_size: float, price: float) -> float:
        step = self._step_for_symbol(symbol)
        min_qty = self._min_qty_for_symbol(symbol)
        size = self._precision_floor(max(0.0, raw_size), step)
        if size < min_qty:
            return 0.0
        if price > 0 and (size * price) < self.exchange_min_notional:
            return 0.0
        return size

    def _build_sizing_debug_payload(
        self,
        symbol: str,
        equity: float,
        available_balance: float,
        price: float,
        risk_capital: float,
        signal_mult: float,
        vol_value: float,
        vol_mult: float,
        regime_mult: float,
        drawdown_mult: float,
        execution_mult: float,
        confidence_mult: float,
        position_value: float,
        leveraged_value: float,
        raw_size: float,
        final_size: float,
        current_position_size: float,
        required_margin: float,
    ) -> Dict[str, Any]:
        return {
            "symbol": symbol,
            "equity": equity,
            "available_balance": available_balance,
            "price": price,
            "risk_capital": risk_capital,
            "signal_mult": signal_mult,
            "vol_value": vol_value,
            "vol_mult": vol_mult,
            "regime_mult": regime_mult,
            "drawdown_mult": drawdown_mult,
            "execution_mult": execution_mult,
            "confidence_mult": confidence_mult,
            "position_value": position_value,
            "leveraged_value": leveraged_value,
            "raw_size": raw_size,
            "final_size": final_size,
            "current_position_size": current_position_size,
            "required_margin": required_margin,
        }

    # ================= PUBLIC =================
    def size(
        self,
        symbol,
        balance,
        price,
        volatility,
        signal_strength,
        regime=None,
        account_context: Optional[Dict[str, Any]] = None,
        execution_quality: Optional[Dict[str, Any]] = None,
        confidence: Optional[float] = None,
        current_position: Optional[Dict[str, Any]] = None,
        action: str = "OPEN",
        side: Optional[str] = None,
        market_context: Optional[Dict[str, Any]] = None,
        signal_meta: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        signal_meta = signal_meta or {}
        _ = kwargs

    # 기존 로직 그대로
        try:
            symbol = self._normalize_symbol(symbol)
            action = self._normalize_action(action)
            side = self._normalize_side(side)

            balance = self._safe_float(balance, 0.0)
            price = self._extract_price(price, market_context)

            if price <= 0 or balance <= 0:
                logging.warning(f"[SIZING_SKIP] invalid input | symbol={symbol} balance={balance} price={price}")
                return 0.0

            balance_ctx = self._extract_balance_context(balance, account_context)
            equity = self._safe_float(balance_ctx.get("equity"), balance)
            available_balance = self._safe_float(balance_ctx.get("available_balance"), balance)
            free_balance = self._safe_float(balance_ctx.get("free_balance"), balance)
            drawdown_ratio = self._safe_float(balance_ctx.get("drawdown_ratio"), 0.0)
            margin_ratio = self._safe_float(balance_ctx.get("margin_ratio"), 0.0)

            current_position_size = self._extract_position_size(current_position)

            # ===== CLOSE / PARTIAL_CLOSE =====
            if action in ("CLOSE", "PARTIAL_CLOSE"):
                requested_qty = self._extract_requested_close_qty(current_position)

                if action == "CLOSE":
                    raw_size = current_position_size if current_position_size > 0 else requested_qty
                else:
                    if requested_qty > 0:
                        raw_size = requested_qty
                    else:
                        raw_size = current_position_size * self.partial_close_size_floor_ratio

                step = self._step_for_symbol(symbol)
                min_qty = self._min_qty_for_symbol(symbol)
                size = self._precision_floor(max(0.0, raw_size), step)

                if current_position_size > 0:
                    size = min(size, current_position_size)
                    size = self._precision_floor(size, step)

                if size < min_qty:
                    return 0.0

                final_notional = size * price
                if size <= 0 or final_notional < self.exchange_min_notional * (self.min_partial_close_notional_ratio if action == "PARTIAL_CLOSE" else 1.0):
                    return 0.0
                return size

            # ===== OPEN =====
            risk_capital = equity * self.base_risk
            if risk_capital <= 0:
                logging.warning(f"[SIZING_SKIP] risk_capital<=0 | symbol={symbol} risk_capital={risk_capital}")
                return 0.0

            signal_mult = self._signal_multiplier(signal_strength)
            vol_value = self._extract_volatility_value(volatility)
            vol_mult = self._volatility_multiplier(vol_value)
            regime_mult = self._regime_multiplier(regime)
            drawdown_mult = self._drawdown_multiplier(drawdown_ratio)
            execution_mult = self._execution_multiplier(execution_quality)
            confidence_mult = self._confidence_multiplier(self._safe_float(confidence, 1.0 if confidence is None else confidence))

            position_value = (
                risk_capital
                * signal_mult
                * vol_mult
                * regime_mult
                * drawdown_mult
                * execution_mult
                * confidence_mult
            )

            # 최소 진입 노셔널 보정
            dynamic_min_notional = max(
                self.exchange_min_notional,
                equity * 0.08
            )
            if position_value < dynamic_min_notional:
                position_value = dynamic_min_notional

            max_value_global = equity * float(MAX_POSITION_SIZE) * self.max_position_value_cap_ratio
            max_value_symbol = max_value_global * self._symbol_cap_multiplier(symbol)

            current_position_value = current_position_size * price
            remaining_symbol_capacity = max(0.0, max_value_symbol - current_position_value)
            position_value = min(position_value, remaining_symbol_capacity)

            # margin ratio가 이미 높으면 공격적 증액 차단
            if margin_ratio >= self.max_margin_usage_ratio:
                logging.warning(
                    f"[SIZING_SKIP] margin ratio exceeded | symbol={symbol} margin_ratio={margin_ratio:.4f} max={self.max_margin_usage_ratio:.4f}"
                )
                return 0.0

            leveraged_value = position_value * float(LEVERAGE)
            required_margin = leveraged_value / max(float(LEVERAGE), 1e-9)

            if required_margin > equity * self.max_margin_usage_ratio:
                logging.warning(
                    f"[SIZING_SKIP] margin too high | symbol={symbol} required_margin={required_margin:.4f} equity={equity:.4f}"
                )
                return 0.0

            # available balance보다 약간 공격적으로 허용
            if required_margin > (available_balance * 0.98):
                logging.warning(
                    f"[SIZING_SKIP] available balance insufficient | symbol={symbol} required_margin={required_margin:.4f} available_balance={available_balance:.4f}"
                )
                return 0.0

            if (free_balance - required_margin) < (equity * self.free_balance_buffer_ratio):
                logging.warning(
                    f"[SIZING_SKIP] free balance buffer violated | symbol={symbol} required_margin={required_margin:.4f} "
                    f"free_balance={free_balance:.4f} equity={equity:.4f}"
                )
                return 0.0

            raw_size = leveraged_value / price
            step = self._step_for_symbol(symbol)
            min_qty = self._min_qty_for_symbol(symbol)

            if raw_size < min_qty:
                raw_size = min_qty

            size = self._precision_floor(raw_size, step)

            if size < min_qty:
                size = min_qty
                size = self._precision_floor(size, step)

            if size < min_qty:
                logging.warning(
                    f"[SIZING_SKIP] size below min_qty | symbol={symbol} raw_size={raw_size:.6f} adjusted={size:.6f} min_qty={min_qty}"
                )
                return 0.0

            final_notional = size * price
            if final_notional < self.exchange_min_notional:
                logging.warning(
                    f"[SIZING_SKIP] final_notional too low | symbol={symbol} size={size:.6f} price={price:.6f} final_notional={final_notional:.6f}"
                )
                return 0.0

            max_final_size = self._precision_floor(max(0.0, (remaining_symbol_capacity * float(LEVERAGE)) / price), step)
            if max_final_size > 0:
                size = min(size, max_final_size)
                size = self._precision_floor(size, step)

            if size <= 0:
                logging.warning(f"[SIZING_SKIP] final size <= 0 | symbol={symbol} size={size}")
                return 0.0

            debug_payload = self._build_sizing_debug_payload(
                symbol=symbol,
                equity=equity,
                available_balance=available_balance,
                price=price,
                risk_capital=risk_capital,
                signal_mult=signal_mult,
                vol_value=vol_value,
                vol_mult=vol_mult,
                regime_mult=regime_mult,
                drawdown_mult=drawdown_mult,
                execution_mult=execution_mult,
                confidence_mult=confidence_mult,
                position_value=position_value,
                leveraged_value=leveraged_value,
                raw_size=raw_size,
                final_size=size,
                current_position_size=current_position_size,
                required_margin=required_margin,
            )
            self.last_debug_payload[symbol] = debug_payload

            logging.warning(
                f"[SIZING_RESULT] symbol={debug_payload['symbol']} equity={debug_payload['equity']:.4f} "
                f"available_balance={debug_payload['available_balance']:.4f} price={debug_payload['price']:.6f} "
                f"risk_capital={debug_payload['risk_capital']:.4f} signal_mult={debug_payload['signal_mult']:.4f} "
                f"vol_value={debug_payload['vol_value']:.6f} vol_mult={debug_payload['vol_mult']:.4f} "
                f"regime_mult={debug_payload['regime_mult']:.4f} drawdown_mult={debug_payload['drawdown_mult']:.4f} "
                f"execution_mult={debug_payload['execution_mult']:.4f} confidence_mult={debug_payload['confidence_mult']:.4f} "
                f"position_value={debug_payload['position_value']:.4f} leveraged_value={debug_payload['leveraged_value']:.4f} "
                f"raw_size={debug_payload['raw_size']:.6f} final_size={debug_payload['final_size']:.6f} "
                f"current_position_size={debug_payload['current_position_size']:.6f} required_margin={debug_payload['required_margin']:.6f}"
            )

            return size

        except Exception as e:
            logging.error(f"Dynamic sizing error: {e}")
            return 0.0

    def size_with_meta(
        self,
        symbol,
        balance,
        price,
        volatility,
        signal_strength,
        regime=None,
        account_context: Optional[Dict[str, Any]] = None,
        execution_quality: Optional[Dict[str, Any]] = None,
        confidence: Optional[float] = None,
        current_position: Optional[Dict[str, Any]] = None,
        action: str = "OPEN",
        side: Optional[str] = None,
        market_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        try:
            size = self.size(
                symbol=symbol,
                balance=balance,
                price=price,
                volatility=volatility,
                signal_strength=signal_strength,
                regime=regime,
                account_context=account_context,
                execution_quality=execution_quality,
                confidence=confidence,
                current_position=current_position,
                action=action,
                side=side,
                market_context=market_context,
            )

            symbol = self._normalize_symbol(symbol)
            action = self._normalize_action(action)
            side = self._normalize_side(side)
            px = self._extract_price(price, market_context)
            notional = size * px if size > 0 and px > 0 else 0.0

            return {
                "symbol": symbol,
                "action": action,
                "side": side,
                "size": size,
                "qty": size,
                "price": px,
                "notional": notional,
                "leverage": float(LEVERAGE),
                "valid": size > 0,
                "debug": copy.deepcopy(self.last_debug_payload.get(symbol, {})),
            }

        except Exception as e:
            logging.error(f"Dynamic sizing meta error: {e}")
            return {
                "symbol": self._normalize_symbol(symbol),
                "action": self._normalize_action(action),
                "side": self._normalize_side(side),
                "size": 0.0,
                "qty": 0.0,
                "price": self._safe_float(price, 0.0),
                "notional": 0.0,
                "leverage": float(LEVERAGE),
                "valid": False,
                "reason": str(e),
            }

    def snapshot(self) -> Dict[str, Any]:
        return {
            "base_risk": self.base_risk,
            "exchange_min_notional": self.exchange_min_notional,
            "min_signal_multiplier": self.min_signal_multiplier,
            "max_signal_multiplier": self.max_signal_multiplier,
            "min_volatility_floor": self.min_volatility_floor,
            "max_volatility_cap": self.max_volatility_cap,
            "max_margin_usage_ratio": self.max_margin_usage_ratio,
            "free_balance_buffer_ratio": self.free_balance_buffer_ratio,
            "min_regime_multiplier": self.min_regime_multiplier,
            "max_regime_multiplier": self.max_regime_multiplier,
            "min_drawdown_multiplier": self.min_drawdown_multiplier,
            "max_drawdown_multiplier": self.max_drawdown_multiplier,
            "min_execution_multiplier": self.min_execution_multiplier,
            "max_execution_multiplier": self.max_execution_multiplier,
            "min_confidence_multiplier": self.min_confidence_multiplier,
            "max_confidence_multiplier": self.max_confidence_multiplier,
            "max_position_value_cap_ratio": self.max_position_value_cap_ratio,
            "partial_close_size_floor_ratio": self.partial_close_size_floor_ratio,
            "min_partial_close_notional_ratio": self.min_partial_close_notional_ratio,
            "step_map": copy.deepcopy(self.step_map),
            "min_qty_map": copy.deepcopy(self.min_qty_map),
            "symbol_risk_caps": copy.deepcopy(self.symbol_risk_caps),
            "last_debug_payload": copy.deepcopy(self.last_debug_payload),
        }
