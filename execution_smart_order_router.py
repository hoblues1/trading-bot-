import copy
import logging
import threading
import time
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Optional, Dict, Any, List

from config import (
    ORDER_TYPE,
    SLIPPAGE_LIMIT,
    MAX_ORDER_RETRY,
    ALLOW_PARTIAL_FILL,
)

try:
    from execution_quality_monitor import ExecutionQualityMonitor
except Exception:
    ExecutionQualityMonitor = None


class SmartOrderRouter:
    """
    High-end smart order router

    핵심 보강:
    1) OPEN / CLOSE / PARTIAL_CLOSE 모두 표준 payload 생성
    2) throttle / same-side cooldown / duplicate order 차단
    3) qty / price precision 안정화
    4) spread / depth / volatility / execution quality 기반 order type 선택
    5) split order 지원
    6) retry 응답 표준화
    7) 성공 시에만 commit
    8) accepted/new 상태는 진짜 성공과 구분해서 처리
    9) duplicate pre-register 후 실패 시 rollback
    10) years-long 운용용 snapshot / metrics 유지
    11) capital_mode 기반으로 초소액 / 방어 / 성장 모드별 routing 보정
    12) 너무 과보수적으로 막아 거래가 죽지 않게 실전형 밸런스 유지
    """

    SUCCESS_STATUSES = {
        "FILLED",
        "PARTIALLY_FILLED",
        "SUCCESS",
        "EXECUTED",
    }

    ACCEPTED_STATUSES = {
        "NEW",
        "ACCEPTED",
    }

    FINAL_FAILURE_STATUSES = {
        "FAILED",
        "REJECTED",
        "CANCELED",
        "CANCELLED",
        "EXPIRED",
        "ERROR",
    }

    BYPASS_REASON_KEYWORDS = ("force", "manual", "override", "rebalance", "emergency")

    def __init__(self):
        self.default_type = str(ORDER_TYPE).upper().strip()
        if self.default_type not in ("MARKET", "LIMIT", "AUTO"):
            self.default_type = "AUTO"

        self.lock = threading.RLock()

        self.last_order_time: Dict[str, float] = {}
        self.last_order_side: Dict[str, str] = {}
        self.last_order_fingerprint: Dict[str, str] = {}
        self.last_route_time: Dict[str, float] = {}

        self.min_order_interval = 1.20
        self.same_side_cooldown = 3.20
        self.duplicate_order_cooldown = 1.80

        self.min_open_volatility = 0.00020
        self.market_order_volatility_threshold = 0.010

        self.max_spread_bps_for_market_open = 8.5
        self.max_spread_bps_for_limit_open = 22.0
        self.max_spread_bps_for_market_close = 12.0
        self.hard_block_spread_bps_for_open = 28.0

        self.min_top_book_notional_ratio = 0.28
        self.min_second_book_notional_ratio = 0.60
        self.max_estimated_slippage_bps = 12.0

        self.default_split_count = 1
        self.max_split_count = 3

        self.default_time_in_force = "GTC"
        self.aggressive_limit_time_in_force = "GTC"
        self.low_urgency_split_sleep = 0.35
        self.normal_urgency_split_sleep = 0.15

        self.route_attempts = 0
        self.route_rejections = 0
        self.route_successes = 0
        self.retry_successes = 0
        self.retry_failures = 0
        self.split_successes = 0
        self.split_partial_successes = 0
        self.updated_at = time.time()

        self.execution_quality_monitor = None
        if ExecutionQualityMonitor is not None:
            try:
                self.execution_quality_monitor = ExecutionQualityMonitor(
                    rolling_window=200,
                    persist_path=None,
                )
                logging.info("ExecutionQualityMonitor attached to SmartOrderRouter")
            except Exception as e:
                logging.warning(f"ExecutionQualityMonitor init failed: {e}")
                self.execution_quality_monitor = None

        self.qty_step_map = {
            "BTCUSDT": 0.001,
            "ETHUSDT": 0.01,
            "SOLUSDT": 0.1,
            "DOGEUSDT": 1.0,
            "BNBUSDT": 0.01,
        }
        self.min_qty_map = {
            "BTCUSDT": 0.001,
            "ETHUSDT": 0.01,
            "SOLUSDT": 0.1,
            "DOGEUSDT": 1.0,
            "BNBUSDT": 0.01,
        }
        self.price_tick_map = {
            "BTCUSDT": 0.10,
            "ETHUSDT": 0.01,
            "SOLUSDT": 0.01,
            "DOGEUSDT": 0.00001,
            "BNBUSDT": 0.01,
        }

    # ================= INTERNAL =================

    def _touch(self):
        self.updated_at = self._now()

    def _safe_float(self, value, default=0.0) -> float:
        try:
            if value is None:
                return float(default)
            return float(value)
        except Exception:
            return float(default)

    def _safe_int(self, value, default=0) -> int:
        try:
            if value is None:
                return int(default)
            return int(value)
        except Exception:
            return int(default)

    def _now(self) -> float:
        return time.time()

    def _floor_to_step(self, value, step):
        try:
            value_dec = Decimal(str(value))
            step_dec = Decimal(str(step))
            adjusted = (value_dec // step_dec) * step_dec
            return float(adjusted.quantize(step_dec, rounding=ROUND_DOWN))
        except Exception as e:
            logging.error(f"Floor step error: {e}")
            return self._safe_float(value, 0.0)

    def _ceil_to_step(self, value, step):
        try:
            value_dec = Decimal(str(value))
            step_dec = Decimal(str(step))
            adjusted = ((value_dec / step_dec).to_integral_value(rounding=ROUND_UP)) * step_dec
            return float(adjusted.quantize(step_dec, rounding=ROUND_UP))
        except Exception as e:
            logging.error(f"Ceil step error: {e}")
            return self._safe_float(value, 0.0)

    def _normalize_symbol(self, symbol: str) -> str:
        return str(symbol).upper().strip()

    def _normalize_side(self, side: Optional[str]) -> Optional[str]:
        if side is None:
            return None
        s = str(side).upper().strip()
        if s in ("BUY", "LONG"):
            return "BUY"
        if s in ("SELL", "SHORT"):
            return "SELL"
        return None

    def _normalize_action(self, action: Optional[str]) -> str:
        if not action:
            return "OPEN"
        a = str(action).upper().strip()
        if a in ("OPEN", "CLOSE", "PARTIAL_CLOSE"):
            return a
        return "OPEN"

    def _normalize_urgency(self, urgency: Optional[str]) -> str:
        u = str(urgency or "").upper().strip()
        if u in ("LOW", "NORMAL", "HIGH", "URGENT"):
            return u
        return "NORMAL"

    def _normalize_aggressiveness(self, value: Optional[str]) -> str:
        v = str(value or "").upper().strip()
        if v in ("PASSIVE", "BALANCED", "AGGRESSIVE"):
            return v
        return "BALANCED"

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

    def _extract_book_context(self, market_context: Optional[Dict[str, Any]]) -> Dict[str, float]:
        ctx = market_context or {}

        bid_price = self._safe_float(ctx.get("bid_price"), 0.0)
        ask_price = self._safe_float(ctx.get("ask_price"), 0.0)
        bid_size = self._safe_float(ctx.get("bid_size"), 0.0)
        ask_size = self._safe_float(ctx.get("ask_size"), 0.0)

        bid_notional = self._safe_float(ctx.get("bid_notional"), bid_price * bid_size)
        ask_notional = self._safe_float(ctx.get("ask_notional"), ask_price * ask_size)
        second_bid_notional = self._safe_float(ctx.get("second_bid_notional"), bid_notional)
        second_ask_notional = self._safe_float(ctx.get("second_ask_notional"), ask_notional)

        mid_price = self._safe_float(ctx.get("mid_price"), 0.0)
        if mid_price <= 0 and bid_price > 0 and ask_price > 0:
            mid_price = (bid_price + ask_price) / 2.0

        spread_bps = self._safe_float(ctx.get("spread_bps"), 0.0)
        if spread_bps <= 0 and bid_price > 0 and ask_price > 0 and mid_price > 0:
            spread_bps = ((ask_price - bid_price) / mid_price) * 10000.0

        book_pressure = self._safe_float(ctx.get("book_pressure"), 0.0)

        return {
            "bid_price": bid_price,
            "ask_price": ask_price,
            "bid_size": bid_size,
            "ask_size": ask_size,
            "bid_notional": bid_notional,
            "ask_notional": ask_notional,
            "second_bid_notional": second_bid_notional,
            "second_ask_notional": second_ask_notional,
            "spread_bps": spread_bps,
            "mid_price": mid_price,
            "book_pressure": book_pressure,
        }

    def _extract_capital_mode(self, extra_meta: Optional[Dict[str, Any]]) -> str:
        meta = extra_meta or {}
        return str(meta.get("capital_mode", "UNKNOWN")).upper().strip()

    def _capital_mode_split_cap(self, capital_mode: str) -> int:
        if capital_mode == "SURVIVAL":
            return 1
        if capital_mode in ("DEFENSIVE", "CAPITAL_PRESERVATION"):
            return 1
        if capital_mode == "MICRO_COMPOUND":
            return 2
        if capital_mode == "ADAPTIVE_GROWTH":
            return 3
        return self.max_split_count

    def _capital_mode_market_open_spread_cap(self, capital_mode: str) -> float:
        if capital_mode == "SURVIVAL":
            return min(self.max_spread_bps_for_market_open, 5.5)
        if capital_mode in ("DEFENSIVE", "CAPITAL_PRESERVATION"):
            return min(self.max_spread_bps_for_market_open, 6.0)
        if capital_mode == "MICRO_COMPOUND":
            return min(self.max_spread_bps_for_market_open, 7.0)
        return self.max_spread_bps_for_market_open

    def _capital_mode_limit_open_spread_cap(self, capital_mode: str) -> float:
        if capital_mode == "SURVIVAL":
            return min(self.max_spread_bps_for_limit_open, 15.0)
        if capital_mode in ("DEFENSIVE", "CAPITAL_PRESERVATION"):
            return min(self.max_spread_bps_for_limit_open, 16.0)
        if capital_mode == "MICRO_COMPOUND":
            return min(self.max_spread_bps_for_limit_open, 18.0)
        return self.max_spread_bps_for_limit_open

    def _capital_mode_hard_spread_cap(self, capital_mode: str) -> float:
        if capital_mode == "SURVIVAL":
            return min(self.hard_block_spread_bps_for_open, 20.0)
        if capital_mode in ("DEFENSIVE", "CAPITAL_PRESERVATION"):
            return min(self.hard_block_spread_bps_for_open, 22.0)
        if capital_mode == "MICRO_COMPOUND":
            return min(self.hard_block_spread_bps_for_open, 24.0)
        return self.hard_block_spread_bps_for_open

    def _capital_mode_estimated_slippage_cap(self, capital_mode: str) -> float:
        if capital_mode == "SURVIVAL":
            return min(self.max_estimated_slippage_bps, 7.0)
        if capital_mode in ("DEFENSIVE", "CAPITAL_PRESERVATION"):
            return min(self.max_estimated_slippage_bps, 8.0)
        if capital_mode == "MICRO_COMPOUND":
            return min(self.max_estimated_slippage_bps, 9.0)
        return self.max_estimated_slippage_bps

    def _normalize_qty(self, symbol: str, qty) -> float:
        try:
            raw_qty = self._safe_float(qty, 0.0)
            if raw_qty <= 0:
                return 0.0
            step = self.qty_step_map.get(symbol, 0.001)
            min_qty = self.min_qty_map.get(symbol, step)
            adjusted = self._floor_to_step(raw_qty, step)
            if adjusted < min_qty:
                logging.warning(
                    f"Adjusted qty invalid | symbol={symbol} raw_qty={raw_qty} adjusted={adjusted} min_qty={min_qty}"
                )
                return 0.0
            return adjusted
        except Exception as e:
            logging.error(f"Qty normalize error: {e}")
            return 0.0

    def _normalize_price(self, symbol: str, price, side: Optional[str]) -> float:
        try:
            tick = self.price_tick_map.get(symbol, 0.01)
            safe_price = self._safe_float(price, 0.0)
            if safe_price <= 0:
                return 0.0
            if side == "BUY":
                return self._ceil_to_step(safe_price, tick)
            if side == "SELL":
                return self._floor_to_step(safe_price, tick)
            return self._floor_to_step(safe_price, tick)
        except Exception as e:
            logging.error(f"Price normalize error: {e}")
            return self._safe_float(price, 0.0)

    def _build_limit_price(self, side: Optional[str], ref_price: float, aggressiveness: str = "BALANCED") -> float:
        try:
            ref_price = self._safe_float(ref_price, 0.0)
            if ref_price <= 0:
                raise ValueError("Invalid reference price")

            slip = abs(self._safe_float(SLIPPAGE_LIMIT, 0.0))
            aggressiveness = self._normalize_aggressiveness(aggressiveness)
            if aggressiveness == "PASSIVE":
                slip *= 0.55
            elif aggressiveness == "AGGRESSIVE":
                slip *= 1.15

            if side == "BUY":
                return ref_price * (1 + slip)
            if side == "SELL":
                return ref_price * (1 - slip)
            raise ValueError(f"Invalid side: {side}")
        except Exception as e:
            logging.error(f"Limit price build error: {e}")
            return self._safe_float(ref_price, 0.0)

    def _estimate_order_notional(self, qty: float, ref_price: float) -> float:
        qty = self._safe_float(qty, 0.0)
        ref_price = self._safe_float(ref_price, 0.0)
        if qty <= 0 or ref_price <= 0:
            return 0.0
        return qty * ref_price

    def _estimate_fill_pressure_ratio(self, side: Optional[str], order_notional: float, book_ctx: Dict[str, float]) -> float:
        if order_notional <= 0:
            return 999.0
        if side == "BUY":
            top = self._safe_float(book_ctx.get("ask_notional"), 0.0)
            second = self._safe_float(book_ctx.get("second_ask_notional"), top)
        else:
            top = self._safe_float(book_ctx.get("bid_notional"), 0.0)
            second = self._safe_float(book_ctx.get("second_bid_notional"), top)
        effective = max(top, 0.0) + max(second, 0.0)
        if effective <= 0:
            return 0.0
        return effective / order_notional

    def _estimate_top_only_ratio(self, side: Optional[str], order_notional: float, book_ctx: Dict[str, float]) -> float:
        if order_notional <= 0:
            return 999.0
        if side == "BUY":
            top = self._safe_float(book_ctx.get("ask_notional"), 0.0)
        else:
            top = self._safe_float(book_ctx.get("bid_notional"), 0.0)
        if top <= 0:
            return 0.0
        return top / order_notional

    def _estimate_slippage_bps(self, spread_bps: float, top_ratio: float, depth_ratio: float, order_type: str) -> float:
        spread_bps = max(0.0, self._safe_float(spread_bps, 0.0))
        top_ratio = max(0.0, self._safe_float(top_ratio, 0.0))
        depth_ratio = max(0.0, self._safe_float(depth_ratio, 0.0))

        if order_type == "MARKET":
            base = spread_bps * 0.75
            if top_ratio < 0.3:
                base += 6.0
            elif top_ratio < 0.6:
                base += 3.0
            if depth_ratio < 0.7:
                base += 3.0
            elif depth_ratio < 1.2:
                base += 1.2
            return base

        base = spread_bps * 0.25
        if top_ratio < 0.3:
            base += 2.0
        elif top_ratio < 0.6:
            base += 1.0
        if depth_ratio < 0.7:
            base += 1.5
        return base

    def _recommend_style_from_quality_monitor(
        self,
        symbol: str,
        spread_bps: float,
        volatility_value: float,
        book_pressure: float,
    ) -> Optional[Dict[str, Any]]:
        try:
            if self.execution_quality_monitor is None:
                return None
            style = self.execution_quality_monitor.recommend_execution_style(
                symbol=symbol,
                current_spread_bps=spread_bps,
                current_volatility_score=min(1.0, volatility_value * 100.0),
                current_book_pressure=book_pressure,
            )
            return style if isinstance(style, dict) else None
        except Exception as e:
            logging.warning(f"Execution quality style recommendation failed: {e}")
            return None

    def _decide_aggressiveness(
        self,
        side: Optional[str],
        action: str,
        volatility_value: float,
        book_ctx: Dict[str, float],
        quality_style: Optional[Dict[str, Any]] = None,
        capital_mode: str = "UNKNOWN",
    ) -> str:
        if quality_style and isinstance(quality_style, dict):
            qs = quality_style.get("aggressiveness")
            if qs:
                aggr = self._normalize_aggressiveness(qs)
                if capital_mode in ("SURVIVAL", "DEFENSIVE", "CAPITAL_PRESERVATION") and aggr == "AGGRESSIVE":
                    return "BALANCED"
                return aggr

        if action != "OPEN":
            return "AGGRESSIVE"

        spread_bps = self._safe_float(book_ctx.get("spread_bps"), 0.0)
        book_pressure = self._safe_float(book_ctx.get("book_pressure"), 0.0)

        if capital_mode in ("SURVIVAL", "DEFENSIVE", "CAPITAL_PRESERVATION"):
            if spread_bps >= 3.5:
                return "PASSIVE"
            if volatility_value >= self.market_order_volatility_threshold * 1.60:
                return "BALANCED"
            return "BALANCED"

        if spread_bps >= self.max_spread_bps_for_market_open:
            return "PASSIVE"

        if volatility_value >= self.market_order_volatility_threshold * 1.35:
            if side == "BUY" and book_pressure > 0.6:
                return "AGGRESSIVE"
            if side == "SELL" and book_pressure < -0.6:
                return "AGGRESSIVE"

        return "BALANCED"

    def _decide_split_count(
        self,
        action: str,
        order_notional: float,
        top_ratio: float,
        depth_ratio: float,
        spread_bps: float,
        quality_style: Optional[Dict[str, Any]] = None,
        capital_mode: str = "UNKNOWN",
    ) -> int:
        split_count = self.default_split_count
        if action != "OPEN":
            return 1

        if capital_mode in ("SURVIVAL", "DEFENSIVE", "CAPITAL_PRESERVATION"):
            return 1

        if order_notional > 0:
            if top_ratio < self.min_top_book_notional_ratio:
                split_count = max(split_count, 2)
            if depth_ratio < self.min_second_book_notional_ratio:
                split_count = max(split_count, 2)
            if spread_bps >= self.max_spread_bps_for_market_open:
                split_count = max(split_count, 2)
            if top_ratio < 0.20 or depth_ratio < 0.45:
                split_count = max(split_count, 3)

        if quality_style and isinstance(quality_style, dict):
            split_count = max(split_count, self._safe_int(quality_style.get("split_count"), 1))

        split_cap = self._capital_mode_split_cap(capital_mode)
        return max(1, min(split_cap, split_count, self.max_split_count))

    def _select_order_type(
        self,
        action: str,
        volatility_value: float,
        spread_bps: float,
        est_slippage_bps: float,
        aggressiveness: str,
        quality_style: Optional[Dict[str, Any]] = None,
        capital_mode: str = "UNKNOWN",
    ) -> str:
        try:
            if action in ("CLOSE", "PARTIAL_CLOSE"):
                return "MARKET" if spread_bps <= self.max_spread_bps_for_market_close else "LIMIT"

            if self.default_type == "MARKET":
                return "MARKET"
            if self.default_type == "LIMIT":
                if volatility_value > self.market_order_volatility_threshold and spread_bps <= 3.5:
                    if capital_mode not in ("SURVIVAL", "DEFENSIVE", "CAPITAL_PRESERVATION"):
                        return "MARKET"
                return "LIMIT"

            allow_market = True
            if quality_style and isinstance(quality_style, dict):
                allow_market = bool(quality_style.get("allow_market", True))
            if capital_mode in ("SURVIVAL", "DEFENSIVE", "CAPITAL_PRESERVATION"):
                allow_market = False if spread_bps > 3.0 else allow_market
            if not allow_market:
                return "LIMIT"

            market_spread_cap = self._capital_mode_market_open_spread_cap(capital_mode)
            market_slippage_cap = self._capital_mode_estimated_slippage_cap(capital_mode)

            if spread_bps >= market_spread_cap:
                return "LIMIT"
            if est_slippage_bps >= market_slippage_cap:
                return "LIMIT"
            if aggressiveness == "AGGRESSIVE" and volatility_value >= self.market_order_volatility_threshold:
                if capital_mode not in ("SURVIVAL", "DEFENSIVE", "CAPITAL_PRESERVATION"):
                    return "MARKET"
            if volatility_value > self.market_order_volatility_threshold and spread_bps <= 4.5:
                if capital_mode not in ("SURVIVAL", "DEFENSIVE", "CAPITAL_PRESERVATION"):
                    return "MARKET"
            return "LIMIT"
        except Exception as e:
            logging.error(f"Order type decision error: {e}")
            return "MARKET"

    def _should_block_open_by_low_volatility(self, volatility_value: float, reason: Optional[str]) -> bool:
        try:
            vol = self._safe_float(volatility_value, 0.0)
            if vol <= 0.0:
                logging.warning("Volatility missing or zero -> bypass low-volatility gate")
                return False
            if vol >= self.min_open_volatility:
                return False
            reason_text = str(reason or "").lower()
            return not any(k in reason_text for k in self.BYPASS_REASON_KEYWORDS)
        except Exception as e:
            logging.error(f"Low volatility block check error: {e}")
            return False

    def _build_order_fingerprint(self, symbol: str, side: Optional[str], action: str, qty: float, order_type: str, price: float) -> str:
        return f"{symbol}|{action}|{side}|{order_type}|{round(self._safe_float(qty, 0.0), 12)}|{round(self._safe_float(price, 0.0), 8)}"

    def _is_throttled(self, symbol: str) -> bool:
        try:
            last_time = self.last_order_time.get(symbol, 0.0)
            return (self._now() - last_time) < self.min_order_interval
        except Exception as e:
            logging.error(f"Throttle check error: {e}")
            return True

    def _same_side_blocked(self, symbol: str, side: Optional[str]) -> bool:
        try:
            if side is None:
                return False
            last_side = self.last_order_side.get(symbol)
            last_time = self.last_order_time.get(symbol, 0.0)
            return last_side == side and (self._now() - last_time) < self.same_side_cooldown
        except Exception as e:
            logging.error(f"Same side block error: {e}")
            return False

    def _is_duplicate_order(self, symbol: str, fingerprint: str) -> bool:
        try:
            last_fp = self.last_order_fingerprint.get(symbol)
            last_rt = self.last_route_time.get(symbol, 0.0)
            return last_fp == fingerprint and (self._now() - last_rt) < self.duplicate_order_cooldown
        except Exception as e:
            logging.error(f"Duplicate order check error: {e}")
            return False

    def _extract_result_status(self, result) -> str:
        try:
            if not isinstance(result, dict):
                return ""
            return str(result.get("execution_status") or result.get("status") or "").upper()
        except Exception:
            return ""

    def _is_success_result(self, result) -> bool:
        try:
            if isinstance(result, bool):
                return result
            if isinstance(result, dict):
                if result.get("ok") is True or result.get("success") is True:
                    status = self._extract_result_status(result)
                    if status in self.FINAL_FAILURE_STATUSES:
                        return False
                    if status in self.ACCEPTED_STATUSES and not result.get("orderId") and not result.get("order_id"):
                        return False
                    return True

                status = self._extract_result_status(result)
                if status in self.SUCCESS_STATUSES:
                    return True
                if status in self.ACCEPTED_STATUSES:
                    return bool(result.get("orderId") or result.get("order_id"))
                if status in self.FINAL_FAILURE_STATUSES:
                    return False
                return bool(result.get("orderId") or result.get("order_id"))
            return bool(result)
        except Exception:
            return False

    def _should_commit_result(self, result) -> bool:
        status = self._extract_result_status(result)
        if status in self.ACCEPTED_STATUSES:
            return False
        return self._is_success_result(result)

    def _register_routed_order(self, symbol: str, fingerprint: str):
        with self.lock:
            self.last_route_time[symbol] = self._now()
            self.last_order_fingerprint[symbol] = fingerprint

    def _rollback_routed_order(self, symbol: str, fingerprint: str):
        with self.lock:
            current_fp = self.last_order_fingerprint.get(symbol)
            if current_fp == fingerprint:
                self.last_order_fingerprint.pop(symbol, None)
                self.last_route_time.pop(symbol, None)
        self._touch()

    def _commit_order_state(self, symbol: str, side: Optional[str], fingerprint: Optional[str] = None):
        now = self._now()
        with self.lock:
            self.last_order_time[symbol] = now
            self.last_route_time[symbol] = now
            if side is not None:
                self.last_order_side[symbol] = side
            if fingerprint:
                self.last_order_fingerprint[symbol] = fingerprint
        self._touch()

    def _build_ref_price(self, side: Optional[str], raw_price: float, book_ctx: Dict[str, float]) -> float:
        ref_price = self._safe_float(raw_price, 0.0)
        if ref_price > 0:
            return ref_price
        if side == "BUY":
            return self._safe_float(book_ctx.get("ask_price"), 0.0) or self._safe_float(book_ctx.get("mid_price"), 0.0)
        if side == "SELL":
            return self._safe_float(book_ctx.get("bid_price"), 0.0) or self._safe_float(book_ctx.get("mid_price"), 0.0)
        return self._safe_float(book_ctx.get("mid_price"), 0.0)

    # ================= BUILD ORDER =================

    def build_order(
        self,
        symbol,
        side=None,
        qty=None,
        price=None,
        volatility=0,
        action="OPEN",
        reason=None,
        reduce_only=None,
        market_context: Optional[Dict[str, Any]] = None,
        urgency: Optional[str] = None,
        extra_meta: Optional[Dict[str, Any]] = None,
    ):
        try:
            symbol = self._normalize_symbol(symbol)
            side = self._normalize_side(side)
            action = self._normalize_action(action)
            vol_value = self._extract_volatility_value(volatility)
            extra_meta = extra_meta or {}
            capital_mode = self._extract_capital_mode(extra_meta)

            if action == "OPEN" and side not in ("BUY", "SELL"):
                raise ValueError(f"Invalid side for OPEN: {side}")
            if action in ("CLOSE", "PARTIAL_CLOSE") and side not in ("BUY", "SELL"):
                raise ValueError(f"Invalid side for {action}: {side}")

            qty = self._normalize_qty(symbol, qty if qty is not None else 0.0)
            if qty <= 0:
                raise ValueError(f"Normalized qty invalid: {qty}")

            book_ctx = self._extract_book_context(market_context)
            ref_price = self._build_ref_price(side, self._safe_float(price, 0.0), book_ctx)
            spread_bps = self._safe_float(book_ctx.get("spread_bps"), 0.0)
            book_pressure = self._safe_float(book_ctx.get("book_pressure"), 0.0)
            order_notional = self._estimate_order_notional(qty, ref_price)
            top_ratio = self._estimate_top_only_ratio(side, order_notional, book_ctx)
            depth_ratio = self._estimate_fill_pressure_ratio(side, order_notional, book_ctx)

            quality_style = self._recommend_style_from_quality_monitor(
                symbol=symbol,
                spread_bps=spread_bps,
                volatility_value=vol_value,
                book_pressure=book_pressure,
            )

            aggressiveness = self._decide_aggressiveness(
                side=side,
                action=action,
                volatility_value=vol_value,
                book_ctx=book_ctx,
                quality_style=quality_style,
                capital_mode=capital_mode,
            )

            est_slippage_market = self._estimate_slippage_bps(
                spread_bps=spread_bps,
                top_ratio=top_ratio,
                depth_ratio=depth_ratio,
                order_type="MARKET",
            )
            est_slippage_limit = self._estimate_slippage_bps(
                spread_bps=spread_bps,
                top_ratio=top_ratio,
                depth_ratio=depth_ratio,
                order_type="LIMIT",
            )

            order_type = self._select_order_type(
                action=action,
                volatility_value=vol_value,
                spread_bps=spread_bps,
                est_slippage_bps=min(est_slippage_market, est_slippage_limit),
                aggressiveness=aggressiveness,
                quality_style=quality_style,
                capital_mode=capital_mode,
            )

            split_count = self._decide_split_count(
                action=action,
                order_notional=order_notional,
                top_ratio=top_ratio,
                depth_ratio=depth_ratio,
                spread_bps=spread_bps,
                quality_style=quality_style,
                capital_mode=capital_mode,
            )

            final_urgency = self._normalize_urgency(urgency)
            if final_urgency == "NORMAL" and quality_style and isinstance(quality_style, dict):
                final_urgency = self._normalize_urgency(quality_style.get("urgency"))

            if capital_mode in ("SURVIVAL", "DEFENSIVE", "CAPITAL_PRESERVATION") and action == "OPEN":
                if final_urgency == "URGENT":
                    final_urgency = "HIGH"

            if order_type == "LIMIT" and ref_price <= 0:
                logging.warning(f"LIMIT requested but price missing -> fallback MARKET | symbol={symbol} side={side}")
                order_type = "MARKET"

            order = {
                "symbol": symbol,
                "action": action,
                "side": side,
                "type": order_type,
                "qty": qty,
                "timestamp": int(self._now() * 1000),
                "allow_partial_fill": bool(ALLOW_PARTIAL_FILL),
                "retryable": True,
                "volatility": vol_value,
                "reason": reason,
                "urgency": final_urgency,
                "aggressiveness": aggressiveness,
                "split_count": split_count,
                "estimated_order_notional": order_notional,
                "estimated_market_slippage_bps": est_slippage_market,
                "estimated_limit_slippage_bps": est_slippage_limit,
                "spread_bps": spread_bps,
                "book_pressure": book_pressure,
                "top_book_ratio": top_ratio,
                "depth_ratio": depth_ratio,
                "capital_mode": capital_mode,
                "router_meta": {
                    "quality_style": quality_style,
                    "market_context": book_ctx,
                    "extra_meta": extra_meta,
                },
            }

            order["reduce_only"] = bool(reduce_only) if reduce_only is not None else action in ("CLOSE", "PARTIAL_CLOSE")

            if order_type == "LIMIT":
                price_aggr = aggressiveness if action == "OPEN" else "AGGRESSIVE"
                limit_price = self._build_limit_price(side, ref_price, aggressiveness=price_aggr)
                limit_price = self._normalize_price(symbol, limit_price, side)
                if limit_price > 0:
                    order["price"] = limit_price
                    order["time_in_force"] = self.default_time_in_force if action == "OPEN" else self.aggressive_limit_time_in_force
                else:
                    order["type"] = "MARKET"

            order["router_fingerprint"] = self._build_order_fingerprint(
                symbol=symbol,
                side=side,
                action=action,
                qty=order.get("qty"),
                order_type=order.get("type", "MARKET"),
                price=order.get("price", ref_price),
            )
            return order

        except Exception as e:
            logging.error(f"Order build error: {e}")
            return None

    def build_split_orders(self, order: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            if order is None:
                return []

            capital_mode = str(order.get("capital_mode", "UNKNOWN")).upper().strip()
            split_cap = self._capital_mode_split_cap(capital_mode)

            split_count = max(1, self._safe_int(order.get("split_count"), 1))
            split_count = min(split_count, split_cap)

            if split_count <= 1:
                return [order]

            symbol = order.get("symbol")
            total_qty = self._safe_float(order.get("qty"), 0.0)
            if total_qty <= 0:
                return [order]

            step = self.qty_step_map.get(symbol, 0.001)
            child_orders: List[Dict[str, Any]] = []
            base_qty = self._floor_to_step(total_qty / split_count, step)
            if base_qty <= 0:
                return [order]

            assigned = 0.0
            for idx in range(split_count):
                child = copy.deepcopy(order)
                child_qty = base_qty if idx < split_count - 1 else self._floor_to_step(max(0.0, total_qty - assigned), step)
                if child_qty <= 0:
                    continue
                child["qty"] = child_qty
                child["split_parent"] = True
                child["split_index"] = idx + 1
                child["split_total"] = split_count
                child["timestamp"] = int(self._now() * 1000)
                child["router_fingerprint"] = self._build_order_fingerprint(
                    symbol=str(child.get("symbol")),
                    side=child.get("side"),
                    action=str(child.get("action", "OPEN")),
                    qty=child_qty,
                    order_type=str(child.get("type", "MARKET")),
                    price=self._safe_float(child.get("price"), 0.0),
                ) + f"|child={idx + 1}/{split_count}"
                child_orders.append(child)
                assigned += child_qty

            return child_orders or [order]

        except Exception as e:
            logging.error(f"Split order build error: {e}")
            return [order] if order else []

    # ================= ROUTE =================

    def route(
        self,
        symbol,
        side=None,
        qty=None,
        price=None,
        volatility=0,
        action="OPEN",
        reason=None,
        reduce_only=None,
        market_context: Optional[Dict[str, Any]] = None,
        urgency: Optional[str] = None,
        extra_meta: Optional[Dict[str, Any]] = None,
    ):
        try:
            self.route_attempts += 1
            symbol = self._normalize_symbol(symbol)
            normalized_side = self._normalize_side(side)
            normalized_action = self._normalize_action(action)
            vol_value = self._extract_volatility_value(volatility)
            extra_meta = extra_meta or {}
            capital_mode = self._extract_capital_mode(extra_meta)

            with self.lock:
                if normalized_action == "OPEN":
                    if self._is_throttled(symbol):
                        self.route_rejections += 1
                        logging.warning(f"Order throttled | symbol={symbol}")
                        return None

                    if self._same_side_blocked(symbol, normalized_side):
                        self.route_rejections += 1
                        logging.warning(f"Same-side order blocked | symbol={symbol} side={normalized_side}")
                        return None

                    if self._should_block_open_by_low_volatility(vol_value, reason):
                        self.route_rejections += 1
                        logging.warning(f"Order blocked by low volatility | symbol={symbol} vol={vol_value}")
                        return None

            order = self.build_order(
                symbol=symbol,
                side=normalized_side,
                qty=qty,
                price=price,
                volatility=volatility,
                action=normalized_action,
                reason=reason,
                reduce_only=reduce_only,
                market_context=market_context,
                urgency=urgency,
                extra_meta=extra_meta,
            )
            if order is None:
                self.route_rejections += 1
                return None

            fingerprint = str(order.get("router_fingerprint", ""))
            registered_duplicate_guard = False
            if normalized_action == "OPEN" and fingerprint:
                with self.lock:
                    if self._is_duplicate_order(symbol, fingerprint):
                        self.route_rejections += 1
                        logging.warning(f"Duplicate order blocked | symbol={symbol} fp={fingerprint}")
                        return None
                    self._register_routed_order(symbol, fingerprint)
                    registered_duplicate_guard = True

            if normalized_action == "OPEN":
                spread_bps = self._safe_float(order.get("spread_bps"), 0.0)
                est_market_slip = self._safe_float(order.get("estimated_market_slippage_bps"), 0.0)

                hard_spread_cap = self._capital_mode_hard_spread_cap(capital_mode)
                limit_spread_cap = self._capital_mode_limit_open_spread_cap(capital_mode)
                market_slippage_cap = self._capital_mode_estimated_slippage_cap(capital_mode)

                if spread_bps > hard_spread_cap:
                    if registered_duplicate_guard:
                        self._rollback_routed_order(symbol, fingerprint)
                    self.route_rejections += 1
                    logging.warning(
                        f"Order blocked by hard spread cap | symbol={symbol} spread_bps={spread_bps:.4f} capital_mode={capital_mode}"
                    )
                    return None

                if spread_bps > limit_spread_cap:
                    if registered_duplicate_guard:
                        self._rollback_routed_order(symbol, fingerprint)
                    self.route_rejections += 1
                    logging.warning(
                        f"Order blocked by excessive spread | symbol={symbol} spread_bps={spread_bps:.4f} capital_mode={capital_mode}"
                    )
                    return None

                if order.get("type") == "MARKET" and est_market_slip > market_slippage_cap * 1.25:
                    if registered_duplicate_guard:
                        self._rollback_routed_order(symbol, fingerprint)
                    self.route_rejections += 1
                    logging.warning(
                        f"Order blocked by excessive estimated slippage | symbol={symbol} "
                        f"est_market_slippage_bps={est_market_slip:.4f} capital_mode={capital_mode}"
                    )
                    return None

            self.route_successes += 1
            self._touch()

            if order.get("type") == "LIMIT" and "price" in order:
                logging.info(
                    f"ORDER ROUTED | symbol={order['symbol']} action={order['action']} side={order['side']} "
                    f"qty={order['qty']} type={order['type']} price={order['price']} vol={order.get('volatility')} "
                    f"spread_bps={order.get('spread_bps')} split={order.get('split_count')} urgency={order.get('urgency')} "
                    f"aggr={order.get('aggressiveness')} capital_mode={order.get('capital_mode')}"
                )
            else:
                logging.info(
                    f"ORDER ROUTED | symbol={order['symbol']} action={order['action']} side={order['side']} "
                    f"qty={order['qty']} type={order['type']} vol={order.get('volatility')} spread_bps={order.get('spread_bps')} "
                    f"split={order.get('split_count')} urgency={order.get('urgency')} aggr={order.get('aggressiveness')} "
                    f"capital_mode={order.get('capital_mode')}"
                )
            return order

        except Exception as e:
            self.route_rejections += 1
            logging.error(f"Routing error: {e}")
            return None

    # ================= RETRY =================

    def retry(self, order, execute_func):
        try:
            if order is None:
                logging.error("Retry aborted: order is None")
                self.retry_failures += 1
                return False

            symbol = str(order.get("symbol"))
            side = self._normalize_side(order.get("side"))
            fingerprint = str(order.get("router_fingerprint", ""))

            attempt_id = None
            market_ctx = {}
            qty = self._safe_float(order.get("qty"), 0.0)
            intended_price = self._safe_float(order.get("price"), 0.0)

            router_meta = order.get("router_meta")
            if isinstance(router_meta, dict):
                market_ctx = router_meta.get("market_context", {}) or {}

            bid_price = self._safe_float(market_ctx.get("bid_price"), 0.0)
            ask_price = self._safe_float(market_ctx.get("ask_price"), 0.0)

            if self.execution_quality_monitor is not None:
                try:
                    attempt_id = self.execution_quality_monitor.start_attempt(
                        symbol=symbol,
                        side=side,
                        order_type=str(order.get("type", "MARKET")).upper(),
                        requested_qty=qty,
                        intended_price=intended_price if intended_price > 0 else None,
                        bid_price=bid_price if bid_price > 0 else None,
                        ask_price=ask_price if ask_price > 0 else None,
                        meta={
                            "action": order.get("action"),
                            "reason": order.get("reason"),
                            "urgency": order.get("urgency"),
                            "aggressiveness": order.get("aggressiveness"),
                            "capital_mode": order.get("capital_mode"),
                        },
                    )
                except Exception as mon_err:
                    logging.warning(f"Execution monitor start_attempt failed: {mon_err}")
                    attempt_id = None

            for attempt in range(1, int(MAX_ORDER_RETRY) + 1):
                try:
                    result = execute_func(order)

                    if attempt_id and self.execution_quality_monitor is not None:
                        try:
                            self.execution_quality_monitor.mark_acknowledged(attempt_id)
                        except Exception:
                            pass

                    if self._is_success_result(result):
                        status = self._extract_result_status(result)
                        logging.info(f"Order success-like result | symbol={symbol} side={side} attempt={attempt} status={status}")

                        if attempt_id and self.execution_quality_monitor is not None:
                            try:
                                if isinstance(result, dict):
                                    filled_qty = self._safe_float(result.get("executedQty", result.get("executed_qty", qty)), qty)
                                    avg_price = self._safe_float(result.get("avgPrice", result.get("avg_price", intended_price)), intended_price)
                                    if avg_price <= 0:
                                        avg_price = intended_price
                                    if status in self.ACCEPTED_STATUSES:
                                        self.execution_quality_monitor.mark_canceled(attempt_id, reason="accepted_not_finalized")
                                    elif filled_qty > 0 and avg_price > 0:
                                        self.execution_quality_monitor.record_fill(
                                            attempt_id,
                                            fill_qty=filled_qty,
                                            fill_price=avg_price,
                                            cumulative_done=True,
                                        )
                                    else:
                                        self.execution_quality_monitor.mark_canceled(attempt_id, reason="success_without_fill_info")
                                else:
                                    proxy_price = intended_price
                                    if proxy_price <= 0:
                                        proxy_price = ask_price if side == "BUY" else bid_price
                                    if proxy_price <= 0:
                                        proxy_price = self._safe_float(market_ctx.get("mid_price"), 0.0)
                                    if qty > 0 and proxy_price > 0:
                                        self.execution_quality_monitor.record_fill(
                                            attempt_id,
                                            fill_qty=qty,
                                            fill_price=proxy_price,
                                            cumulative_done=True,
                                        )
                                    else:
                                        self.execution_quality_monitor.mark_canceled(attempt_id, reason="success_without_pricing_info")
                            except Exception as mon_fill_err:
                                logging.warning(f"Execution monitor fill update failed: {mon_fill_err}")

                        if self._should_commit_result(result):
                            self.retry_successes += 1
                            self._commit_order_state(symbol, side, fingerprint=fingerprint)
                        else:
                            logging.info(f"Commit deferred | symbol={symbol} side={side} status={status}")
                        return result if isinstance(result, dict) else True

                    logging.warning(f"Order retry {attempt}/{MAX_ORDER_RETRY} | symbol={symbol} side={side} result={result}")

                    if attempt_id and self.execution_quality_monitor is not None and attempt == int(MAX_ORDER_RETRY):
                        try:
                            self.execution_quality_monitor.mark_failed(attempt_id, reason=f"retry_exhausted_result={result}")
                        except Exception:
                            pass

                except Exception as exec_err:
                    logging.error(
                        f"Execution exception on attempt {attempt}/{MAX_ORDER_RETRY} | symbol={symbol} side={side} err={exec_err}"
                    )
                    if attempt_id and self.execution_quality_monitor is not None and attempt == int(MAX_ORDER_RETRY):
                        try:
                            self.execution_quality_monitor.mark_failed(attempt_id, reason=f"execution_exception={exec_err}")
                        except Exception:
                            pass

                time.sleep(min(0.2 * attempt, 1.0))

            if fingerprint:
                self._rollback_routed_order(symbol, fingerprint)
            self.retry_failures += 1
            logging.error(f"Order failed after retries | symbol={symbol} side={side}")
            return False

        except Exception as e:
            self.retry_failures += 1
            logging.error(f"Retry error: {e}")
            return False

    def retry_split(self, order, execute_func) -> Any:
        try:
            if order is None:
                self.retry_failures += 1
                return False

            child_orders = self.build_split_orders(order)
            if not child_orders:
                self.retry_failures += 1
                return False

            if len(child_orders) == 1:
                return self.retry(child_orders[0], execute_func)

            results = []
            success_count = 0

            for child in child_orders:
                result = self.retry(child, execute_func)
                results.append(result)
                if self._is_success_result(result):
                    success_count += 1

                urgency = self._normalize_urgency(order.get("urgency", "NORMAL"))
                if urgency == "LOW":
                    time.sleep(self.low_urgency_split_sleep)
                elif urgency == "NORMAL":
                    time.sleep(self.normal_urgency_split_sleep)

            if success_count == len(child_orders):
                self.split_successes += 1
                return {
                    "ok": True,
                    "success": True,
                    "status": "FILLED",
                    "execution_status": "FILLED",
                    "split": True,
                    "results": results,
                    "success_count": success_count,
                    "total_count": len(child_orders),
                }

            if success_count > 0:
                self.split_partial_successes += 1
                return {
                    "ok": True,
                    "success": True,
                    "status": "PARTIALLY_FILLED",
                    "execution_status": "PARTIALLY_FILLED",
                    "split": True,
                    "results": results,
                    "success_count": success_count,
                    "total_count": len(child_orders),
                }

            parent_fp = str(order.get("router_fingerprint", ""))
            parent_symbol = str(order.get("symbol", ""))
            if parent_fp and parent_symbol:
                self._rollback_routed_order(parent_symbol, parent_fp)
            self.retry_failures += 1
            return {
                "ok": False,
                "success": False,
                "status": "FAILED",
                "execution_status": "FAILED",
                "split": True,
                "results": results,
                "success_count": success_count,
                "total_count": len(child_orders),
            }

        except Exception as e:
            self.retry_failures += 1
            logging.error(f"Split retry error: {e}")
            return False

    # ================= FORCE COMMIT =================

    def mark_order_committed(self, symbol: str, side: Optional[str] = None, fingerprint: Optional[str] = None):
        symbol = self._normalize_symbol(symbol)
        side = self._normalize_side(side)
        self._commit_order_state(symbol, side, fingerprint=fingerprint)

    # ================= EXTENSION =================

    def attach_execution_quality_monitor(self, monitor):
        self.execution_quality_monitor = monitor

    # ================= SNAPSHOT =================

    def snapshot(self) -> Dict[str, Any]:
        return {
            "default_type": self.default_type,
            "min_order_interval": self.min_order_interval,
            "same_side_cooldown": self.same_side_cooldown,
            "duplicate_order_cooldown": self.duplicate_order_cooldown,
            "min_open_volatility": self.min_open_volatility,
            "market_order_volatility_threshold": self.market_order_volatility_threshold,
            "max_spread_bps_for_market_open": self.max_spread_bps_for_market_open,
            "max_spread_bps_for_limit_open": self.max_spread_bps_for_limit_open,
            "max_spread_bps_for_market_close": self.max_spread_bps_for_market_close,
            "hard_block_spread_bps_for_open": self.hard_block_spread_bps_for_open,
            "min_top_book_notional_ratio": self.min_top_book_notional_ratio,
            "min_second_book_notional_ratio": self.min_second_book_notional_ratio,
            "max_estimated_slippage_bps": self.max_estimated_slippage_bps,
            "default_split_count": self.default_split_count,
            "max_split_count": self.max_split_count,
            "route_attempts": self.route_attempts,
            "route_rejections": self.route_rejections,
            "route_successes": self.route_successes,
            "retry_successes": self.retry_successes,
            "retry_failures": self.retry_failures,
            "split_successes": self.split_successes,
            "split_partial_successes": self.split_partial_successes,
            "last_order_time": copy.deepcopy(self.last_order_time),
            "last_order_side": copy.deepcopy(self.last_order_side),
            "last_order_fingerprint": copy.deepcopy(self.last_order_fingerprint),
            "last_route_time": copy.deepcopy(self.last_route_time),
            "qty_step_map": copy.deepcopy(self.qty_step_map),
            "min_qty_map": copy.deepcopy(self.min_qty_map),
            "price_tick_map": copy.deepcopy(self.price_tick_map),
            "execution_quality_monitor_attached": self.execution_quality_monitor is not None,
            "updated_at": self.updated_at,
        }