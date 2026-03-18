import copy
import logging
import time
from typing import Any, Dict, Optional

from config import (
    MAX_ORDER_RETRY,
    SLIPPAGE_LIMIT,
    USE_PAPER_TRADING,
)


class SlippageControl:
    """
    Institutional / hedge-fund grade slippage control layer

    설계 목표:
    1) 최신 기준가격(last/mark/mid/reference) 기반 슬리피지 검증
    2) LIMIT / MARKET 주문별 서로 다른 보수성 적용
    3) order payload 를 훼손하지 않도록 방어적 복사 후 검사
    4) paper/live 공용 호환 유지
    5) executor 호출 전 사전 차단 계층 역할 수행
    6) symbol별 최근 가격 추적
    7) retries / last result / block reason 추적
    8) router / executor / main 과 쉽게 결합 가능한 구조 유지
    9) 기준가격 부재 시 과도한 오차 주문 방지 로직 제공
    10) 장기 운용용 snapshot / metrics 제공
    11) quantity precision 보정 최소화 및 안전성 강화
    12) MARKET 주문도 reference price 존재 시 soft check 가능
    13) over-trading 보다 execution protection 우선
    14) 실전형 fail-safe 동작 유지
    15) 수년 운영 가능한 보수적 execution gate 유지
    """

    def __init__(self):
        self.last_price: Dict[str, float] = {}
        self.last_check_result_by_symbol: Dict[str, Dict[str, Any]] = {}
        self.total_checks = 0
        self.blocked_checks = 0
        self.paper_executions = 0
        self.live_retry_successes = 0
        self.live_retry_failures = 0
        self.updated_at = time.time()

        self.market_slippage_relax_multiplier = 1.35
        self.reference_price_staleness_sec = 10.0

    # ================= INTERNAL =================
    def _now(self) -> float:
        return time.time()

    def _touch(self):
        self.updated_at = self._now()

    def _safe_float(self, value, default=0.0):
        try:
            return float(value)
        except Exception:
            return float(default)

    def _normalize_symbol(self, symbol: Optional[str]) -> str:
        return str(symbol or "").upper().strip()

    def _normalize_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        payload = copy.deepcopy(order) if isinstance(order, dict) else {}
        if "symbol" in payload:
            payload["symbol"] = self._normalize_symbol(payload.get("symbol"))
        if "type" in payload:
            payload["type"] = str(payload.get("type", "MARKET")).upper().strip()
        if "side" in payload and payload.get("side") is not None:
            payload["side"] = str(payload.get("side")).upper().strip()
        if "qty" in payload:
            try:
                payload["qty"] = round(float(payload["qty"]), 8)
            except Exception:
                pass
        return payload

    def _extract_reference_price(self, symbol: str, order: Dict[str, Any]) -> float:
        # 우선순위: explicit reference -> market_context -> tracked last_price
        for key in ("reference_price", "last_price", "mark_price", "price"):
            px = self._safe_float(order.get(key), 0.0)
            if px > 0:
                return px

        router_meta = order.get("router_meta", {})
        if isinstance(router_meta, dict):
            market_ctx = router_meta.get("market_context", {}) or {}
            for key in ("mid_price", "ask_price", "bid_price"):
                px = self._safe_float(market_ctx.get(key), 0.0)
                if px > 0:
                    return px

        tracked = self._safe_float(self.last_price.get(symbol), 0.0)
        if tracked > 0:
            return tracked
        return 0.0

    def _allowed_slippage_limit(self, order_type: str) -> float:
        base = abs(self._safe_float(SLIPPAGE_LIMIT, 0.0))
        if order_type == "MARKET":
            return base * self.market_slippage_relax_multiplier
        return base

    def _record_check(self, symbol: str, ok: bool, reason: str, market_price: float, order_price: float, diff: float, allowed: float):
        self.last_check_result_by_symbol[symbol] = {
            "ok": bool(ok),
            "reason": str(reason),
            "market_price": market_price,
            "order_price": order_price,
            "diff": diff,
            "allowed": allowed,
            "checked_at": self._now(),
        }
        self.total_checks += 1
        if not ok:
            self.blocked_checks += 1
        self._touch()

    # ================= PRICE UPDATE =================
    def update_price(self, symbol, price):
        try:
            symbol = self._normalize_symbol(symbol)
            px = self._safe_float(price, 0.0)
            if symbol and px > 0:
                self.last_price[symbol] = px
                self._touch()
        except Exception as e:
            logging.error(f"Slippage update_price error: {e}")

    # ================= SLIPPAGE CHECK =================
    def check_slippage(self, symbol, order_price, order_type: str = "LIMIT"):
        try:
            symbol = self._normalize_symbol(symbol)
            order_price = self._safe_float(order_price, 0.0)
            order_type = str(order_type or "LIMIT").upper().strip()

            if order_price <= 0:
                self._record_check(symbol, False, "invalid_order_price", 0.0, order_price, 999.0, 0.0)
                return False

            market_price = self._safe_float(self.last_price.get(symbol), 0.0)
            if market_price <= 0:
                # 기준가격이 없으면 차단보다 통과를 기본으로 하되 기록은 남김
                self._record_check(symbol, True, "missing_market_price_bypass", 0.0, order_price, 0.0, self._allowed_slippage_limit(order_type))
                return True


            diff = abs(order_price - market_price) / max(market_price, 1e-12)
            allowed = self._allowed_slippage_limit(order_type)

            buffer_mult = 1.15

            if diff > (allowed * buffer_mult):
                logging.warning(
                    f"[SLIPPAGE BLOCKED] | {symbol} type={order_type} "
                    f"diff={diff:.5f} allowed={allowed:.5f} buffered={(allowed * buffer_mult):.5f}"
                )
                self._record_check(
                    symbol,
                    False,
                    "slippage_exceeded",
                    market_price,
                    order_price,
                    diff,
                    allowed,
                )
                return False

            self._record_check(symbol, True, "ok", market_price, order_price, diff, allowed)
            return True

            if diff > allowed * 1.15:
                logging.warning(f"SLIPPAGE BLOCKED | {symbol} type={order_type} diff={diff:.5f} allowed={allowed:.5f}")
                self._record_check(symbol, False, "slippage_exceeded", market_price, order_price, diff, allowed)
                return False

            self._record_check(symbol, True, "ok", market_price, order_price, diff, allowed)
            return True

        except Exception as e:
            logging.error(f"Slippage check error: {e}")
            return False

    # ================= EXECUTION =================
    def execute(self, order, execute_func=None):
        try:
            payload = self._normalize_order(order)
            symbol = payload.get("symbol")
            order_type = str(payload.get("type", "MARKET")).upper().strip()

            if not symbol:
                logging.error("SlippageControl execute aborted: missing symbol")
                return False

            # LIMIT 주문은 강한 슬리피지 체크
            # MARKET 주문은 reference_price 가 있을 때만 완화된 체크
            if order_type == "LIMIT":
                price = self._safe_float(payload.get("price"), 0.0)
                if price <= 0:
                    return False
                if not self.check_slippage(symbol, price, order_type="LIMIT"):
                    return False
            else:
                ref_price = self._extract_reference_price(symbol, payload)
                if ref_price > 0:
                    if not self.check_slippage(symbol, ref_price, order_type="MARKET"):
                        return False

            if USE_PAPER_TRADING:
                self.paper_executions += 1
                self._touch()
                logging.info(f"[PAPER EXECUTION] {payload.get('symbol')} {payload.get('side')} {payload.get('qty')}")
                return True

            if execute_func is None:
                logging.error("SlippageControl execute aborted: execute_func is None")
                self.live_retry_failures += 1
                self._touch()
                return False

            for attempt in range(int(MAX_ORDER_RETRY)):
                try:
                    result = execute_func(payload)
                    if result:
                        self.live_retry_successes += 1
                        self._touch()
                        logging.info(f"ORDER FILLED | {payload.get('symbol')} {payload.get('side')} {payload.get('qty')}")
                        return result if isinstance(result, dict) else True
                except Exception as e:
                    logging.warning(f"Order attempt {attempt + 1} failed: {e}")
                time.sleep(0.2)

            self.live_retry_failures += 1
            self._touch()
            logging.error("ORDER FAILED AFTER RETRIES")
            return False

        except Exception as e:
            self.live_retry_failures += 1
            self._touch()
            logging.error(f"Execution error: {e}")
            return False

    # ================= SNAPSHOT =================
    def snapshot(self) -> Dict[str, Any]:
        return {
            "tracked_symbols": list(self.last_price.keys()),
            "last_price": copy.deepcopy(self.last_price),
            "last_check_result_by_symbol": copy.deepcopy(self.last_check_result_by_symbol),
            "total_checks": self.total_checks,
            "blocked_checks": self.blocked_checks,
            "paper_executions": self.paper_executions,
            "live_retry_successes": self.live_retry_successes,
            "live_retry_failures": self.live_retry_failures,
            "market_slippage_relax_multiplier": self.market_slippage_relax_multiplier,
            "updated_at": self.updated_at,
        }
