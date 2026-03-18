import copy
import logging
import time
from typing import Dict, Any, Optional


class SmartExecutor:
    """
    High-end smart executor

    핵심 강화:
    1) 최소 수량 / 최소 notional 재검증
    2) OPEN 진입 시 기대값 부족 주문 차단
    3) LIMIT / MARKET payload 모두 처리
    4) symbol lock / order interval 유지
    5) 실패 시 lock 정리
    6) partial/close 시 거래소 포지션 기준 재검증
    7) router payload와 호환 강화
    8) 거래소 응답 표준화
    9) LIMIT 응답이 NEW / PARTIALLY_FILLED 여도 정상 수용
    10) split order 실행과도 자연스럽게 연동되도록 표준 response 제공
    11) market_context / router_meta / urgency / aggressiveness 보존
    12) exchange 상태 조회 및 timeout cancel 보강
    """

    SUCCESS_STATUSES = {
        "NEW",
        "PARTIALLY_FILLED",
        "FILLED",
        "ACCEPTED",
        "SUCCESS",
        "EXECUTED",
    }

    FINAL_FAILURE_STATUSES = {
        "REJECTED",
        "EXPIRED",
        "CANCELED",
        "FAILED",
    }

    def __init__(
        self,
        client,
        position_engine=None,
        min_order_interval_seconds: float = 2.0,
        symbol_lock_seconds: float = 3.0,
        retry_attempts: int = 2,
        retry_sleep_seconds: float = 0.35,
        qty_precision: int = 3,
        min_qty: float = 0.001,
        min_notional_usdt: float = 5.0,
        taker_fee_rate: float = 0.0005,
        maker_fee_rate: float = 0.0002,
        expected_edge_floor_ratio: float = 0.0018,
        min_edge_to_cost_ratio: float = 2.2,
        default_stop_loss_ratio: float = 0.0035,
        default_take_profit_ratio: float = 0.0085,
        default_slippage_ratio: float = 0.0004,
        limit_order_timeout_seconds: float = 2.5,
        cancel_on_limit_timeout: bool = False,
    ):
        self.client = client
        self.position_engine = position_engine

        self.min_order_interval_seconds = float(min_order_interval_seconds)
        self.symbol_lock_seconds = float(symbol_lock_seconds)
        self.retry_attempts = int(retry_attempts)
        self.retry_sleep_seconds = float(retry_sleep_seconds)
        self.qty_precision = int(qty_precision)
        self.min_qty = float(min_qty)

        self.min_notional_usdt = float(min_notional_usdt)
        self.taker_fee_rate = float(taker_fee_rate)
        self.maker_fee_rate = float(maker_fee_rate)
        self.expected_edge_floor_ratio = float(expected_edge_floor_ratio)
        self.min_edge_to_cost_ratio = float(min_edge_to_cost_ratio)
        self.default_stop_loss_ratio = float(default_stop_loss_ratio)
        self.default_take_profit_ratio = float(default_take_profit_ratio)
        self.default_slippage_ratio = float(default_slippage_ratio)

        self.limit_order_timeout_seconds = float(limit_order_timeout_seconds)
        self.cancel_on_limit_timeout = bool(cancel_on_limit_timeout)

        self.last_order_ts: Dict[str, float] = {}
        self.symbol_locked_until: Dict[str, float] = {}

        self.qty_precision_map: Dict[str, int] = {
            "BTCUSDT": 3,
            "ETHUSDT": 2,
            "SOLUSDT": 1,
            "BNBUSDT": 2,
            "DOGEUSDT": 0,
        }
        self.min_qty_map: Dict[str, float] = {
            "BTCUSDT": 0.001,
            "ETHUSDT": 0.01,
            "SOLUSDT": 0.1,
            "BNBUSDT": 0.01,
            "DOGEUSDT": 1.0,
        }

    def _now(self) -> float:
        return time.time()

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

    def _normalize_symbol(self, symbol: Optional[str]) -> str:
        return str(symbol or "").upper().strip()

    def _normalize_side(self, side: Optional[str]) -> Optional[str]:
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

    def _normalize_order_type(self, order_type: Optional[str]) -> str:
        if not order_type:
            return "MARKET"
        t = str(order_type).upper().strip()
        if t in ("MARKET", "LIMIT"):
            return t
        return "MARKET"

    def _get_qty_precision(self, symbol: str) -> int:
        return int(self.qty_precision_map.get(symbol, self.qty_precision))

    def _get_min_qty(self, symbol: str) -> float:
        return float(self.min_qty_map.get(symbol, self.min_qty))

    def _round_qty(self, symbol: str, qty: float) -> float:
        try:
            precision = self._get_qty_precision(symbol)
            min_qty = self._get_min_qty(symbol)
            q = round(float(qty), precision)
            if q < min_qty:
                return 0.0
            return q
        except Exception:
            return 0.0

    def _is_symbol_locked(self, symbol: str) -> bool:
        return self._now() < self.symbol_locked_until.get(symbol, 0.0)

    def _lock_symbol(self, symbol: str, seconds: float):
        self.symbol_locked_until[symbol] = self._now() + float(seconds)

    def _unlock_symbol(self, symbol: str):
        self.symbol_locked_until[symbol] = 0.0

    def _recent_order_blocked(self, symbol: str) -> bool:
        last_ts = self.last_order_ts.get(symbol, 0.0)
        return (self._now() - last_ts) < self.min_order_interval_seconds

    def _mark_order_sent(self, symbol: str):
        self.last_order_ts[symbol] = self._now()

    def _build_response(
        self,
        ok: bool,
        symbol: str,
        action: str,
        side: Optional[str],
        qty: float,
        result: Any = None,
        reason: Optional[str] = None,
        status: Optional[str] = None,
        order_type: Optional[str] = None,
        executed_qty: Optional[float] = None,
        avg_price: Optional[float] = None,
        reduce_only: Optional[bool] = None,
        attempt_count: Optional[int] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload = {
            "ok": bool(ok),
            "success": bool(ok),
            "symbol": symbol,
            "action": action,
            "side": side,
            "qty": float(qty),
            "reason": reason,
            "result": result,
            "ts": self._now(),
            "status": status,
            "execution_status": status,
            "type": order_type,
            "executed_qty": self._safe_float(executed_qty, 0.0),
            "executedQty": self._safe_float(executed_qty, 0.0),
            "avg_price": self._safe_float(avg_price, 0.0),
            "avgPrice": self._safe_float(avg_price, 0.0),
            "reduce_only": bool(reduce_only) if reduce_only is not None else False,
            "attempt_count": self._safe_int(attempt_count, 0),
        }
        if isinstance(extra, dict):
            payload.update(extra)
        return payload

    def _extract_router_meta(self, order: Dict[str, Any]) -> Dict[str, Any]:
        meta = order.get("router_meta")
        return meta if isinstance(meta, dict) else {}

    def _extract_market_context(self, order: Dict[str, Any]) -> Dict[str, Any]:
        router_meta = self._extract_router_meta(order)
        market_context = router_meta.get("market_context")
        return market_context if isinstance(market_context, dict) else {}

    def _get_close_side(self, symbol: str) -> Optional[str]:
        if self.position_engine is not None and hasattr(self.position_engine, "get_position"):
            try:
                pos = self.position_engine.get_position(symbol)
                if pos:
                    pos_side = str(pos.get("side", "")).upper()
                    if pos_side == "BUY":
                        return "SELL"
                    if pos_side == "SELL":
                        return "BUY"
            except Exception:
                pass

        try:
            if hasattr(self.client, "futures_position_information"):
                positions = self.client.futures_position_information(symbol=symbol)
                for p in positions:
                    amt = float(p.get("positionAmt", 0.0))
                    if amt > 0:
                        return "SELL"
                    if amt < 0:
                        return "BUY"
        except Exception:
            pass

        return None

    def _get_exchange_position_qty(self, symbol: str) -> float:
        try:
            if not hasattr(self.client, "futures_position_information"):
                return 0.0

            positions = self.client.futures_position_information(symbol=symbol)
            for p in positions:
                amt = abs(float(p.get("positionAmt", 0.0)))
                if amt > 0:
                    return self._round_qty(symbol, amt)

            return 0.0
        except Exception:
            return 0.0

    def _get_engine_position_qty(self, symbol: str) -> float:
        try:
            if self.position_engine is not None and hasattr(self.position_engine, "get_position"):
                pos = self.position_engine.get_position(symbol)
                if pos:
                    return self._round_qty(symbol, pos.get("size", 0.0))
        except Exception:
            pass
        return 0.0

    def _safe_create_futures_order(self, **kwargs):
        if hasattr(self.client, "futures_create_order"):
            return self.client.futures_create_order(**kwargs)
        return self.client.create_order(**kwargs)

    def _safe_get_futures_order(self, symbol: str, order_id: Any):
        if hasattr(self.client, "futures_get_order"):
            return self.client.futures_get_order(symbol=symbol, orderId=order_id)
        return None

    def _safe_cancel_futures_order(self, symbol: str, order_id: Any):
        if hasattr(self.client, "futures_cancel_order"):
            return self.client.futures_cancel_order(symbol=symbol, orderId=order_id)
        return None

    def _estimate_notional(self, qty: float, price: float) -> float:
        if qty <= 0 or price <= 0:
            return 0.0
        return float(qty) * float(price)

    def _extract_ref_price(self, order: Dict[str, Any]) -> float:
        for key in (
            "price",
            "mark_price",
            "last_price",
            "close",
            "px",
            "reference_price",
        ):
            if key in order:
                p = self._safe_float(order.get(key), 0.0)
                if p > 0:
                    return p

        market_context = self._extract_market_context(order)
        for key in ("mid_price", "ask_price", "bid_price"):
            p = self._safe_float(market_context.get(key), 0.0)
            if p > 0:
                return p

        return 0.0

    def _estimate_round_trip_cost_ratio(self, order_type: str, slippage_ratio: float) -> float:
        fee_rate = self.taker_fee_rate if order_type == "MARKET" else self.maker_fee_rate
        return (fee_rate * 2.0) + max(0.0, slippage_ratio)

    def _extract_expected_edge_ratio(self, order: Dict[str, Any]) -> float:
        for key in ("expected_edge_ratio", "edge_ratio", "alpha_edge_ratio", "adjusted_edge_ratio"):
            val = self._safe_float(order.get(key), 0.0)
            if val > 0:
                return val

        tp_ratio = self._safe_float(order.get("take_profit_ratio"), self.default_take_profit_ratio)
        sl_ratio = self._safe_float(order.get("stop_loss_ratio"), self.default_stop_loss_ratio)
        confidence = self._safe_float(order.get("confidence"), 0.0)
        quality = self._safe_float(order.get("quality"), 0.0)
        score = self._safe_float(order.get("score"), 0.0)

        confidence_factor = max(0.65, min(1.15, 0.72 + confidence * 0.35))
        quality_factor = max(0.70, min(1.10, 0.75 + quality * 0.30))
        score_factor = max(0.80, min(1.20, 0.85 + min(score, 3.0) * 0.08))

        inferred_edge = tp_ratio * confidence_factor * quality_factor * score_factor
        inferred_edge = max(inferred_edge, self.expected_edge_floor_ratio)
        inferred_edge = min(inferred_edge, tp_ratio)
        if sl_ratio > 0:
            inferred_edge = max(inferred_edge, min(tp_ratio, sl_ratio * 0.75))

        return float(inferred_edge)

    def _extract_slippage_ratio(self, order: Dict[str, Any], order_type: str) -> float:
        explicit = self._safe_float(
            order.get("slippage_ratio", order.get("expected_slippage_ratio", 0.0)),
            0.0,
        )
        if explicit > 0:
            return explicit

        if order_type == "MARKET":
            est_bps = self._safe_float(order.get("estimated_market_slippage_bps"), 0.0)
        else:
            est_bps = self._safe_float(order.get("estimated_limit_slippage_bps"), 0.0)

        if est_bps > 0:
            return est_bps / 10000.0

        return self.default_slippage_ratio

    def _open_order_risk_checks(
        self,
        symbol: str,
        qty: float,
        order: Dict[str, Any],
        order_type: str,
    ) -> Optional[str]:
        ref_price = self._extract_ref_price(order)
        if ref_price <= 0:
            return "missing_reference_price"

        notional = self._estimate_notional(qty, ref_price)
        if notional < self.min_notional_usdt:
            return f"min_notional_blocked:{notional:.6f}"

        slippage_ratio = self._extract_slippage_ratio(order, order_type)
        round_trip_cost_ratio = self._estimate_round_trip_cost_ratio(order_type, slippage_ratio)
        expected_edge_ratio = self._extract_expected_edge_ratio(order)

        if expected_edge_ratio < self.expected_edge_floor_ratio:
            return f"expected_edge_too_low:{expected_edge_ratio:.6f}"

        if expected_edge_ratio < (round_trip_cost_ratio * self.min_edge_to_cost_ratio):
            return (
                f"edge_to_cost_blocked:edge={expected_edge_ratio:.6f}"
                f":cost={round_trip_cost_ratio:.6f}"
            )

        return None

    def _sync_position_engine_after_success(
        self,
        symbol: str,
        action: str,
        qty: float,
        reason: Optional[str],
    ):
        if self.position_engine is None:
            return

        try:
            if action == "CLOSE" and hasattr(self.position_engine, "close"):
                self.position_engine.close(symbol, reason=reason or "executor_close")
            elif action == "PARTIAL_CLOSE" and hasattr(self.position_engine, "apply_partial_close"):
                self.position_engine.apply_partial_close(symbol, qty)
        except Exception as e:
            logging.error(f"Position engine sync error | symbol={symbol} action={action} err={e}")

    def _standardize_exchange_result(
        self,
        symbol: str,
        action: str,
        side: Optional[str],
        requested_qty: float,
        order_type: str,
        reduce_only: bool,
        raw_result: Any,
        reason: Optional[str] = None,
        attempt_count: int = 1,
    ) -> Dict[str, Any]:
        if not isinstance(raw_result, dict):
            return self._build_response(
                True,
                symbol,
                action,
                side,
                requested_qty,
                result=raw_result,
                reason=reason or "executed",
                status="SUCCESS",
                order_type=order_type,
                executed_qty=requested_qty,
                avg_price=0.0,
                reduce_only=reduce_only,
                attempt_count=attempt_count,
            )

        status = str(
            raw_result.get("status")
            or raw_result.get("execution_status")
            or "SUCCESS"
        ).upper()

        executed_qty = self._safe_float(
            raw_result.get("executedQty", raw_result.get("executed_qty", 0.0)),
            0.0,
        )

        orig_qty = self._safe_float(
            raw_result.get("origQty", raw_result.get("orig_qty", requested_qty)),
            requested_qty,
        )
        if executed_qty <= 0 and status in ("FILLED", "PARTIALLY_FILLED", "NEW", "ACCEPTED"):
            if status == "FILLED":
                executed_qty = requested_qty if requested_qty > 0 else orig_qty
            elif status == "PARTIALLY_FILLED":
                executed_qty = min(requested_qty, orig_qty) if requested_qty > 0 else orig_qty

        avg_price = self._safe_float(
            raw_result.get("avgPrice", raw_result.get("avg_price", 0.0)),
            0.0,
        )
        if avg_price <= 0:
            avg_price = self._safe_float(
                raw_result.get("price", raw_result.get("stopPrice", 0.0)),
                0.0,
            )

        return self._build_response(
            True,
            symbol,
            action,
            side,
            requested_qty,
            result=raw_result,
            reason=reason or "executed",
            status=status,
            order_type=order_type,
            executed_qty=executed_qty,
            avg_price=avg_price,
            reduce_only=reduce_only,
            attempt_count=attempt_count,
            extra={
                "orderId": raw_result.get("orderId"),
                "clientOrderId": raw_result.get("clientOrderId"),
                "origQty": orig_qty,
                "orig_qty": orig_qty,
            },
        )

    def _is_success_status(self, status: str) -> bool:
        return str(status).upper() in self.SUCCESS_STATUSES

    def _query_limit_order_final_state(
        self,
        symbol: str,
        order_id: Any,
        fallback_response: Dict[str, Any],
    ) -> Dict[str, Any]:
        try:
            if not order_id:
                return fallback_response

            queried = self._safe_get_futures_order(symbol, order_id)
            if not isinstance(queried, dict):
                return fallback_response

            status = str(queried.get("status", "")).upper()
            executed_qty = self._safe_float(
                queried.get("executedQty", queried.get("executed_qty", 0.0)),
                fallback_response.get("executed_qty", 0.0),
            )
            avg_price = self._safe_float(
                queried.get("avgPrice", queried.get("avg_price", 0.0)),
                fallback_response.get("avg_price", 0.0),
            )

            updated = copy.deepcopy(fallback_response)
            updated["result"] = queried
            updated["status"] = status or fallback_response.get("status")
            updated["execution_status"] = updated["status"]
            updated["executed_qty"] = executed_qty
            updated["executedQty"] = executed_qty
            updated["avg_price"] = avg_price
            updated["avgPrice"] = avg_price

            if status in self.FINAL_FAILURE_STATUSES:
                updated["ok"] = False
                updated["success"] = False
                updated["reason"] = f"limit_final_status:{status}"

            return updated

        except Exception as e:
            logging.warning(f"Limit final state query failed | symbol={symbol} order_id={order_id} err={e}")
            return fallback_response

    def _maybe_wait_limit_order(
        self,
        symbol: str,
        standardized_response: Dict[str, Any],
    ) -> Dict[str, Any]:
        try:
            order_id = standardized_response.get("orderId")
            status = str(standardized_response.get("status", "")).upper()

            if standardized_response.get("type") != "LIMIT":
                return standardized_response

            if status not in ("NEW", "PARTIALLY_FILLED", "ACCEPTED"):
                return standardized_response

            if self.limit_order_timeout_seconds <= 0:
                return standardized_response

            time.sleep(self.limit_order_timeout_seconds)
            updated = self._query_limit_order_final_state(
                symbol=symbol,
                order_id=order_id,
                fallback_response=standardized_response,
            )

            final_status = str(updated.get("status", "")).upper()
            if (
                self.cancel_on_limit_timeout
                and order_id
                and final_status in ("NEW", "PARTIALLY_FILLED", "ACCEPTED")
            ):
                try:
                    self._safe_cancel_futures_order(symbol, order_id)
                    updated["reason"] = "limit_timeout_canceled"
                except Exception as e:
                    logging.warning(f"Limit cancel failed | symbol={symbol} order_id={order_id} err={e}")

            return updated

        except Exception as e:
            logging.warning(f"Limit wait processing failed | symbol={symbol} err={e}")
            return standardized_response

    def _log_order_attempt(
        self,
        symbol: str,
        action: str,
        side: Optional[str],
        qty: float,
        order_type: str,
        reduce_only: bool,
        attempt: int,
        params: Dict[str, Any],
        order: Dict[str, Any],
    ):
        logging.warning(
            f"[ORDER_ATTEMPT] {symbol} | action={action} | side={side} "
            f"| qty={qty} | type={order_type} | reduce_only={reduce_only} "
            f"| attempt={attempt} | urgency={order.get('urgency')} "
            f"| aggr={order.get('aggressiveness')} | split={order.get('split_index')}/{order.get('split_total')} "
            f"| params={params}"
        )

    def execute(self, order: Dict[str, Any]) -> Dict[str, Any]:
        try:
            if not isinstance(order, dict):
                return self._build_response(
                    False, "UNKNOWN", "OPEN", None, 0.0, reason="invalid_order_type"
                )

            symbol = self._normalize_symbol(order.get("symbol"))
            if not symbol:
                return self._build_response(
                    False, "UNKNOWN", "OPEN", None, 0.0, reason="missing_symbol"
                )

            action = self._normalize_action(order.get("action"))
            order_type = self._normalize_order_type(order.get("type"))

            side = None
            qty = 0.0
            reduce_only = False

            if action == "OPEN":
                side = self._normalize_side(order.get("side"))
                qty = self._round_qty(symbol, order.get("qty", 0.0))

                if side is None:
                    return self._build_response(
                        False, symbol, action, None, qty, reason="invalid_side"
                    )

                if qty <= 0:
                    return self._build_response(
                        False, symbol, action, side, qty, reason="invalid_qty"
                    )

                open_block_reason = self._open_order_risk_checks(symbol, qty, order, order_type)
                if open_block_reason:
                    return self._build_response(
                        False, symbol, action, side, qty, reason=open_block_reason
                    )

                if self.position_engine is not None and hasattr(self.position_engine, "can_open"):
                    if not self.position_engine.can_open(symbol, side):
                        return self._build_response(
                            False, symbol, action, side, qty, reason="position_blocked"
                        )

            elif action == "CLOSE":
                side = self._get_close_side(symbol)
                if side is None:
                    return self._build_response(
                        False, symbol, action, None, 0.0, reason="no_position_to_close"
                    )

                qty = self._get_engine_position_qty(symbol)
                if qty <= 0:
                    qty = self._get_exchange_position_qty(symbol)
                if qty <= 0:
                    qty = self._round_qty(symbol, order.get("qty", 0.0))

                if qty <= 0:
                    return self._build_response(
                        False, symbol, action, side, qty, reason="invalid_close_qty"
                    )

                reduce_only = True

            elif action == "PARTIAL_CLOSE":
                side = self._get_close_side(symbol)
                if side is None:
                    return self._build_response(
                        False,
                        symbol,
                        action,
                        None,
                        0.0,
                        reason="no_position_to_partial_close",
                    )

                requested_qty = self._round_qty(symbol, order.get("size", order.get("qty", 0.0)))
                engine_qty = self._get_engine_position_qty(symbol)
                exchange_qty = self._get_exchange_position_qty(symbol)

                available_qty = engine_qty if engine_qty > 0 else exchange_qty
                if available_qty > 0:
                    qty = min(requested_qty, available_qty)
                    qty = self._round_qty(symbol, qty)
                else:
                    qty = requested_qty

                if qty <= 0:
                    return self._build_response(
                        False, symbol, action, side, qty, reason="invalid_partial_qty"
                    )

                reduce_only = True

            if bool(order.get("reduce_only", False)):
                reduce_only = True

            if self._is_symbol_locked(symbol):
                return self._build_response(
                    False, symbol, action, side, qty, reason="symbol_locked",
                    order_type=order_type, reduce_only=reduce_only
                )

            if self._recent_order_blocked(symbol):
                return self._build_response(
                    False, symbol, action, side, qty, reason="order_interval_blocked",
                    order_type=order_type, reduce_only=reduce_only
                )

            last_error = None
            locked_here = False

            for attempt in range(1, self.retry_attempts + 2):
                try:
                    if not locked_here:
                        self._lock_symbol(symbol, self.symbol_lock_seconds)
                        locked_here = True

                    params = {
                        "symbol": symbol,
                        "side": side,
                        "type": order_type,
                        "quantity": qty,
                    }

                    if order_type == "LIMIT":
                        limit_price = self._safe_float(order.get("price"), 0.0)
                        if limit_price <= 0:
                            return self._build_response(
                                False,
                                symbol,
                                action,
                                side,
                                qty,
                                reason="invalid_limit_price",
                                order_type=order_type,
                                reduce_only=reduce_only,
                            )
                        params["price"] = limit_price
                        params["timeInForce"] = order.get("time_in_force", "GTC")

                    if reduce_only:
                        params["reduceOnly"] = True

                    self._log_order_attempt(
                        symbol=symbol,
                        action=action,
                        side=side,
                        qty=qty,
                        order_type=order_type,
                        reduce_only=reduce_only,
                        attempt=attempt,
                        params=params,
                        order=order,
                    )

                    result = self._safe_create_futures_order(**params)
                    self._mark_order_sent(symbol)

                    standardized = self._standardize_exchange_result(
                        symbol=symbol,
                        action=action,
                        side=side,
                        requested_qty=qty,
                        order_type=order_type,
                        reduce_only=reduce_only,
                        raw_result=result,
                        reason="executed",
                        attempt_count=attempt,
                    )

                    if order_type == "LIMIT":
                        standardized = self._maybe_wait_limit_order(symbol, standardized)

                    final_status = str(standardized.get("status", "")).upper()

                    if self._is_success_status(final_status):
                        self._sync_position_engine_after_success(
                            symbol=symbol,
                            action=action,
                            qty=standardized.get("executed_qty", qty) if action != "CLOSE" else qty,
                            reason=order.get("reason", "executed"),
                        )

                        logging.info(
                            f"ORDER EXECUTED | {symbol} | action={action} | side={side} "
                            f"| qty={qty} | type={order_type} | order_id={standardized.get('orderId')} "
                            f"| status={final_status} | executed_qty={standardized.get('executed_qty')} "
                            f"| avg_price={standardized.get('avg_price')}"
                        )

                        return standardized

                    last_error = standardized.get("reason") or f"final_status={final_status}"
                    logging.warning(
                        f"Order non-success status | symbol={symbol} action={action} side={side} "
                        f"| qty={qty} | type={order_type} | attempt={attempt} | status={final_status} "
                        f"| reason={last_error}"
                    )

                    if attempt <= self.retry_attempts:
                        time.sleep(self.retry_sleep_seconds)

                except Exception as e:
                    last_error = str(e)
                    logging.error(
                        f"Execution Error | {symbol} | action={action} | side={side} "
                        f"| qty={qty} | type={order_type} | attempt={attempt} | error={e}"
                    )

                    if attempt <= self.retry_attempts:
                        time.sleep(self.retry_sleep_seconds)

            return self._build_response(
                False,
                symbol,
                action,
                side,
                qty,
                result=None,
                reason=last_error or "execution_failed",
                status="FAILED",
                order_type=order_type,
                reduce_only=reduce_only,
                attempt_count=self.retry_attempts + 1,
            )

        except Exception as e:
            logging.error(f"SmartExecutor fatal error: {e}")
            symbol = order.get("symbol", "UNKNOWN") if isinstance(order, dict) else "UNKNOWN"
            action = self._normalize_action(order.get("action")) if isinstance(order, dict) else "OPEN"
            return self._build_response(
                False,
                symbol,
                action,
                None,
                0.0,
                result=None,
                reason=str(e),
                status="FAILED",
            )

        finally:
            try:
                if isinstance(order, dict):
                    symbol = self._normalize_symbol(order.get("symbol"))
                    if symbol:
                        self._unlock_symbol(symbol)
            except Exception:
                pass