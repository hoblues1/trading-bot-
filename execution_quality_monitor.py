import json
import logging
import math
import os
import statistics
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, asdict, field
from typing import Any, Deque, Dict, List, Optional, Tuple


@dataclass
class FillEvent:
    timestamp: float
    qty: float
    price: float


@dataclass
class ExecutionAttempt:
    attempt_id: str
    symbol: str
    side: str
    order_type: str
    requested_qty: float
    intended_price: Optional[float]
    bid_price: Optional[float]
    ask_price: Optional[float]
    spread_bps_at_submit: Optional[float]
    created_ts: float
    meta: Dict[str, Any] = field(default_factory=dict)

    ack_ts: Optional[float] = None
    completed_ts: Optional[float] = None
    status: str = "NEW"  # NEW / ACKED / FILLED / PARTIALLY_FILLED / CANCELED / REJECTED / EXPIRED / FAILED

    fills: List[FillEvent] = field(default_factory=list)

    canceled_reason: Optional[str] = None
    rejected_reason: Optional[str] = None
    exchange_order_id: Optional[str] = None

    @property
    def filled_qty(self) -> float:
        return sum(f.qty for f in self.fills)

    @property
    def remaining_qty(self) -> float:
        return max(0.0, self.requested_qty - self.filled_qty)

    @property
    def fill_ratio(self) -> float:
        if self.requested_qty <= 0:
            return 0.0
        return max(0.0, min(1.0, self.filled_qty / self.requested_qty))

    @property
    def weighted_avg_fill_price(self) -> Optional[float]:
        total_qty = self.filled_qty
        if total_qty <= 0:
            return None
        notional = sum(f.qty * f.price for f in self.fills)
        return notional / total_qty

    @property
    def submit_to_ack_ms(self) -> Optional[float]:
        if self.ack_ts is None:
            return None
        return max(0.0, (self.ack_ts - self.created_ts) * 1000.0)

    @property
    def submit_to_complete_ms(self) -> Optional[float]:
        if self.completed_ts is None:
            return None
        return max(0.0, (self.completed_ts - self.created_ts) * 1000.0)

    @property
    def completion_ratio(self) -> float:
        return self.fill_ratio


class ExecutionQualityMonitor:
    """
    Institutional / hedge-fund grade execution quality monitor

    설계 목표:
    1) 주문 시도 lifecycle(제출/ack/partial/fill/cancel/reject/fail) 전부 추적
    2) 심볼별 + 전체 체결 품질을 동시에 집계
    3) slippage / spread / ack latency / completion latency / fill ratio 관리
    4) stale active attempt 자동 만료
    5) router/executor가 바로 참고할 수 있는 health flag / execution style recommendation 제공
    6) 파일 저장/복원 지원
    7) 장기 운용에서도 history 메모리 사용량 통제
    8) thread-safe 구조로 상태 오염 방지
    9) snapshot / recent attempts / health severity 제공
    10) partial fill 악화, rejection 증가, cancel 증가를 조기 탐지
    11) execution style을 PASSIVE/BALANCED/DEFENSIVE 등으로 제안
    12) 수년간 운영 가능한 체결 품질 진단 계층 유지
    13) existing router / executor 코드와 바로 붙는 호환 API 유지
    14) persistence 실패가 전체 시스템을 죽이지 않도록 보호
    15) symbol reset / full reset / autosave 모두 안전 동작
    16) 최근 데이터 기준으로 실행 품질 악화 추세를 빠르게 반영
    """

    FINAL_STATUSES = {"FILLED", "PARTIALLY_FILLED", "CANCELED", "REJECTED", "EXPIRED", "FAILED"}

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        rolling_window: int = 200,
        persist_path: Optional[str] = None,
        autosave_interval_sec: float = 30.0,
        poor_fill_ratio_threshold: float = 0.65,
        high_slippage_bps_threshold: float = 8.0,
        slow_fill_ms_threshold: float = 2500.0,
        rejection_rate_threshold: float = 0.12,
        cancel_rate_threshold: float = 0.45,
        stale_attempt_expire_sec: float = 30.0,
    ):
        self.logger = logger or logging.getLogger(__name__)
        self.rolling_window = max(20, int(rolling_window))
        self.persist_path = persist_path
        self.autosave_interval_sec = max(5.0, float(autosave_interval_sec))
        self.stale_attempt_expire_sec = max(5.0, float(stale_attempt_expire_sec))

        self.poor_fill_ratio_threshold = float(poor_fill_ratio_threshold)
        self.high_slippage_bps_threshold = float(high_slippage_bps_threshold)
        self.slow_fill_ms_threshold = float(slow_fill_ms_threshold)
        self.rejection_rate_threshold = float(rejection_rate_threshold)
        self.cancel_rate_threshold = float(cancel_rate_threshold)

        self.active_attempts: Dict[str, ExecutionAttempt] = {}
        self.history_by_symbol: Dict[str, Deque[ExecutionAttempt]] = defaultdict(
            lambda: deque(maxlen=self.rolling_window)
        )
        self.global_history: Deque[ExecutionAttempt] = deque(maxlen=self.rolling_window * 5)

        self.last_save_ts = 0.0
        self.last_expire_check_ts = 0.0
        self.updated_at = time.time()

        self.total_attempts_started = 0
        self.total_attempts_finalized = 0
        self.total_expired_attempts = 0
        self.total_persist_failures = 0

        self.lock = threading.RLock()

        if self.persist_path and os.path.exists(self.persist_path):
            try:
                self.load_state()
                self.logger.info("[ExecutionQualityMonitor] Loaded state from %s", self.persist_path)
            except Exception as e:
                self.logger.exception(
                    "[ExecutionQualityMonitor] Failed loading state from %s: %s",
                    self.persist_path,
                    e,
                )

    # -------------------------------------------------------------------------
    # Public API: Attempt lifecycle
    # -------------------------------------------------------------------------

    def start_attempt(
        self,
        symbol: str,
        side: str,
        order_type: str,
        requested_qty: float,
        intended_price: Optional[float] = None,
        bid_price: Optional[float] = None,
        ask_price: Optional[float] = None,
        meta: Optional[Dict[str, Any]] = None,
        attempt_id: Optional[str] = None,
    ) -> str:
        now = time.time()
        attempt_id = attempt_id or str(uuid.uuid4())

        spread_bps = self._calc_spread_bps(bid_price, ask_price)
        attempt = ExecutionAttempt(
            attempt_id=attempt_id,
            symbol=str(symbol).upper().strip(),
            side=str(side).upper().strip(),
            order_type=str(order_type).upper().strip(),
            requested_qty=float(requested_qty),
            intended_price=float(intended_price) if intended_price is not None else None,
            bid_price=float(bid_price) if bid_price is not None else None,
            ask_price=float(ask_price) if ask_price is not None else None,
            spread_bps_at_submit=spread_bps,
            created_ts=now,
            meta=meta or {},
        )

        with self.lock:
            self._expire_stale_if_needed(now)
            self.active_attempts[attempt_id] = attempt
            self.total_attempts_started += 1
            self.updated_at = now

        self.logger.debug(
            "[ExecutionQualityMonitor] start_attempt | id=%s symbol=%s side=%s type=%s qty=%.8f intended_price=%s spread_bps=%s",
            attempt_id,
            symbol,
            side,
            order_type,
            requested_qty,
            intended_price,
            spread_bps,
        )
        return attempt_id

    def mark_acknowledged(
        self,
        attempt_id: str,
        exchange_order_id: Optional[str] = None,
        ack_ts: Optional[float] = None,
    ) -> None:
        with self.lock:
            attempt = self.active_attempts.get(attempt_id)
            if attempt is None:
                self.logger.warning(
                    "[ExecutionQualityMonitor] mark_acknowledged ignored: unknown attempt_id=%s",
                    attempt_id,
                )
                return

            attempt.ack_ts = ack_ts or time.time()
            attempt.status = "ACKED"
            if exchange_order_id is not None:
                attempt.exchange_order_id = str(exchange_order_id)
            self.updated_at = time.time()

    def record_fill(
        self,
        attempt_id: str,
        fill_qty: float,
        fill_price: float,
        fill_ts: Optional[float] = None,
        cumulative_done: bool = False,
    ) -> None:
        with self.lock:
            attempt = self.active_attempts.get(attempt_id)
            if attempt is None:
                self.logger.warning(
                    "[ExecutionQualityMonitor] record_fill ignored: unknown attempt_id=%s",
                    attempt_id,
                )
                return

            event = FillEvent(
                timestamp=fill_ts or time.time(),
                qty=float(fill_qty),
                price=float(fill_price),
            )
            attempt.fills.append(event)

            if attempt.fill_ratio >= 1.0 or cumulative_done:
                attempt.status = "FILLED" if attempt.fill_ratio >= 1.0 else "PARTIALLY_FILLED"
                attempt.completed_ts = event.timestamp
                self._finalize_attempt_locked(attempt_id)
            else:
                attempt.status = "PARTIALLY_FILLED"
                self.updated_at = time.time()

    def mark_canceled(
        self,
        attempt_id: str,
        reason: Optional[str] = None,
        canceled_ts: Optional[float] = None,
    ) -> None:
        with self.lock:
            attempt = self.active_attempts.get(attempt_id)
            if attempt is None:
                self.logger.warning(
                    "[ExecutionQualityMonitor] mark_canceled ignored: unknown attempt_id=%s",
                    attempt_id,
                )
                return

            attempt.status = "CANCELED"
            attempt.canceled_reason = reason
            attempt.completed_ts = canceled_ts or time.time()
            self._finalize_attempt_locked(attempt_id)

    def mark_rejected(
        self,
        attempt_id: str,
        reason: Optional[str] = None,
        rejected_ts: Optional[float] = None,
    ) -> None:
        with self.lock:
            attempt = self.active_attempts.get(attempt_id)
            if attempt is None:
                self.logger.warning(
                    "[ExecutionQualityMonitor] mark_rejected ignored: unknown attempt_id=%s",
                    attempt_id,
                )
                return

            attempt.status = "REJECTED"
            attempt.rejected_reason = reason
            attempt.completed_ts = rejected_ts or time.time()
            self._finalize_attempt_locked(attempt_id)

    def mark_failed(
        self,
        attempt_id: str,
        reason: Optional[str] = None,
        failed_ts: Optional[float] = None,
    ) -> None:
        with self.lock:
            attempt = self.active_attempts.get(attempt_id)
            if attempt is None:
                self.logger.warning(
                    "[ExecutionQualityMonitor] mark_failed ignored: unknown attempt_id=%s",
                    attempt_id,
                )
                return

            attempt.status = "FAILED"
            attempt.rejected_reason = reason
            attempt.completed_ts = failed_ts or time.time()
            self._finalize_attempt_locked(attempt_id)

    def force_expire_stale_attempts(self, stale_after_sec: Optional[float] = None) -> int:
        stale_after_sec = max(1.0, float(stale_after_sec or self.stale_attempt_expire_sec))
        with self.lock:
            return self._force_expire_stale_attempts_locked(stale_after_sec)

    # -------------------------------------------------------------------------
    # Analytics / Snapshot
    # -------------------------------------------------------------------------

    def get_symbol_snapshot(self, symbol: str) -> Dict[str, Any]:
        with self.lock:
            history = list(self.history_by_symbol.get(str(symbol).upper().strip(), []))
        return self._build_snapshot(symbol=str(symbol).upper().strip(), history=history)

    def get_global_snapshot(self) -> Dict[str, Any]:
        with self.lock:
            history = list(self.global_history)
        return self._build_snapshot(symbol="GLOBAL", history=history)

    def get_recent_attempts(self, symbol: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        limit = max(1, int(limit))
        with self.lock:
            if symbol:
                hist = list(self.history_by_symbol.get(str(symbol).upper().strip(), []))[-limit:]
            else:
                hist = list(self.global_history)[-limit:]
        return [self._attempt_to_dict(a) for a in hist]

    def get_health_flag(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        snapshot = self.get_symbol_snapshot(symbol) if symbol else self.get_global_snapshot()

        poor_fill_ratio = snapshot["avg_fill_ratio"] is not None and snapshot["avg_fill_ratio"] < self.poor_fill_ratio_threshold
        high_slippage = snapshot["avg_slippage_bps"] is not None and snapshot["avg_slippage_bps"] > self.high_slippage_bps_threshold
        slow_fill = snapshot["avg_completion_ms"] is not None and snapshot["avg_completion_ms"] > self.slow_fill_ms_threshold
        high_rejection = snapshot["rejection_rate"] is not None and snapshot["rejection_rate"] > self.rejection_rate_threshold
        high_cancel = snapshot["cancel_rate"] is not None and snapshot["cancel_rate"] > self.cancel_rate_threshold

        degradation_score = sum(
            int(x) for x in [poor_fill_ratio, high_slippage, slow_fill, high_rejection, high_cancel]
        )

        if degradation_score >= 4:
            severity = "CRITICAL"
        elif degradation_score >= 2:
            severity = "WARNING"
        else:
            severity = "NORMAL"

        return {
            "symbol": symbol or "GLOBAL",
            "severity": severity,
            "degradation_score": degradation_score,
            "poor_fill_ratio": poor_fill_ratio,
            "high_slippage": high_slippage,
            "slow_fill": slow_fill,
            "high_rejection": high_rejection,
            "high_cancel": high_cancel,
            "snapshot": snapshot,
        }

    def recommend_execution_style(
        self,
        symbol: str,
        current_spread_bps: Optional[float] = None,
        current_volatility_score: Optional[float] = None,
        current_book_pressure: Optional[float] = None,
    ) -> Dict[str, Any]:
        health = self.get_health_flag(symbol)
        snapshot = health["snapshot"]
        reasons = []

        avg_slippage = snapshot.get("avg_slippage_bps")
        avg_completion_ms = snapshot.get("avg_completion_ms")
        avg_fill_ratio = snapshot.get("avg_fill_ratio")

        mode = "BALANCED"
        allow_market = True
        split_count = 1
        urgency = "NORMAL"
        aggressiveness = "BALANCED"

        if current_spread_bps is not None:
            if current_spread_bps >= 10:
                reasons.append(f"현재 스프레드 넓음({current_spread_bps:.2f}bps)")
                mode = "PASSIVE_LIMIT"
                allow_market = False
                split_count = max(split_count, 2)
                urgency = "LOW"
                aggressiveness = "PASSIVE"
            elif current_spread_bps >= 5:
                reasons.append(f"현재 스프레드 다소 넓음({current_spread_bps:.2f}bps)")
                split_count = max(split_count, 2)

        if avg_slippage is not None and avg_slippage > self.high_slippage_bps_threshold:
            reasons.append(f"최근 슬리피지 악화({avg_slippage:.2f}bps)")
            mode = "PASSIVE_LIMIT"
            allow_market = False
            split_count = max(split_count, 2)
            urgency = "LOW"
            aggressiveness = "PASSIVE"

        if avg_completion_ms is not None and avg_completion_ms > self.slow_fill_ms_threshold:
            reasons.append(f"최근 체결 지연({avg_completion_ms:.1f}ms)")
            split_count = max(split_count, 2)

        if avg_fill_ratio is not None and avg_fill_ratio < self.poor_fill_ratio_threshold:
            reasons.append(f"최근 체결비율 저하({avg_fill_ratio:.2%})")
            mode = "BALANCED"
            split_count = max(split_count, 3)

        if current_volatility_score is not None and current_volatility_score >= 0.85:
            reasons.append(f"현재 변동성 높음({current_volatility_score:.2f})")
            split_count = max(split_count, 2)
            if mode != "PASSIVE_LIMIT":
                mode = "BALANCED"

        if current_book_pressure is not None and abs(current_book_pressure) >= 0.8 and allow_market:
            reasons.append(f"호가 압력 강함({current_book_pressure:.2f})")
            urgency = "HIGH"
            aggressiveness = "AGGRESSIVE"

        if health["severity"] == "CRITICAL":
            reasons.append("체결 품질 상태가 위험 수준")
            mode = "DEFENSIVE"
            allow_market = False
            split_count = max(split_count, 3)
            urgency = "LOW"
            aggressiveness = "PASSIVE"

        return {
            "symbol": str(symbol).upper().strip(),
            "mode": mode,
            "allow_market": allow_market,
            "split_count": int(split_count),
            "urgency": urgency,
            "aggressiveness": aggressiveness,
            "reason": reasons or ["기본 균형 모드"],
            "health_severity": health["severity"],
        }

    # -------------------------------------------------------------------------
    # Convenience helpers
    # -------------------------------------------------------------------------

    def register_completed_trade(
        self,
        symbol: str,
        side: str,
        order_type: str,
        requested_qty: float,
        fills: List[Tuple[float, float]],
        intended_price: Optional[float] = None,
        bid_price: Optional[float] = None,
        ask_price: Optional[float] = None,
        created_ts: Optional[float] = None,
        ack_ts: Optional[float] = None,
        completed_ts: Optional[float] = None,
        status: str = "FILLED",
        meta: Optional[Dict[str, Any]] = None,
    ) -> str:
        attempt_id = str(uuid.uuid4())
        attempt = ExecutionAttempt(
            attempt_id=attempt_id,
            symbol=str(symbol).upper().strip(),
            side=str(side).upper().strip(),
            order_type=str(order_type).upper().strip(),
            requested_qty=float(requested_qty),
            intended_price=float(intended_price) if intended_price is not None else None,
            bid_price=float(bid_price) if bid_price is not None else None,
            ask_price=float(ask_price) if ask_price is not None else None,
            spread_bps_at_submit=self._calc_spread_bps(bid_price, ask_price),
            created_ts=created_ts or time.time(),
            meta=meta or {},
        )
        attempt.ack_ts = ack_ts or attempt.created_ts
        attempt.completed_ts = completed_ts or time.time()
        attempt.status = str(status).upper().strip()

        fill_ts = attempt.completed_ts
        for qty, price in fills:
            attempt.fills.append(FillEvent(timestamp=fill_ts, qty=float(qty), price=float(price)))

        with self.lock:
            self._append_history_locked(attempt)
            self.total_attempts_started += 1
            self.total_attempts_finalized += 1
            self.updated_at = time.time()
            self._autosave_if_needed_locked()
        return attempt_id

    def reset_symbol(self, symbol: str) -> None:
        symbol = str(symbol).upper().strip()
        with self.lock:
            if symbol in self.history_by_symbol:
                del self.history_by_symbol[symbol]
            active_ids = [aid for aid, att in self.active_attempts.items() if att.symbol == symbol]
            for aid in active_ids:
                self.active_attempts.pop(aid, None)
            self.updated_at = time.time()

    def reset_all(self) -> None:
        with self.lock:
            self.active_attempts.clear()
            self.history_by_symbol.clear()
            self.global_history.clear()
            self.total_attempts_started = 0
            self.total_attempts_finalized = 0
            self.total_expired_attempts = 0
            self.updated_at = time.time()

    # -------------------------------------------------------------------------
    # Persistence
    # -------------------------------------------------------------------------

    def save_state(self) -> None:
        if not self.persist_path:
            return

        try:
            dirpath = os.path.dirname(self.persist_path)
            if dirpath:
                os.makedirs(dirpath, exist_ok=True)

            with self.lock:
                payload = {
                    "rolling_window": self.rolling_window,
                    "last_save_ts": time.time(),
                    "history_by_symbol": {
                        symbol: [self._attempt_to_dict(a) for a in history]
                        for symbol, history in self.history_by_symbol.items()
                    },
                    "global_history": [self._attempt_to_dict(a) for a in self.global_history],
                    "counters": {
                        "total_attempts_started": self.total_attempts_started,
                        "total_attempts_finalized": self.total_attempts_finalized,
                        "total_expired_attempts": self.total_expired_attempts,
                    },
                }

            with open(self.persist_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

            self.last_save_ts = time.time()
            self.updated_at = time.time()
        except Exception as e:
            self.total_persist_failures += 1
            raise e

    def load_state(self) -> None:
        if not self.persist_path or not os.path.exists(self.persist_path):
            return

        with open(self.persist_path, "r", encoding="utf-8") as f:
            payload = json.load(f)

        with self.lock:
            self.rolling_window = int(payload.get("rolling_window", self.rolling_window))
            self.history_by_symbol = defaultdict(lambda: deque(maxlen=self.rolling_window))
            self.global_history = deque(maxlen=self.rolling_window * 5)

            raw_history_by_symbol = payload.get("history_by_symbol", {})
            for symbol, attempts in raw_history_by_symbol.items():
                dq = deque(maxlen=self.rolling_window)
                for item in attempts:
                    dq.append(self._attempt_from_dict(item))
                self.history_by_symbol[symbol] = dq

            for item in payload.get("global_history", []):
                self.global_history.append(self._attempt_from_dict(item))

            counters = payload.get("counters", {}) or {}
            self.total_attempts_started = int(counters.get("total_attempts_started", self.total_attempts_started))
            self.total_attempts_finalized = int(counters.get("total_attempts_finalized", self.total_attempts_finalized))
            self.total_expired_attempts = int(counters.get("total_expired_attempts", self.total_expired_attempts))
            self.last_save_ts = float(payload.get("last_save_ts", time.time()))
            self.updated_at = time.time()

    # -------------------------------------------------------------------------
    # Internal
    # -------------------------------------------------------------------------

    def _finalize_attempt_locked(self, attempt_id: str) -> None:
        attempt = self.active_attempts.pop(attempt_id, None)
        if attempt is None:
            return

        self._append_history_locked(attempt)
        self.total_attempts_finalized += 1
        self.updated_at = time.time()
        self._autosave_if_needed_locked()

        self.logger.debug(
            "[ExecutionQualityMonitor] finalize_attempt | id=%s symbol=%s status=%s fill_ratio=%.4f slippage_bps=%s completion_ms=%s",
            attempt.attempt_id,
            attempt.symbol,
            attempt.status,
            attempt.fill_ratio,
            self._calc_slippage_bps(attempt),
            attempt.submit_to_complete_ms,
        )

    def _append_history_locked(self, attempt: ExecutionAttempt) -> None:
        self.history_by_symbol[attempt.symbol].append(attempt)
        self.global_history.append(attempt)

    def _autosave_if_needed_locked(self) -> None:
        if not self.persist_path:
            return
        now = time.time()
        if (now - self.last_save_ts) >= self.autosave_interval_sec:
            try:
                self.save_state()
            except Exception as e:
                self.total_persist_failures += 1
                self.logger.exception("[ExecutionQualityMonitor] autosave failed: %s", e)

    def _expire_stale_if_needed(self, now: Optional[float] = None):
        now = now or time.time()
        if (now - self.last_expire_check_ts) < 1.0:
            return
        self.last_expire_check_ts = now
        self._force_expire_stale_attempts_locked(self.stale_attempt_expire_sec)

    def _force_expire_stale_attempts_locked(self, stale_after_sec: float) -> int:
        now = time.time()
        expired_ids = []
        for attempt_id, attempt in list(self.active_attempts.items()):
            if now - attempt.created_ts >= stale_after_sec:
                expired_ids.append(attempt_id)

        for attempt_id in expired_ids:
            attempt = self.active_attempts.get(attempt_id)
            if attempt is None:
                continue
            attempt.status = "EXPIRED"
            attempt.completed_ts = now
            self._finalize_attempt_locked(attempt_id)
            self.total_expired_attempts += 1

        if expired_ids:
            self.logger.warning(
                "[ExecutionQualityMonitor] force_expire_stale_attempts | expired=%d stale_after_sec=%.1f",
                len(expired_ids),
                stale_after_sec,
            )
        return len(expired_ids)

    def _build_snapshot(self, symbol: str, history: List[ExecutionAttempt]) -> Dict[str, Any]:
        if not history:
            return {
                "symbol": symbol,
                "sample_size": 0,
                "filled_count": 0,
                "partial_or_better_count": 0,
                "cancel_count": 0,
                "rejection_count": 0,
                "failure_count": 0,
                "avg_fill_ratio": None,
                "median_fill_ratio": None,
                "avg_slippage_bps": None,
                "median_slippage_bps": None,
                "avg_ack_ms": None,
                "avg_completion_ms": None,
                "median_completion_ms": None,
                "avg_spread_bps_at_submit": None,
                "cancel_rate": None,
                "rejection_rate": None,
                "failure_rate": None,
                "poor_fill_rate": None,
            }

        fill_ratios = []
        slippages_bps = []
        ack_ms = []
        completion_ms = []
        spreads_bps = []

        filled_count = 0
        partial_or_better_count = 0
        cancel_count = 0
        rejection_count = 0
        failure_count = 0
        poor_fill_count = 0

        for a in history:
            fill_ratios.append(a.fill_ratio)

            if a.fill_ratio >= 1.0:
                filled_count += 1
            if a.fill_ratio > 0.0:
                partial_or_better_count += 1
            if a.fill_ratio < self.poor_fill_ratio_threshold:
                poor_fill_count += 1

            if a.status == "CANCELED":
                cancel_count += 1
            elif a.status == "REJECTED":
                rejection_count += 1
            elif a.status == "FAILED":
                failure_count += 1

            slippage = self._calc_slippage_bps(a)
            if slippage is not None:
                slippages_bps.append(slippage)

            if a.submit_to_ack_ms is not None:
                ack_ms.append(a.submit_to_ack_ms)
            if a.submit_to_complete_ms is not None:
                completion_ms.append(a.submit_to_complete_ms)
            if a.spread_bps_at_submit is not None:
                spreads_bps.append(a.spread_bps_at_submit)

        sample_size = len(history)
        return {
            "symbol": symbol,
            "sample_size": sample_size,
            "filled_count": filled_count,
            "partial_or_better_count": partial_or_better_count,
            "cancel_count": cancel_count,
            "rejection_count": rejection_count,
            "failure_count": failure_count,
            "avg_fill_ratio": self._safe_mean(fill_ratios),
            "median_fill_ratio": self._safe_median(fill_ratios),
            "avg_slippage_bps": self._safe_mean(slippages_bps),
            "median_slippage_bps": self._safe_median(slippages_bps),
            "avg_ack_ms": self._safe_mean(ack_ms),
            "avg_completion_ms": self._safe_mean(completion_ms),
            "median_completion_ms": self._safe_median(completion_ms),
            "avg_spread_bps_at_submit": self._safe_mean(spreads_bps),
            "cancel_rate": cancel_count / sample_size if sample_size > 0 else None,
            "rejection_rate": rejection_count / sample_size if sample_size > 0 else None,
            "failure_rate": failure_count / sample_size if sample_size > 0 else None,
            "poor_fill_rate": poor_fill_count / sample_size if sample_size > 0 else None,
        }

    @staticmethod
    def _calc_spread_bps(bid_price: Optional[float], ask_price: Optional[float]) -> Optional[float]:
        if bid_price is None or ask_price is None:
            return None
        if bid_price <= 0 or ask_price <= 0:
            return None
        mid = (bid_price + ask_price) / 2.0
        if mid <= 0:
            return None
        spread = ask_price - bid_price
        return (spread / mid) * 10000.0

    @staticmethod
    def _safe_mean(values: List[float]) -> Optional[float]:
        clean = [v for v in values if v is not None and math.isfinite(v)]
        if not clean:
            return None
        return float(statistics.fmean(clean))

    @staticmethod
    def _safe_median(values: List[float]) -> Optional[float]:
        clean = [v for v in values if v is not None and math.isfinite(v)]
        if not clean:
            return None
        return float(statistics.median(clean))

    @staticmethod
    def _attempt_to_dict(a: ExecutionAttempt) -> Dict[str, Any]:
        return asdict(a)

    @staticmethod
    def _attempt_from_dict(data: Dict[str, Any]) -> ExecutionAttempt:
        fills = [FillEvent(**f) for f in data.get("fills", [])]
        return ExecutionAttempt(
            attempt_id=data["attempt_id"],
            symbol=data["symbol"],
            side=data["side"],
            order_type=data["order_type"],
            requested_qty=float(data["requested_qty"]),
            intended_price=data.get("intended_price"),
            bid_price=data.get("bid_price"),
            ask_price=data.get("ask_price"),
            spread_bps_at_submit=data.get("spread_bps_at_submit"),
            created_ts=float(data["created_ts"]),
            meta=data.get("meta", {}),
            ack_ts=data.get("ack_ts"),
            completed_ts=data.get("completed_ts"),
            status=data.get("status", "NEW"),
            fills=fills,
            canceled_reason=data.get("canceled_reason"),
            rejected_reason=data.get("rejected_reason"),
            exchange_order_id=data.get("exchange_order_id"),
        )

    def _calc_slippage_bps(self, attempt: ExecutionAttempt) -> Optional[float]:
        avg_fill = attempt.weighted_avg_fill_price
        intended = attempt.intended_price
        if avg_fill is None or intended is None or intended <= 0:
            return None

        if attempt.side == "BUY":
            slip = (avg_fill - intended) / intended * 10000.0
        else:
            slip = (intended - avg_fill) / intended * 10000.0
        return float(slip)

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "rolling_window": self.rolling_window,
                "persist_path": self.persist_path,
                "autosave_interval_sec": self.autosave_interval_sec,
                "stale_attempt_expire_sec": self.stale_attempt_expire_sec,
                "active_attempt_count": len(self.active_attempts),
                "tracked_symbol_count": len(self.history_by_symbol),
                "global_history_size": len(self.global_history),
                "total_attempts_started": self.total_attempts_started,
                "total_attempts_finalized": self.total_attempts_finalized,
                "total_expired_attempts": self.total_expired_attempts,
                "total_persist_failures": self.total_persist_failures,
                "last_save_ts": self.last_save_ts,
                "last_expire_check_ts": self.last_expire_check_ts,
                "updated_at": self.updated_at,
            }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    monitor = ExecutionQualityMonitor(
        rolling_window=100,
        persist_path=None,
    )

    attempt_id = monitor.start_attempt(
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        requested_qty=0.01,
        intended_price=65000.0,
        bid_price=64999.5,
        ask_price=65000.5,
        meta={"source": "demo"},
    )
    monitor.mark_acknowledged(attempt_id, exchange_order_id="12345")
    monitor.record_fill(attempt_id, fill_qty=0.004, fill_price=65000.2)
    monitor.record_fill(attempt_id, fill_qty=0.006, fill_price=65000.3, cumulative_done=True)

    attempt_id2 = monitor.start_attempt(
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        requested_qty=0.02,
        intended_price=65100.0,
        bid_price=65099.5,
        ask_price=65100.5,
    )
    monitor.mark_acknowledged(attempt_id2)
    monitor.record_fill(attempt_id2, fill_qty=0.02, fill_price=65098.8, cumulative_done=True)

    attempt_id3 = monitor.start_attempt(
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        requested_qty=0.03,
        intended_price=64980.0,
        bid_price=64979.8,
        ask_price=64980.2,
    )
    monitor.mark_acknowledged(attempt_id3)
    monitor.mark_canceled(attempt_id3, reason="timeout")

    print("\n[SYMBOL SNAPSHOT]")
    print(json.dumps(monitor.get_symbol_snapshot("BTCUSDT"), indent=2, ensure_ascii=False))

    print("\n[GLOBAL HEALTH]")
    print(json.dumps(monitor.get_health_flag(), indent=2, ensure_ascii=False))

    print("\n[EXECUTION STYLE RECOMMENDATION]")
    print(json.dumps(
        monitor.recommend_execution_style(
            symbol="BTCUSDT",
            current_spread_bps=4.2,
            current_volatility_score=0.72,
            current_book_pressure=0.35,
        ),
        indent=2,
        ensure_ascii=False
    ))
