# 🔥 FINAL INSTITUTIONAL-GRADE POSITION ENGINE (FULL FEATURES PRESERVED + ENHANCED)
# ✔ 기존 구조 유지 (삭제 없음)
# ✔ 모든 기능 유지 + 보강
# ✔ 거래 정상화 + 수익 구조 유지
# ✔ 한글 설명 포함 (이해 + 유지보수 용이)

import copy
import logging
import threading
import time
from typing import Dict, Optional, Any

from config import (
    MAX_POSITIONS,
    CAPITAL_PER_TRADE,
    LEVERAGE,
    STOP_LOSS,
    TAKE_PROFIT,
)


class PositionEngine:
    """
    🔥 기관급 포지션 엔진 (최종 완성형)

    설계 목표
    1) 기존 포지션 엔진의 핵심 구조는 유지
    2) 유령 포지션 / stale lock / sync 꼬임 제거
    3) split 주문 구조와 충돌 완화
    4) 거래소 실제 상태와 로컬 상태 불일치 최소화
    5) 기존 수익 구조(트레일링, 부분익절, 손절/익절)는 유지
    6) 너무 빡빡해서 거래가 안 되는 구조는 피하고 실전형으로 유지
    """

    MIN_POSITION_EPSILON = 1e-8

    def __init__(self):
        # ================= 자본 / 리스크 =================
        self.max_positions = int(MAX_POSITIONS)
        self.risk_per_trade = float(CAPITAL_PER_TRADE)

        # ================= 수익 구조 =================
        self.trailing_trigger = 0.004
        self.trailing_distance = 0.002
        self.partial_tp = 0.006
        self.partial_close_ratio = 0.5

        # ================= 거래 밸런스 (핵심 튜닝) =================
        self.min_hold_seconds = 8.0
        self.reentry_cooldown_seconds = 18.0
        self.flip_cooldown_seconds = 28.0

        # ================= 실행 안정성 =================
        self.execution_lock_seconds = 1.2
        self.stale_lock_max_seconds = 6.0
        self.min_position_size = 0.001

        # ================= 상태 =================
        self.positions: Dict[str, Dict[str, Any]] = {}
        self.closed_history: list[Dict[str, Any]] = []

        self.last_closed_ts: Dict[str, float] = {}
        self.last_closed_side: Dict[str, str] = {}
        self.last_close_reason: Dict[str, str] = {}

        self.pending_close_until: Dict[str, float] = {}
        self.pending_partial_until: Dict[str, float] = {}
        self.pending_open_until: Dict[str, float] = {}

        self.lock = threading.RLock()
        self.updated_at = time.time()

    # ================= INTERNAL =================
    def _now(self):
        return time.time()

    def _touch(self):
        self.updated_at = self._now()

    def _safe_float(self, value, default=0.0):
        try:
            return float(value)
        except Exception:
            return default

    def _normalize_symbol(self, symbol):
        return str(symbol).upper().strip()

    def _normalize_side(self, side):
        s = str(side).upper().strip()
        if s in ("BUY", "LONG"):
            return "BUY"
        if s in ("SELL", "SHORT"):
            return "SELL"
        return s

    def _is_locked(self, lock_dict, symbol):
        return self._now() < self._safe_float(lock_dict.get(symbol), 0.0)

    def _lock(self, lock_dict, symbol, seconds):
        lock_dict[symbol] = self._now() + max(0.0, self._safe_float(seconds, 0.0))
        self._touch()

    def _clear_locks(self, symbol):
        self.pending_close_until.pop(symbol, None)
        self.pending_partial_until.pop(symbol, None)
        self.pending_open_until.pop(symbol, None)
        self._touch()

    def _cleanup_stale_locks(self):
        """
        lock_dict 안 값은 '락 만료 시각'이다.
        만료된 락은 제거하면 충분하다.
        """
        now = self._now()
        for d in (self.pending_close_until, self.pending_partial_until, self.pending_open_until):
            for k in list(d.keys()):
                try:
                    until_ts = float(d.get(k, 0.0))
                except Exception:
                    until_ts = 0.0
                if now >= until_ts:
                    d.pop(k, None)

    def _cleanup_ghost_positions(self):
        """
        0 또는 비정상 size 포지션 제거.
        """
        removed = []
        for symbol in list(self.positions.keys()):
            pos = self.positions.get(symbol) or {}
            size = self._safe_float(pos.get("size"), 0.0)
            if size <= self.MIN_POSITION_EPSILON:
                removed.append(symbol)
                self.positions.pop(symbol, None)
                self._clear_locks(symbol)
        if removed:
            logging.warning(f"[POSITION] ghost positions cleaned: {removed}")
            self._touch()

    def _record_close(self, symbol, side=None, reason="manual"):
        now = self._now()
        if side:
            self.last_closed_side[symbol] = side
        self.last_closed_ts[symbol] = now
        self.last_close_reason[symbol] = reason
        self.closed_history.append({
            "symbol": symbol,
            "side": side,
            "reason": reason,
            "ts": now,
        })
        if len(self.closed_history) > 2000:
            self.closed_history = self.closed_history[-1000:]
        self._touch()

    # ================= QUERY =================
    def position_count(self):
        with self.lock:
            self._cleanup_stale_locks()
            self._cleanup_ghost_positions()
            return len(self.positions)

    def has_position(self, symbol):
        symbol = self._normalize_symbol(symbol)
        with self.lock:
            self._cleanup_stale_locks()
            self._cleanup_ghost_positions()
            return symbol in self.positions

    def get_position(self, symbol) -> Optional[Dict[str, Any]]:
        symbol = self._normalize_symbol(symbol)
        with self.lock:
            self._cleanup_stale_locks()
            self._cleanup_ghost_positions()
            pos = self.positions.get(symbol)
            return copy.deepcopy(pos) if pos else None

    def get_all_positions(self):
        with self.lock:
            self._cleanup_stale_locks()
            self._cleanup_ghost_positions()
            return copy.deepcopy(self.positions)

    # ================= POSITION LIMIT =================
    def can_open(self, symbol, side=None):
        symbol = self._normalize_symbol(symbol)
        side = self._normalize_side(side) if side else None

        with self.lock:
            self._cleanup_stale_locks()
            self._cleanup_ghost_positions()
            now = self._now()

            existing = self.positions.get(symbol)
            if existing:
                size = self._safe_float(existing.get("size"), 0.0)
                if size > self.MIN_POSITION_EPSILON:
                    return False
                self.positions.pop(symbol, None)

            if self._is_locked(self.pending_open_until, symbol):
                return False

            if self._is_locked(self.pending_close_until, symbol):
                return False

            if len(self.positions) >= self.max_positions:
                return False

            last_ts = self.last_closed_ts.get(symbol)
            last_side = self.last_closed_side.get(symbol)

            if last_ts is not None and (now - last_ts) < self.reentry_cooldown_seconds:
                return False

            if (
                side
                and last_side
                and side != last_side
                and last_ts is not None
                and (now - last_ts) < self.flip_cooldown_seconds
            ):
                return False

            return True

    # ================= SIZE =================
    def position_size(self, balance, price):
        try:
            balance = self._safe_float(balance, 0.0)
            price = self._safe_float(price, 0.0)
            if balance <= 0.0 or price <= 0.0:
                return 0.0

            risk_amount = balance * self.risk_per_trade
            size = (risk_amount * LEVERAGE) / price
            size = round(size, 3)

            return size if size >= self.min_position_size else 0.0
        except Exception:
            return 0.0

    # ================= OPEN =================
    def open_position(self, symbol, side, price, size, source="local"):
        symbol = self._normalize_symbol(symbol)
        side = self._normalize_side(side)
        price = self._safe_float(price, 0.0)
        size = self._safe_float(size, 0.0)

        with self.lock:
            self._cleanup_stale_locks()
            self._cleanup_ghost_positions()

            if size <= self.MIN_POSITION_EPSILON:
                return False

            if not self.can_open(symbol, side):
                return False

            self._lock(self.pending_open_until, symbol, self.execution_lock_seconds)

            self.positions[symbol] = {
                "symbol": symbol,
                "side": side,
                "entry": price,
                "size": size,
                "opened_at": self._now(),
                "highest": price,
                "lowest": price,
                "partial_taken": False,
                "source": source,
            }
            self._touch()
            return True

    # ================= UPDATE =================
    def update(self, symbol, price):
        symbol = self._normalize_symbol(symbol)
        price = self._safe_float(price, 0.0)

        with self.lock:
            self._cleanup_stale_locks()
            self._cleanup_ghost_positions()

            if symbol not in self.positions:
                return None

            pos = self.positions[symbol]
            entry = self._safe_float(pos.get("entry"), 0.0)
            side = pos.get("side")
            size = self._safe_float(pos.get("size"), 0.0)
            opened_at = self._safe_float(pos.get("opened_at"), self._now())

            if size <= self.MIN_POSITION_EPSILON:
                return {"action": "CLOSE", "reason": "zero_cleanup"}

            if price <= 0.0 or entry <= 0.0:
                return None

            if side == "BUY":
                profit = (price - entry) / entry
                pos["highest"] = max(self._safe_float(pos.get("highest"), price), price)
            else:
                profit = (entry - price) / entry
                lowest = self._safe_float(pos.get("lowest"), price)
                if lowest <= 0.0:
                    lowest = price
                pos["lowest"] = min(lowest, price)

            held_seconds = self._now() - opened_at
            min_hold_ok = held_seconds >= self.min_hold_seconds

            if min_hold_ok and profit <= -STOP_LOSS:
                return {"action": "CLOSE", "reason": "SL"}

            if min_hold_ok and profit >= TAKE_PROFIT:
                return {"action": "CLOSE", "reason": "TP"}

            if profit >= self.partial_tp and not bool(pos.get("partial_taken", False)):
                pos["partial_taken"] = True
                self._lock(self.pending_partial_until, symbol, self.execution_lock_seconds)
                return {
                    "action": "PARTIAL_CLOSE",
                    "size": max(size * self.partial_close_ratio, 0.0),
                }

            if profit > self.trailing_trigger and min_hold_ok:
                if side == "BUY":
                    highest = self._safe_float(pos.get("highest"), price)
                    if highest > 0.0 and price < highest * (1 - self.trailing_distance):
                        return {"action": "CLOSE", "reason": "TRAIL"}
                elif side == "SELL":
                    lowest = self._safe_float(pos.get("lowest"), price)
                    if lowest > 0.0 and price > lowest * (1 + self.trailing_distance):
                        return {"action": "CLOSE", "reason": "TRAIL"}

            return None

    # ================= CLOSE =================
    def close(self, symbol, reason="manual"):
        symbol = self._normalize_symbol(symbol)

        with self.lock:
            self._cleanup_stale_locks()

            if symbol not in self.positions:
                self._clear_locks(symbol)
                return False

            pos = self.positions.get(symbol, {})
            side = pos.get("side")

            self.positions.pop(symbol, None)
            self._clear_locks(symbol)
            self._record_close(symbol, side=side, reason=reason)
            return True

    # ================= PARTIAL =================
    def apply_partial_close(self, symbol, closed_size):
        symbol = self._normalize_symbol(symbol)
        closed_size = self._safe_float(closed_size, 0.0)

        with self.lock:
            self._cleanup_stale_locks()

            if symbol not in self.positions:
                self._clear_locks(symbol)
                return False

            pos = self.positions[symbol]
            cur_size = self._safe_float(pos.get("size"), 0.0)
            new_size = max(cur_size - closed_size, 0.0)

            if new_size <= self.MIN_POSITION_EPSILON:
                side = pos.get("side")
                self.positions.pop(symbol, None)
                self._clear_locks(symbol)
                self._record_close(symbol, side=side, reason="partial_to_zero")
                return True

            pos["size"] = new_size
            self.pending_partial_until.pop(symbol, None)
            self._touch()
            return True

    # ================= EXTERNAL LOCK CONTROL =================
    def mark_open_pending(self, symbol, seconds=None):
        symbol = self._normalize_symbol(symbol)
        seconds = self.execution_lock_seconds if seconds is None else seconds
        with self.lock:
            self._lock(self.pending_open_until, symbol, seconds)
            return True

    def mark_close_pending(self, symbol, seconds=None):
        symbol = self._normalize_symbol(symbol)
        seconds = self.execution_lock_seconds if seconds is None else seconds
        with self.lock:
            self._lock(self.pending_close_until, symbol, seconds)
            return True

    def release_symbol(self, symbol):
        symbol = self._normalize_symbol(symbol)
        with self.lock:
            self._clear_locks(symbol)
            return True

    # ================= SYNC =================
    def sync_from_exchange_position(self, symbol, side=None, entry=None, size=None):
        symbol = self._normalize_symbol(symbol)
        side = self._normalize_side(side) if side else None
        entry = self._safe_float(entry, 0.0)
        size = self._safe_float(size, 0.0)

        with self.lock:
            self._cleanup_stale_locks()

            if size <= self.MIN_POSITION_EPSILON:
                if symbol in self.positions:
                    old_side = self.positions[symbol].get("side")
                    self.positions.pop(symbol, None)
                    self._clear_locks(symbol)
                    self._record_close(symbol, side=old_side, reason="exchange_zero")
                    return True

                self._clear_locks(symbol)
                return False

            existing = self.positions.get(symbol)
            partial_taken = False
            opened_at = self._now()

            if existing:
                partial_taken = bool(existing.get("partial_taken", False))
                opened_at = self._safe_float(existing.get("opened_at"), self._now())

            self.positions[symbol] = {
                "symbol": symbol,
                "side": side,
                "entry": entry,
                "size": size,
                "opened_at": opened_at,
                "highest": entry,
                "lowest": entry,
                "partial_taken": partial_taken,
                "source": "exchange",
            }

            self.pending_open_until.pop(symbol, None)
            self.pending_close_until.pop(symbol, None)
            self.pending_partial_until.pop(symbol, None)
            self._touch()
            return True

    async def sync(self, *args, **kwargs):
        with self.lock:
            self._cleanup_stale_locks()
            self._cleanup_ghost_positions()
            return copy.deepcopy(self.positions)