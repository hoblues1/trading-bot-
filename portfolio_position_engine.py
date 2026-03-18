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
    Institutional / hedge-fund grade position engine

    설계 목표:
    1) 같은 심볼 중복 포지션 방지
    2) 청산 직후 재진입 쿨다운
    3) 반대 방향 즉시 플립 방지
    4) 최소 보유 시간 보장
    5) partial close 후 size/state/flags 정상 반영
    6) close / partial / open 중복 호출 방지
    7) 실전용 상태 조회 / pending 상태 조회 제공
    8) executor / router 실수에도 최대한 방어
    9) 거래소 sync 보조 메서드 강화
    10) open/close/partial 요청 상태 추적
    11) state corruption 최소화를 위한 lock 보호
    12) tiny remainder 자동 정리
    13) trailing / TP / SL / partial 우선순위 명확화
    14) stale pending lock 자동 해제
    15) exchange sync overwrite 지원
    16) action decision 결과 표준화
    17) 장기 운용용 snapshot / stats 제공
    18) close 이후 history / reason / hold-time 추적
    """

    MIN_POSITION_EPSILON = 1e-8

    def __init__(self):
        self.max_positions = int(MAX_POSITIONS)
        self.risk_per_trade = float(CAPITAL_PER_TRADE)

        self.trailing_trigger = 0.004
        self.trailing_distance = 0.002
        self.partial_tp = 0.006
        self.partial_close_ratio = 0.5

        self.min_hold_seconds = 20.0
        self.reentry_cooldown_seconds = 120.0
        self.flip_cooldown_seconds = 180.0
        self.execution_lock_seconds = 3.0
        self.stale_lock_max_seconds = 12.0
        self.min_position_size = 0.001

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
    def _now(self) -> float:
        return time.time()

    def _touch(self):
        self.updated_at = self._now()

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return float(default)
            return float(value)
        except Exception:
            return float(default)

    def _normalize_symbol(self, symbol: str) -> str:
        return str(symbol).upper().strip()

    def _normalize_side(self, side: str) -> str:
        s = str(side).upper().strip()
        if s in ("BUY", "LONG"):
            return "BUY"
        if s in ("SELL", "SHORT"):
            return "SELL"
        return s

    def _is_locked(self, lock_dict: Dict[str, float], symbol: str) -> bool:
        until = self._safe_float(lock_dict.get(symbol), 0.0)
        return self._now() < until

    def _lock(self, lock_dict: Dict[str, float], symbol: str, seconds: float):
        lock_dict[symbol] = self._now() + max(0.0, self._safe_float(seconds, 0.0))

    def _unlock(self, lock_dict: Dict[str, float], symbol: str):
        lock_dict.pop(symbol, None)

    def _clear_locks(self, symbol: str):
        self.pending_close_until.pop(symbol, None)
        self.pending_partial_until.pop(symbol, None)
        self.pending_open_until.pop(symbol, None)

    def _cleanup_stale_locks(self, symbol: Optional[str] = None):
        now = self._now()
        targets = [self._normalize_symbol(symbol)] if symbol else None

        for lock_dict in (self.pending_close_until, self.pending_partial_until, self.pending_open_until):
            keys = targets if targets is not None else list(lock_dict.keys())
            for key in list(keys):
                until = self._safe_float(lock_dict.get(key), 0.0)
                if until <= 0 or now >= until + self.stale_lock_max_seconds:
                    lock_dict.pop(key, None)

    def _build_action_response(self, symbol: str, action: str, reason: str, profit: Optional[float] = None, size: Optional[float] = None):
        payload = {
            "symbol": symbol,
            "action": action,
            "reason": reason,
        }
        if profit is not None:
            payload["profit"] = float(profit)
        if size is not None:
            payload["size"] = float(size)
        return payload

    def _new_position_dict(self, symbol: str, side: str, entry: float, size: float, now: Optional[float] = None) -> Dict[str, Any]:
        ts = self._safe_float(now, self._now())
        return {
            "symbol": symbol,
            "side": side,
            "entry": entry,
            "size": round(size, 6),
            "initial_size": round(size, 6),
            "opened_at": ts,
            "updated_at": ts,
            "highest": entry,
            "lowest": entry,
            "partial_taken": False,
            "partial_count": 0,
            "close_requested": False,
            "close_reason": None,
            "last_partial_at": None,
            "unrealized_profit": 0.0,
            "close_request_count": 0,
            "close_attempts": 0,
            "partial_attempts": 0,
            "open_source": "local",
        }

    def _record_close_history(self, symbol: str, pos: Dict[str, Any], reason: str):
        now = self._now()
        opened_at = self._safe_float(pos.get("opened_at"), now)
        self.closed_history.append({
            "symbol": symbol,
            "side": pos.get("side"),
            "entry": self._safe_float(pos.get("entry"), 0.0),
            "final_size": self._safe_float(pos.get("size"), 0.0),
            "initial_size": self._safe_float(pos.get("initial_size"), 0.0),
            "opened_at": opened_at,
            "closed_at": now,
            "hold_seconds": max(0.0, now - opened_at),
            "reason": reason,
            "partial_count": int(pos.get("partial_count", 0)),
            "close_request_count": int(pos.get("close_request_count", 0)),
            "unrealized_profit": self._safe_float(pos.get("unrealized_profit"), 0.0),
        })

    # ================= QUERY =================
    def has_position(self, symbol: str) -> bool:
        symbol = self._normalize_symbol(symbol)
        with self.lock:
            return symbol in self.positions

    def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        symbol = self._normalize_symbol(symbol)
        with self.lock:
            pos = self.positions.get(symbol)
            return copy.deepcopy(pos) if pos else None

    def get_side(self, symbol: str) -> Optional[str]:
        pos = self.get_position(symbol)
        if not pos:
            return None
        return pos.get("side")

    def is_same_side(self, symbol: str, side: str) -> bool:
        pos = self.get_position(symbol)
        if not pos:
            return False
        return pos.get("side") == self._normalize_side(side)

    def is_opposite_side(self, symbol: str, side: str) -> bool:
        pos = self.get_position(symbol)
        if not pos:
            return False
        return pos.get("side") != self._normalize_side(side)

    def position_count(self) -> int:
        with self.lock:
            return len(self.positions)

    def symbols(self):
        with self.lock:
            return list(self.positions.keys())

    def is_close_pending(self, symbol: str) -> bool:
        symbol = self._normalize_symbol(symbol)
        with self.lock:
            self._cleanup_stale_locks(symbol)
            return self._is_locked(self.pending_close_until, symbol)

    def is_partial_pending(self, symbol: str) -> bool:
        symbol = self._normalize_symbol(symbol)
        with self.lock:
            self._cleanup_stale_locks(symbol)
            return self._is_locked(self.pending_partial_until, symbol)

    def is_open_pending(self, symbol: str) -> bool:
        symbol = self._normalize_symbol(symbol)
        with self.lock:
            self._cleanup_stale_locks(symbol)
            return self._is_locked(self.pending_open_until, symbol)

    # ================= POSITION LIMIT =================
    def can_open(self, symbol, side=None):
        symbol = self._normalize_symbol(symbol)
        normalized_side = self._normalize_side(side) if side else None

        with self.lock:
            self._cleanup_stale_locks(symbol)
            now = self._now()

            if symbol in self.positions:
                return False
            if self._is_locked(self.pending_open_until, symbol):
                return False
            if self._is_locked(self.pending_close_until, symbol):
                return False
            if len(self.positions) >= self.max_positions:
                return False

            last_closed_ts = self.last_closed_ts.get(symbol)
            last_closed_side = self.last_closed_side.get(symbol)

            if last_closed_ts is not None and (now - last_closed_ts) < self.reentry_cooldown_seconds:
                return False

            if (
                normalized_side is not None
                and last_closed_ts is not None
                and last_closed_side is not None
                and normalized_side != last_closed_side
                and (now - last_closed_ts) < self.flip_cooldown_seconds
            ):
                return False

            return True

    # ================= POSITION SIZE =================
    def position_size(self, balance, price):
        try:
            balance = float(balance)
            price = float(price)
            if balance <= 0 or price <= 0:
                return 0.0

            risk_amount = balance * self.risk_per_trade
            size = (risk_amount * LEVERAGE) / price
            size = round(max(size, 0.0), 3)
            if size < self.min_position_size:
                return 0.0
            return size

        except Exception as e:
            logging.error(f"Position size error: {e}")
            return 0.0

    # ================= OPEN POSITION =================
    def open_position(self, symbol, side, price, size, source: str = "local"):
        try:
            symbol = self._normalize_symbol(symbol)
            side = self._normalize_side(side)
            price = float(price)
            size = float(size)

            if side not in ("BUY", "SELL"):
                logging.warning(f"Invalid side on open_position | {symbol} | side={side}")
                return False
            if price <= 0 or size < self.min_position_size:
                logging.warning(f"Invalid open args | {symbol} | price={price} | size={size}")
                return False

            with self.lock:
                self._cleanup_stale_locks(symbol)
                if not self.can_open(symbol, side):
                    logging.info(f"OPEN BLOCKED | {symbol} | {side}")
                    return False

                now = self._now()
                self._lock(self.pending_open_until, symbol, self.execution_lock_seconds)

                pos = self._new_position_dict(symbol, side, price, size, now=now)
                pos["open_source"] = str(source or "local")
                self.positions[symbol] = pos

                self.pending_open_until.pop(symbol, None)
                self.pending_close_until.pop(symbol, None)
                self.pending_partial_until.pop(symbol, None)
                self._touch()

            logging.info(f"POSITION OPENED | {symbol} | {side} | entry={price} | size={size}")
            return True

        except Exception as e:
            logging.error(f"Open position error: {e}")
            with self.lock:
                self.pending_open_until.pop(self._normalize_symbol(symbol), None)
            return False

    # ================= UPDATE =================
    def update(self, symbol, price):
        try:
            symbol = self._normalize_symbol(symbol)
            price = float(price)
            if price <= 0:
                return None

            with self.lock:
                self._cleanup_stale_locks(symbol)
                if symbol not in self.positions:
                    return None

                pos = self.positions[symbol]
                now = self._now()
                entry = self._safe_float(pos.get("entry"), 0.0)
                side = pos.get("side")
                size = self._safe_float(pos.get("size"), 0.0)
                hold_time = now - self._safe_float(pos.get("opened_at"), now)

                if size < self.min_position_size:
                    return self._build_action_response(symbol, "CLOSE", "size_too_small")

                if side == "BUY":
                    pos["highest"] = max(self._safe_float(pos.get("highest"), price), price)
                    pos["lowest"] = min(self._safe_float(pos.get("lowest"), price), price)
                    profit = (price - entry) / entry if entry > 0 else 0.0
                else:
                    pos["highest"] = max(self._safe_float(pos.get("highest"), price), price)
                    pos["lowest"] = min(self._safe_float(pos.get("lowest"), price), price)
                    profit = (entry - price) / entry if entry > 0 else 0.0

                pos["updated_at"] = now
                pos["unrealized_profit"] = profit
                allow_active_management = hold_time >= self.min_hold_seconds

                if profit <= -float(STOP_LOSS):
                    if not self._is_locked(self.pending_close_until, symbol):
                        self._lock(self.pending_close_until, symbol, self.execution_lock_seconds)
                        pos["close_requested"] = True
                        pos["close_reason"] = "stop_loss"
                        pos["close_request_count"] = int(pos.get("close_request_count", 0)) + 1
                        pos["close_attempts"] = int(pos.get("close_attempts", 0)) + 1
                        logging.warning(f"STOP LOSS | {symbol} | profit={profit:.6f}")
                        return self._build_action_response(symbol, "CLOSE", "stop_loss", profit=profit)

                if allow_active_management and profit >= float(TAKE_PROFIT):
                    if not self._is_locked(self.pending_close_until, symbol):
                        self._lock(self.pending_close_until, symbol, self.execution_lock_seconds)
                        pos["close_requested"] = True
                        pos["close_reason"] = "take_profit"
                        pos["close_request_count"] = int(pos.get("close_request_count", 0)) + 1
                        pos["close_attempts"] = int(pos.get("close_attempts", 0)) + 1
                        logging.info(f"TAKE PROFIT | {symbol} | profit={profit:.6f}")
                        return self._build_action_response(symbol, "CLOSE", "take_profit", profit=profit)

                if allow_active_management and profit >= self.partial_tp and not bool(pos.get("partial_taken", False)):
                    if not self._is_locked(self.pending_partial_until, symbol):
                        partial_size = round(size * self.partial_close_ratio, 6)
                        if partial_size >= self.min_position_size:
                            self._lock(self.pending_partial_until, symbol, self.execution_lock_seconds)
                            pos["partial_attempts"] = int(pos.get("partial_attempts", 0)) + 1
                            logging.info(f"PARTIAL TP | {symbol} | size={partial_size} | profit={profit:.6f}")
                            return self._build_action_response(symbol, "PARTIAL_CLOSE", "partial_tp", profit=profit, size=partial_size)

                if allow_active_management and profit >= self.trailing_trigger:
                    if side == "BUY":
                        stop = self._safe_float(pos.get("highest"), price) * (1 - self.trailing_distance)
                        if price <= stop and not self._is_locked(self.pending_close_until, symbol):
                            self._lock(self.pending_close_until, symbol, self.execution_lock_seconds)
                            pos["close_requested"] = True
                            pos["close_reason"] = "trailing_stop"
                            pos["close_request_count"] = int(pos.get("close_request_count", 0)) + 1
                            pos["close_attempts"] = int(pos.get("close_attempts", 0)) + 1
                            logging.info(f"TRAIL STOP | {symbol} | BUY | price={price} | stop={stop}")
                            return self._build_action_response(symbol, "CLOSE", "trailing_stop", profit=profit)
                    else:
                        stop = self._safe_float(pos.get("lowest"), price) * (1 + self.trailing_distance)
                        if price >= stop and not self._is_locked(self.pending_close_until, symbol):
                            self._lock(self.pending_close_until, symbol, self.execution_lock_seconds)
                            pos["close_requested"] = True
                            pos["close_reason"] = "trailing_stop"
                            pos["close_request_count"] = int(pos.get("close_request_count", 0)) + 1
                            pos["close_attempts"] = int(pos.get("close_attempts", 0)) + 1
                            logging.info(f"TRAIL STOP | {symbol} | SELL | price={price} | stop={stop}")
                            return self._build_action_response(symbol, "CLOSE", "trailing_stop", profit=profit)

                self._touch()
                return None

        except Exception as e:
            logging.error(f"Position update error: {e}")
            return None

    # ================= APPLY PARTIAL FILL =================
    def apply_partial_close(self, symbol, closed_size):
        try:
            symbol = self._normalize_symbol(symbol)
            closed_size = float(closed_size)
            if closed_size <= 0:
                return False

            with self.lock:
                self._cleanup_stale_locks(symbol)
                if symbol not in self.positions:
                    return False

                pos = self.positions[symbol]
                current_size = self._safe_float(pos.get("size"), 0.0)
                if current_size <= 0:
                    return False

                closed_size = min(closed_size, current_size)
                remaining = round(current_size - closed_size, 6)

                pos["partial_taken"] = True
                pos["partial_count"] = int(pos.get("partial_count", 0)) + 1
                pos["last_partial_at"] = self._now()

                if remaining < self.min_position_size or remaining <= self.MIN_POSITION_EPSILON:
                    reason = "partial_left_too_small"
                    self.last_closed_ts[symbol] = self._now()
                    self.last_closed_side[symbol] = pos.get("side")
                    self.last_close_reason[symbol] = reason
                    self._record_close_history(symbol, pos, reason)
                    del self.positions[symbol]
                    self._clear_locks(symbol)
                    self._touch()
                    logging.info(f"POSITION CLOSED BY PARTIAL REMAINDER | {symbol} | closed={closed_size}")
                    return True

                pos["size"] = remaining
                pos["updated_at"] = self._now()
                self.pending_partial_until.pop(symbol, None)
                self._touch()

            logging.info(f"PARTIAL APPLIED | {symbol} | closed={closed_size} | remaining={remaining}")
            return True

        except Exception as e:
            logging.error(f"Apply partial close error: {e}")
            return False

    def cancel_pending_close(self, symbol: str):
        symbol = self._normalize_symbol(symbol)
        with self.lock:
            self.pending_close_until.pop(symbol, None)
            if symbol in self.positions:
                self.positions[symbol]["close_requested"] = False
                self.positions[symbol]["close_reason"] = None
            self._touch()

    def cancel_pending_partial(self, symbol: str):
        symbol = self._normalize_symbol(symbol)
        with self.lock:
            self.pending_partial_until.pop(symbol, None)
            self._touch()

    def cancel_pending_open(self, symbol: str):
        symbol = self._normalize_symbol(symbol)
        with self.lock:
            self.pending_open_until.pop(symbol, None)
            self._touch()

    # ================= CLOSE =================
    def close(self, symbol, reason="manual"):
        try:
            symbol = self._normalize_symbol(symbol)
            with self.lock:
                self._cleanup_stale_locks(symbol)
                if symbol not in self.positions:
                    return False

                pos = self.positions[symbol]
                side = pos.get("side")
                self.last_closed_ts[symbol] = self._now()
                self.last_closed_side[symbol] = side
                self.last_close_reason[symbol] = str(reason)
                self._record_close_history(symbol, pos, str(reason))

                del self.positions[symbol]
                self._clear_locks(symbol)
                self._touch()

            logging.info(f"POSITION CLOSED | {symbol} | reason={reason}")
            return True

        except Exception as e:
            logging.error(f"Close position error: {e}")
            return False

    # ================= FORCE SYNC HELPERS =================
    def mark_closed_from_exchange(self, symbol, reason="exchange_sync"):
        return self.close(symbol, reason=reason)

    def mark_open_from_exchange(self, symbol, side, entry, size, overwrite: bool = True):
        try:
            symbol = self._normalize_symbol(symbol)
            side = self._normalize_side(side)
            entry = float(entry)
            size = float(size)

            if entry <= 0 or size < self.min_position_size or side not in ("BUY", "SELL"):
                return False

            with self.lock:
                self._cleanup_stale_locks(symbol)
                if symbol in self.positions and not overwrite:
                    return False

                now = self._now()
                pos = self._new_position_dict(symbol, side, entry, size, now=now)
                pos["open_source"] = "exchange_sync"
                self.positions[symbol] = pos
                self._clear_locks(symbol)
                self._touch()
            return True

        except Exception as e:
            logging.error(f"Mark open from exchange error: {e}")
            return False

    def sync_from_exchange_position(self, symbol, side=None, entry=None, size=None):
        symbol = self._normalize_symbol(symbol)

        if side is None or entry is None or size is None or self._safe_float(size, 0.0) < self.min_position_size:
            if self.has_position(symbol):
                return self.mark_closed_from_exchange(symbol, reason="exchange_position_missing")
            return False

        return self.mark_open_from_exchange(symbol, side, entry, size, overwrite=True)

    # ================= PNL =================
    def calculate_pnl(self):
        return 0

    # ================= STATS =================
    def stats(self):
        with self.lock:
            self._cleanup_stale_locks()
            return {
                "position_count": len(self.positions),
                "max_positions": self.max_positions,
                "closed_history_count": len(self.closed_history),
                "pending_open_count": len(self.pending_open_until),
                "pending_partial_count": len(self.pending_partial_until),
                "pending_close_count": len(self.pending_close_until),
                "reentry_cooldown_seconds": self.reentry_cooldown_seconds,
                "flip_cooldown_seconds": self.flip_cooldown_seconds,
                "min_hold_seconds": self.min_hold_seconds,
                "updated_at": self.updated_at,
            }

    # ================= SNAPSHOT =================
    def snapshot(self, symbol=None):
        with self.lock:
            self._cleanup_stale_locks(symbol if symbol is not None else None)
            if symbol is not None:
                symbol = self._normalize_symbol(symbol)
                pos = self.positions.get(symbol)
                return copy.deepcopy(pos) if pos else None

            return {
                "count": len(self.positions),
                "positions": copy.deepcopy(self.positions),
                "last_closed_ts": copy.deepcopy(self.last_closed_ts),
                "last_closed_side": copy.deepcopy(self.last_closed_side),
                "last_close_reason": copy.deepcopy(self.last_close_reason),
                "pending_close_until": copy.deepcopy(self.pending_close_until),
                "pending_partial_until": copy.deepcopy(self.pending_partial_until),
                "pending_open_until": copy.deepcopy(self.pending_open_until),
                "closed_history_count": len(self.closed_history),
                "updated_at": self.updated_at,
            }

    # ================= SYNC =================
    async def sync(self, *args, **kwargs):
        symbol = kwargs.get("symbol")
        if symbol:
            return self.get_position(symbol)
        with self.lock:
            return copy.deepcopy(self.positions)
