import asyncio
import logging
import time
from typing import Dict, Any, Optional

from config import SYMBOLS


class ExchangePositionSync:
    """
    Institutional / hedge-fund grade exchange position synchronization layer

    설계 목표:
    1) 거래소 실포지션과 내부 position engine 상태 정합성 유지
    2) 거래소 실포지션과 pnl engine 상태 정합성 유지
    3) target symbol만 선택적으로 sync
    4) flat / open / changed state를 안정적으로 감지
    5) entry / size / side drift를 정확히 반영
    6) stale internal position 자동 정리
    7) pnl orphan position 자동 정리
    8) sync loop 장애가 전체 시스템을 죽이지 않도록 보호
    9) sync 결과 메트릭 / snapshot 제공
    10) 장기운용형 poller 구조 제공
    11) overwrite-safe exchange sync 지원
    12) sync interval 동안 중복 로그 / 중복 반영 최소화
    13) 거래소 응답 이상치 방어
    14) partial size drift도 구조적으로 흡수 가능하게 설계
    15) 향후 websocket/userData sync 확장 가능한 구조 유지
    """

    def __init__(self, client, position_engine, pnl_engine=None):
        self.client = client
        self.position_engine = position_engine
        self.pnl_engine = pnl_engine

        self.sync_interval = 5.0
        self.sync_timeout_seconds = 15.0
        self.zero_epsilon = 1e-8

        self.target_symbols = {str(s).upper().strip() for s in SYMBOLS}

        self.last_seen_exchange_state: Dict[str, Dict[str, Any]] = {}
        self.last_sync_ts: Dict[str, float] = {}
        self.last_flat_ts: Dict[str, float] = {}
        self.last_error_ts: float = 0.0

        self.sync_iterations = 0
        self.sync_success_count = 0
        self.sync_error_count = 0
        self.open_sync_count = 0
        self.close_sync_count = 0
        self.pnl_open_sync_count = 0
        self.pnl_close_sync_count = 0
        self.stale_cleanup_count = 0
        self.updated_at = time.time()

    # ================= INTERNAL =================
    def _now(self) -> float:
        return time.time()

    def _touch(self):
        self.updated_at = self._now()

    def _safe_float(self, value, default=0.0):
        try:
            return float(value)
        except Exception:
            return default

    def _normalize_symbol(self, symbol: str) -> str:
        return str(symbol).upper().strip()

    def _normalize_side(self, amt: float) -> Optional[str]:
        if amt > 0:
            return "BUY"
        if amt < 0:
            return "SELL"
        return None

    def _is_flat(self, amt: float) -> bool:
        return abs(amt) < self.zero_epsilon

    def _build_state(self, side: Optional[str], entry: float, size: float) -> Dict[str, Any]:
        return {
            "side": side,
            "entry": round(self._safe_float(entry, 0.0), 8),
            "size": round(self._safe_float(size, 0.0), 8),
        }

    def _state_changed(self, symbol: str, side: Optional[str], entry: float, size: float) -> bool:
        prev = self.last_seen_exchange_state.get(symbol)
        current = self._build_state(side, entry, size)
        if prev != current:
            self.last_seen_exchange_state[symbol] = current
            return True
        return False

    def _set_flat_state(self, symbol: str):
        self.last_seen_exchange_state[symbol] = self._build_state(None, 0.0, 0.0)
        self.last_flat_ts[symbol] = self._now()

    def _get_position_engine_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        try:
            if hasattr(self.position_engine, "get_position"):
                return self.position_engine.get_position(symbol)
            positions = getattr(self.position_engine, "positions", None)
            if isinstance(positions, dict):
                return positions.get(symbol)
        except Exception:
            return None
        return None

    def _get_pnl_engine_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        try:
            if self.pnl_engine is None:
                return None
            if hasattr(self.pnl_engine, "get_position"):
                return self.pnl_engine.get_position(symbol)
            positions = getattr(self.pnl_engine, "positions", None)
            if isinstance(positions, dict):
                return positions.get(symbol)
        except Exception:
            return None
        return None

    async def _sync_open_position(self, symbol: str, side: str, entry: float, size: float):
        try:
            if hasattr(self.position_engine, "sync_from_exchange_position"):
                result = self.position_engine.sync_from_exchange_position(
                    symbol=symbol,
                    side=side,
                    entry=entry,
                    size=size,
                )
                if asyncio.iscoroutine(result):
                    await result
                self.open_sync_count += 1
                return

            if hasattr(self.position_engine, "mark_open_from_exchange"):
                result = self.position_engine.mark_open_from_exchange(
                    symbol=symbol,
                    side=side,
                    entry=entry,
                    size=size,
                    overwrite=True,
                )
                if asyncio.iscoroutine(result):
                    await result
                self.open_sync_count += 1
                return

            if hasattr(self.position_engine, "sync"):
                result = self.position_engine.sync(symbol=symbol, side=side, entry=entry, size=size)
                if asyncio.iscoroutine(result):
                    await result
                self.open_sync_count += 1
                return

            if hasattr(self.position_engine, "open_position"):
                result = self.position_engine.open_position(symbol, side, entry, size)
                if asyncio.iscoroutine(result):
                    await result
                self.open_sync_count += 1

        except TypeError:
            try:
                result = self.position_engine.mark_open_from_exchange(symbol, side, entry, size)
                if asyncio.iscoroutine(result):
                    await result
                self.open_sync_count += 1
            except Exception as e:
                logging.error(f"Sync open position error | {symbol}: {e}")
        except Exception as e:
            logging.error(f"Sync open position error | {symbol}: {e}")

    async def _sync_closed_position(self, symbol: str):
        try:
            if hasattr(self.position_engine, "mark_closed_from_exchange"):
                result = self.position_engine.mark_closed_from_exchange(symbol=symbol, reason="exchange_sync")
                if asyncio.iscoroutine(result):
                    await result
                self.close_sync_count += 1
                return

            if hasattr(self.position_engine, "close"):
                result = self.position_engine.close(symbol, reason="exchange_sync")
                if asyncio.iscoroutine(result):
                    await result
                self.close_sync_count += 1
                return

            if hasattr(self.position_engine, "clear"):
                result = self.position_engine.clear(symbol)
                if asyncio.iscoroutine(result):
                    await result
                self.close_sync_count += 1
                return

        except Exception as e:
            logging.error(f"Sync close position error | {symbol}: {e}")

    def _sync_pnl_open(self, symbol: str, side: str, entry: float, size: float):
        try:
            if self.pnl_engine is None:
                return

            current = self._get_pnl_engine_position(symbol)
            if current:
                current_side = str(current.get("side", "")).upper().strip()
                current_entry = round(self._safe_float(current.get("entry"), 0.0), 8)
                current_size = round(self._safe_float(current.get("size"), 0.0), 8)
                if current_side == side and current_entry == round(entry, 8) and current_size == round(size, 8):
                    return

                # drift 발생 시 overwrite 가능하면 overwrite, 아니면 close 후 reopen
                if hasattr(self.pnl_engine, "close_position"):
                    mark_price = None
                    if hasattr(self.pnl_engine, "last_price") and isinstance(self.pnl_engine.last_price, dict):
                        mark_price = self._safe_float(self.pnl_engine.last_price.get(symbol), 0.0)
                    close_px = mark_price if mark_price > 0 else entry
                    self.pnl_engine.close_position(symbol, close_px, fee=0.0)

            if hasattr(self.pnl_engine, "open_position"):
                try:
                    self.pnl_engine.open_position(symbol, side, entry, size, fee=0.0, overwrite=True)
                except TypeError:
                    self.pnl_engine.open_position(symbol, side, entry, size)
                self.pnl_open_sync_count += 1

        except Exception as e:
            logging.error(f"PnL sync open error | {symbol}: {e}")

    def _sync_pnl_close(self, symbol: str):
        try:
            if self.pnl_engine is None:
                return

            current = self._get_pnl_engine_position(symbol)
            if not current:
                return

            last_price = None
            if hasattr(self.pnl_engine, "last_price") and isinstance(self.pnl_engine.last_price, dict):
                last_price = self._safe_float(self.pnl_engine.last_price.get(symbol), 0.0)

            if last_price and hasattr(self.pnl_engine, "close_position"):
                self.pnl_engine.close_position(symbol, last_price, fee=0.0)
                self.pnl_close_sync_count += 1
                return

            positions = getattr(self.pnl_engine, "positions", None)
            if isinstance(positions, dict) and symbol in positions:
                del positions[symbol]
                self.pnl_close_sync_count += 1

        except Exception as e:
            logging.error(f"PnL sync close error | {symbol}: {e}")

    async def _cleanup_missing_symbols(self, seen_symbols: set[str]):
        try:
            for symbol in self.target_symbols:
                if symbol in seen_symbols:
                    continue

                local_pos = self._get_position_engine_position(symbol)
                pnl_pos = self._get_pnl_engine_position(symbol)
                if local_pos or pnl_pos:
                    self.stale_cleanup_count += 1
                    logging.warning(f"SYNC CLEANUP | {symbol} | exchange response missing target symbol")
                    await self._sync_closed_position(symbol)
                    self._sync_pnl_close(symbol)
                    self._set_flat_state(symbol)
        except Exception as e:
            logging.error(f"Missing symbol cleanup error: {e}")

    # ================= MAIN =================
    async def start(self):
        while True:
            try:
                self.sync_iterations += 1
                seen_symbols: set[str] = set()
                positions = self.client.futures_position_information()

                for p in positions:
                    symbol = self._normalize_symbol(p.get("symbol"))
                    if not symbol or symbol not in self.target_symbols:
                        continue
                    seen_symbols.add(symbol)

                    amt = self._safe_float(p.get("positionAmt"), 0.0)
                    entry = self._safe_float(p.get("entryPrice"), 0.0)
                    side = self._normalize_side(amt)
                    size = abs(amt)
                    self.last_sync_ts[symbol] = self._now()

                    if self._is_flat(amt):
                        changed = self._state_changed(symbol, None, 0.0, 0.0)
                        if changed:
                            logging.info(f"SYNC CLOSED | {symbol} | exchange has no position")
                            await self._sync_closed_position(symbol)
                            self._sync_pnl_close(symbol)
                            self._set_flat_state(symbol)
                        continue

                    if side not in ("BUY", "SELL") or entry <= 0 or size <= 0:
                        logging.warning(f"SYNC SKIP INVALID | {symbol} | side={side} | entry={entry} | size={size}")
                        continue

                    changed = self._state_changed(symbol, side, entry, size)
                    if changed:
                        logging.info(f"SYNC OPEN | {symbol} | side={side} | entry={entry} | size={size}")
                        await self._sync_open_position(symbol, side, entry, size)
                        self._sync_pnl_open(symbol, side, entry, size)

                await self._cleanup_missing_symbols(seen_symbols)
                self.sync_success_count += 1
                self._touch()

            except Exception as e:
                self.sync_error_count += 1
                self.last_error_ts = self._now()
                logging.error(f"Position sync error: {e}")

            await asyncio.sleep(self.sync_interval)

    # ================= SNAPSHOT =================
    def snapshot(self) -> Dict[str, Any]:
        return {
            "sync_interval": self.sync_interval,
            "sync_timeout_seconds": self.sync_timeout_seconds,
            "target_symbols": sorted(list(self.target_symbols)),
            "zero_epsilon": self.zero_epsilon,
            "sync_iterations": self.sync_iterations,
            "sync_success_count": self.sync_success_count,
            "sync_error_count": self.sync_error_count,
            "open_sync_count": self.open_sync_count,
            "close_sync_count": self.close_sync_count,
            "pnl_open_sync_count": self.pnl_open_sync_count,
            "pnl_close_sync_count": self.pnl_close_sync_count,
            "stale_cleanup_count": self.stale_cleanup_count,
            "last_seen_exchange_state": dict(self.last_seen_exchange_state),
            "last_sync_ts": dict(self.last_sync_ts),
            "last_flat_ts": dict(self.last_flat_ts),
            "last_error_ts": self.last_error_ts,
            "updated_at": self.updated_at,
        }
