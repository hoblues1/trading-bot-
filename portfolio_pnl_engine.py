import logging
import time
from typing import Dict, Any, Optional, List

from config import INITIAL_CAPITAL


class PnLEngine:
    """
    High-end PnL engine

    강화 포인트:
    1) balance / realized / unrealized / equity 분리 유지
    2) open / partial close / full close 모두 안정 지원
    3) main.py 호환용 set_balance 유지
    4) 포지션별 상태 / MFE / MAE / mark_price 추적 강화
    5) 수수료 / trade history / 승률 / profit factor / drawdown 통계 제공
    6) 중복 open 방지 및 overwrite 명시화
    7) tiny remainder(잔량 찌꺼기) 자동 정리
    8) snapshot / stats 응답 표준화
    """

    MIN_POSITION_EPSILON = 1e-8

    def __init__(self):
        self.start_balance = float(INITIAL_CAPITAL)
        self.balance = float(INITIAL_CAPITAL)

        self.realized_pnl = 0.0
        self.unrealized_pnl = 0.0
        self.fees = 0.0

        self.positions: Dict[str, Dict[str, Any]] = {}
        self.last_price: Dict[str, float] = {}

        self.trade_history: List[Dict[str, Any]] = []
        self.closed_positions: List[Dict[str, Any]] = []

        self.peak_equity = float(INITIAL_CAPITAL)
        self.max_drawdown = 0.0
        self.updated_at = time.time()

    # ================= INTERNAL =================
    def _now(self) -> float:
        return time.time()

    def _safe_float(self, value, default=0.0) -> float:
        try:
            return float(value)
        except Exception:
            return float(default)

    def _normalize_symbol(self, symbol: str) -> str:
        return str(symbol).upper().strip()

    def _normalize_side(self, side: str) -> str:
        return str(side).upper().strip()

    def _touch(self):
        self.updated_at = self._now()

    def _calc_position_pnl(self, side: str, entry: float, exit_price: float, size: float) -> float:
        if side == "BUY":
            return (exit_price - entry) * size
        return (entry - exit_price) * size

    def _calc_return_pct(self, entry: float, exit_price: float, side: str) -> float:
        if entry <= 0:
            return 0.0
        if side == "BUY":
            return (exit_price - entry) / entry
        return (entry - exit_price) / entry

    def _update_position_excursion(self, symbol: str, mark_price: float):
        pos = self.positions.get(symbol)
        if not pos:
            return

        entry = self._safe_float(pos.get("entry"), 0.0)
        size = self._safe_float(pos.get("size"), 0.0)
        side = self._normalize_side(pos.get("side", ""))

        if entry <= 0 or size <= 0 or mark_price <= 0:
            return

        current_unrealized = self._calc_position_pnl(side, entry, mark_price, size)
        pos["mark_price"] = mark_price
        pos["unrealized_pnl"] = current_unrealized
        pos["mfe"] = max(self._safe_float(pos.get("mfe"), 0.0), current_unrealized)
        pos["mae"] = min(self._safe_float(pos.get("mae"), 0.0), current_unrealized)
        pos["highest_price"] = max(self._safe_float(pos.get("highest_price"), entry), mark_price)
        pos["lowest_price"] = min(self._safe_float(pos.get("lowest_price"), entry), mark_price)
        pos["last_update_time"] = self._now()

    def _recalculate_drawdown(self):
        eq = self.equity()
        if eq > self.peak_equity:
            self.peak_equity = eq

        if self.peak_equity > 0:
            dd = (self.peak_equity - eq) / self.peak_equity
            self.max_drawdown = max(self.max_drawdown, dd)

    def _finalize_position_close(
        self,
        symbol: str,
        pos: Dict[str, Any],
        exit_price: float,
        final_fee: float,
        close_action: str,
        close_size: float,
        close_pnl_after_fee: float,
    ):
        now_ts = self._now()
        open_time = self._safe_float(pos.get("open_time"), now_ts)
        total_position_realized = self._safe_float(pos.get("realized_pnl"), 0.0)
        total_position_fees = self._safe_float(pos.get("fees"), 0.0)
        entry = self._safe_float(pos.get("entry"), 0.0)
        side = self._normalize_side(pos.get("side", ""))
        initial_size = self._safe_float(pos.get("initial_size"), close_size)

        summary = {
            "symbol": symbol,
            "side": side,
            "entry": entry,
            "exit": exit_price,
            "initial_size": initial_size,
            "final_close_size": close_size,
            "action": close_action,
            "open_time": open_time,
            "close_time": now_ts,
            "hold_seconds": max(0.0, now_ts - open_time),
            "partial_count": int(pos.get("partial_count", 0)),
            "total_position_realized_pnl": total_position_realized,
            "total_position_fees": total_position_fees,
            "net_return_pct": self._calc_return_pct(entry, exit_price, side),
            "mfe": self._safe_float(pos.get("mfe"), 0.0),
            "mae": self._safe_float(pos.get("mae"), 0.0),
            "highest_price": self._safe_float(pos.get("highest_price"), entry),
            "lowest_price": self._safe_float(pos.get("lowest_price"), entry),
            "last_mark_price": self._safe_float(pos.get("mark_price"), exit_price),
            "close_fee": final_fee,
            "close_pnl_after_fee": close_pnl_after_fee,
        }
        self.closed_positions.append(summary)

    # ================= BALANCE =================
    def set_balance(self, balance: float):
        try:
            balance = float(balance)
            if balance > 0:
                self.start_balance = balance
                self.balance = balance
                self.peak_equity = max(self.peak_equity, balance)
                self._touch()
        except Exception as e:
            logging.error(f"PnL set_balance error: {e}")

    # ================= PRICE UPDATE =================
    def update_price(self, symbol, price):
        try:
            symbol = self._normalize_symbol(symbol)
            price = float(price)
            if price <= 0:
                return

            self.last_price[symbol] = price
            if symbol in self.positions:
                self._update_position_excursion(symbol, price)
            self.calculate_unrealized()
            self._recalculate_drawdown()
            self._touch()
        except Exception as e:
            logging.error(f"PnL update_price error: {e}")

    # ================= QUERY =================
    def has_position(self, symbol: str) -> bool:
        return self._normalize_symbol(symbol) in self.positions

    def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        return self.positions.get(self._normalize_symbol(symbol))

    def position_count(self) -> int:
        return len(self.positions)

    # ================= OPEN POSITION =================
    def open_position(self, symbol, side, price, size, fee=0.0, overwrite=False):
        try:
            symbol = self._normalize_symbol(symbol)
            side = self._normalize_side(side)
            price = float(price)
            size = float(size)
            fee = float(fee)

            if side not in ("BUY", "SELL"):
                logging.warning(f"PnL open ignored: invalid side | {symbol} | {side}")
                return False

            if price <= 0 or size <= 0:
                logging.warning(f"PnL open ignored: invalid args | {symbol} | price={price} size={size}")
                return False

            if symbol in self.positions and not overwrite:
                logging.warning(f"PnL open ignored: position already exists | {symbol}")
                return False

            now_ts = self._now()
            self.positions[symbol] = {
                "symbol": symbol,
                "side": side,
                "entry": price,
                "size": size,
                "initial_size": size,
                "open_time": now_ts,
                "last_update_time": now_ts,
                "partial_count": 0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "fees": fee,
                "mark_price": self.last_price.get(symbol, price),
                "mfe": 0.0,
                "mae": 0.0,
                "highest_price": price,
                "lowest_price": price,
            }

            self.fees += fee
            self.balance -= fee

            if symbol in self.last_price:
                self._update_position_excursion(symbol, self.last_price[symbol])

            self.calculate_unrealized()
            self._recalculate_drawdown()
            self._touch()
            return True

        except Exception as e:
            logging.error(f"PnL open_position error: {e}")
            return False

    # ================= PARTIAL CLOSE =================
    def partial_close_position(self, symbol, price, closed_size, fee=0.0):
        try:
            symbol = self._normalize_symbol(symbol)
            price = float(price)
            closed_size = float(closed_size)
            fee = float(fee)

            if symbol not in self.positions:
                return 0.0

            pos = self.positions[symbol]
            entry = self._safe_float(pos.get("entry"), 0.0)
            current_size = self._safe_float(pos.get("size"), 0.0)
            side = self._normalize_side(pos.get("side", ""))

            if price <= 0 or closed_size <= 0 or current_size <= 0:
                return 0.0

            closed_size = min(closed_size, current_size)
            raw_pnl = self._calc_position_pnl(side, entry, price, closed_size)
            pnl_after_fee = raw_pnl - fee
            remain_size = current_size - closed_size
            now_ts = self._now()

            self.realized_pnl += pnl_after_fee
            self.balance += pnl_after_fee
            self.fees += fee

            pos["size"] = 0.0 if remain_size <= self.MIN_POSITION_EPSILON else round(remain_size, 12)
            pos["partial_count"] = int(pos.get("partial_count", 0)) + 1
            pos["realized_pnl"] = self._safe_float(pos.get("realized_pnl"), 0.0) + pnl_after_fee
            pos["fees"] = self._safe_float(pos.get("fees"), 0.0) + fee
            pos["last_partial_time"] = now_ts
            pos["last_update_time"] = now_ts
            pos["mark_price"] = price

            self.trade_history.append({
                "symbol": symbol,
                "side": side,
                "entry": entry,
                "exit": price,
                "size": closed_size,
                "remaining_size": pos["size"],
                "action": "PARTIAL_CLOSE",
                "raw_pnl": raw_pnl,
                "pnl": pnl_after_fee,
                "fee": fee,
                "time": now_ts,
                "hold_seconds_from_open": max(0.0, now_ts - self._safe_float(pos.get("open_time"), now_ts)),
            })

            logging.info(
                f"PARTIAL CLOSED | {symbol} | size={closed_size:.6f} | remain={pos['size']:.6f} | pnl={pnl_after_fee:.6f}"
            )

            if pos["size"] <= self.MIN_POSITION_EPSILON:
                self._finalize_position_close(
                    symbol=symbol,
                    pos=pos,
                    exit_price=price,
                    final_fee=fee,
                    close_action="PARTIAL_CLOSE_FINAL",
                    close_size=closed_size,
                    close_pnl_after_fee=pnl_after_fee,
                )
                del self.positions[symbol]
            else:
                self._update_position_excursion(symbol, price)

            self.calculate_unrealized()
            self._recalculate_drawdown()
            self._touch()
            return pnl_after_fee

        except Exception as e:
            logging.error(f"PnL partial_close_position error: {e}")
            return 0.0

    # ================= CLOSE POSITION =================
    def close_position(self, symbol, price, fee=0.0):
        try:
            symbol = self._normalize_symbol(symbol)
            price = float(price)
            fee = float(fee)

            if symbol not in self.positions:
                return 0.0

            pos = self.positions[symbol]
            entry = self._safe_float(pos.get("entry"), 0.0)
            size = self._safe_float(pos.get("size"), 0.0)
            side = self._normalize_side(pos.get("side", ""))

            if price <= 0 or size <= 0:
                return 0.0

            raw_pnl = self._calc_position_pnl(side, entry, price, size)
            pnl_after_fee = raw_pnl - fee
            now_ts = self._now()

            self.realized_pnl += pnl_after_fee
            self.balance += pnl_after_fee
            self.fees += fee

            pos["realized_pnl"] = self._safe_float(pos.get("realized_pnl"), 0.0) + pnl_after_fee
            pos["fees"] = self._safe_float(pos.get("fees"), 0.0) + fee
            pos["mark_price"] = price
            pos["last_update_time"] = now_ts

            self.trade_history.append({
                "symbol": symbol,
                "side": side,
                "entry": entry,
                "exit": price,
                "size": size,
                "action": "CLOSE",
                "raw_pnl": raw_pnl,
                "pnl": pnl_after_fee,
                "fee": fee,
                "open_time": pos.get("open_time"),
                "close_time": now_ts,
                "hold_seconds": max(0.0, now_ts - self._safe_float(pos.get("open_time"), now_ts)),
                "partial_count": int(pos.get("partial_count", 0)),
                "total_position_realized_pnl": self._safe_float(pos.get("realized_pnl"), 0.0),
                "total_position_fees": self._safe_float(pos.get("fees"), 0.0),
            })

            self._finalize_position_close(
                symbol=symbol,
                pos=pos,
                exit_price=price,
                final_fee=fee,
                close_action="CLOSE",
                close_size=size,
                close_pnl_after_fee=pnl_after_fee,
            )
            del self.positions[symbol]

            logging.info(f"TRADE CLOSED | {symbol} | pnl={pnl_after_fee:.6f}")

            self.calculate_unrealized()
            self._recalculate_drawdown()
            self._touch()
            return pnl_after_fee

        except Exception as e:
            logging.error(f"PnL close_position error: {e}")
            return 0.0

    # ================= UNREALIZED =================
    def calculate_unrealized(self):
        try:
            total = 0.0

            for symbol, pos in self.positions.items():
                if symbol not in self.last_price:
                    pos["unrealized_pnl"] = 0.0
                    continue

                price = self._safe_float(self.last_price[symbol], 0.0)
                entry = self._safe_float(pos.get("entry"), 0.0)
                size = self._safe_float(pos.get("size"), 0.0)
                side = self._normalize_side(pos.get("side", ""))

                if price <= 0 or entry <= 0 or size <= 0:
                    pos["unrealized_pnl"] = 0.0
                    continue

                pnl = self._calc_position_pnl(side, entry, price, size)
                pos["unrealized_pnl"] = pnl
                pos["mark_price"] = price
                total += pnl

            self.unrealized_pnl = total
            return total

        except Exception as e:
            logging.error(f"PnL calculate_unrealized error: {e}")
            return 0.0

    # ================= TOTAL =================
    def total_pnl(self):
        unreal = self.calculate_unrealized()
        return self.realized_pnl + unreal

    def equity(self):
        return self.balance + self.calculate_unrealized()

    # ================= ANALYTICS =================
    def _closed_trades_only(self):
        return [t for t in self.trade_history if t.get("action") == "CLOSE"]

    def _position_summaries_only(self):
        return list(self.closed_positions)

    def win_rate(self):
        closed = self._closed_trades_only()
        if not closed:
            return 0.0

        wins = sum(1 for t in closed if self._safe_float(t.get("pnl"), 0.0) > 0)
        return wins / len(closed)

    def average_win(self):
        closed = self._closed_trades_only()
        wins = [self._safe_float(t.get("pnl"), 0.0) for t in closed if self._safe_float(t.get("pnl"), 0.0) > 0]
        if not wins:
            return 0.0
        return sum(wins) / len(wins)

    def average_loss(self):
        closed = self._closed_trades_only()
        losses = [self._safe_float(t.get("pnl"), 0.0) for t in closed if self._safe_float(t.get("pnl"), 0.0) < 0]
        if not losses:
            return 0.0
        return sum(losses) / len(losses)

    def gross_profit(self):
        closed = self._closed_trades_only()
        return sum(self._safe_float(t.get("pnl"), 0.0) for t in closed if self._safe_float(t.get("pnl"), 0.0) > 0)

    def gross_loss(self):
        closed = self._closed_trades_only()
        return sum(self._safe_float(t.get("pnl"), 0.0) for t in closed if self._safe_float(t.get("pnl"), 0.0) < 0)

    def profit_factor(self):
        gp = self.gross_profit()
        gl = abs(self.gross_loss())
        if gl <= 0:
            return gp if gp > 0 else 0.0
        return gp / gl

    def expectancy(self):
        wr = self.win_rate()
        aw = self.average_win()
        al = abs(self.average_loss())
        return (wr * aw) - ((1.0 - wr) * al)

    # ================= SNAPSHOT =================
    def snapshot(self, symbol: Optional[str] = None):
        if symbol is not None:
            symbol = self._normalize_symbol(symbol)
            return self.positions.get(symbol)

        return {
            "start_balance": self.start_balance,
            "balance": self.balance,
            "equity": self.equity(),
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "total_pnl": self.total_pnl(),
            "fees": self.fees,
            "peak_equity": self.peak_equity,
            "max_drawdown": self.max_drawdown,
            "positions": self.positions,
            "last_price": self.last_price,
            "trade_count": len(self.trade_history),
            "closed_positions": len(self.closed_positions),
            "win_rate": self.win_rate(),
            "average_win": self.average_win(),
            "average_loss": self.average_loss(),
            "profit_factor": self.profit_factor(),
            "expectancy": self.expectancy(),
            "updated_at": self.updated_at,
        }

    # ================= STATS =================
    def stats(self):
        self.calculate_unrealized()
        self._recalculate_drawdown()

        closed = self._closed_trades_only()
        summaries = self._position_summaries_only()

        return {
            "balance": self.balance,
            "equity": self.balance + self.unrealized_pnl,
            "start_balance": self.start_balance,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "total_pnl": self.realized_pnl + self.unrealized_pnl,
            "fees": self.fees,
            "open_positions": len(self.positions),
            "trades": len(self.trade_history),
            "closed_trades": len(closed),
            "closed_positions": len(summaries),
            "win_rate": self.win_rate(),
            "average_win": self.average_win(),
            "average_loss": self.average_loss(),
            "gross_profit": self.gross_profit(),
            "gross_loss": self.gross_loss(),
            "profit_factor": self.profit_factor(),
            "expectancy": self.expectancy(),
            "peak_equity": self.peak_equity,
            "max_drawdown": self.max_drawdown,
            "updated_at": self.updated_at,
        }
