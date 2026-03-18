import copy
import logging
import time
from typing import Optional, Dict, Any

from config import (
    MAX_CONSECUTIVE_LOSSES,
    MAX_DAILY_LOSS,
    MAX_DRAWDOWN,
    INITIAL_CAPITAL,
)


class KillSwitch:
    """
    High-end live trading kill switch

    반환 규칙:
    - True  = 거래 가능
    - False = 거래 차단
    """

    def __init__(self):
        base = float(INITIAL_CAPITAL)

        self.start_balance = base
        self.daily_start_balance = base

        self.current_balance = base
        self.current_equity = base

        self.peak_balance = base
        self.peak_equity = base

        self.loss_streak = 0
        self.total_trades = 0
        self.losing_trades = 0
        self.winning_trades = 0

        self.daily_realized_pnl = 0.0
        self.daily_unrealized_pnl = 0.0
        self.daily_total_pnl = 0.0

        self.last_reset_day = self._today()

        self.triggered = False
        self.trigger_reason = None
        self.triggered_at = None
        self.trigger_count = 0
        self.last_check_at = None
        self.updated_at = time.time()

    # ================= INTERNAL =================
    def _today(self) -> str:
        return time.strftime("%Y-%m-%d")

    def _now(self) -> float:
        return time.time()

    def _touch(self):
        self.updated_at = self._now()

    def _safe_float(self, value, default=0.0):
        try:
            return float(value)
        except Exception:
            return float(default)

    def _calc_drawdown(self) -> float:
        peak_equity = self.peak_equity if self.peak_equity > 0 else 1e-9
        current_equity = self.current_equity if self.current_equity > 0 else 0.0
        return max(0.0, (peak_equity - current_equity) / peak_equity)

    # ================= INIT / RESET =================
    def set_start_balance(self, balance: float, reset_daily_anchor: bool = True) -> bool:
        balance = self._safe_float(balance, 0.0)
        if balance <= 0:
            return False

        self.start_balance = balance
        if reset_daily_anchor:
            self.daily_start_balance = balance

        self.current_balance = balance
        self.current_equity = balance
        self.peak_balance = balance
        self.peak_equity = balance

        self.triggered = False
        self.trigger_reason = None
        self.triggered_at = None

        self._touch()
        return True

    def sync_with_exchange_balance(self, balance: float, equity: Optional[float] = None) -> bool:
        balance = self._safe_float(balance, 0.0)
        if balance <= 0:
            return False

        if equity is None:
            equity = balance
        equity = self._safe_float(equity, balance)

        self.start_balance = balance
        self.daily_start_balance = balance
        self.current_balance = balance
        self.current_equity = equity
        self.peak_balance = balance
        self.peak_equity = equity

        self.triggered = False
        self.trigger_reason = None
        self.triggered_at = None
        self.last_reset_day = self._today()

        self._touch()
        return True

    def reset_daily(self):
        today = self._today()
        if today != self.last_reset_day:
            logging.info("KillSwitch daily reset")
            self.daily_realized_pnl = 0.0
            self.daily_unrealized_pnl = 0.0
            self.daily_total_pnl = 0.0
            self.loss_streak = 0
            self.last_reset_day = today

            if self.current_balance > 0:
                self.daily_start_balance = self.current_balance

            self._touch()

    def manual_reset(self, reset_peaks: bool = False):
        self.triggered = False
        self.trigger_reason = None
        self.triggered_at = None
        self.loss_streak = 0
        self.daily_realized_pnl = 0.0
        self.daily_unrealized_pnl = 0.0
        self.daily_total_pnl = 0.0
        self.last_reset_day = self._today()

        if self.current_balance > 0:
            self.daily_start_balance = self.current_balance

        if reset_peaks:
            self.peak_balance = self.current_balance
            self.peak_equity = self.current_equity

        self._touch()

    # ================= UPDATE BALANCE / EQUITY =================
    def update_balance(self, balance, equity=None):
        balance = self._safe_float(balance, self.current_balance)
        self.current_balance = balance
        if balance > self.peak_balance:
            self.peak_balance = balance

        if equity is None:
            equity = balance
        equity = self._safe_float(equity, balance)
        self.current_equity = equity
        if equity > self.peak_equity:
            self.peak_equity = equity

        self._touch()

    # ================= UPDATE TRADE RESULT =================
    def update_trade_result(self, pnl):
        pnl = self._safe_float(pnl, 0.0)

        self.total_trades += 1
        if pnl < 0:
            self.loss_streak += 1
            self.losing_trades += 1
        elif pnl > 0:
            self.loss_streak = 0
            self.winning_trades += 1
        else:
            self.loss_streak = 0

        self.daily_realized_pnl += pnl
        self.daily_total_pnl = self.daily_realized_pnl + self.daily_unrealized_pnl
        self._touch()

    def update_unrealized(self, unrealized_pnl):
        self.daily_unrealized_pnl = self._safe_float(unrealized_pnl, 0.0)
        self.daily_total_pnl = self.daily_realized_pnl + self.daily_unrealized_pnl
        self._touch()

    def set_daily_total_pnl(self, total_pnl):
        self.daily_total_pnl = self._safe_float(total_pnl, self.daily_realized_pnl)
        self.daily_unrealized_pnl = self.daily_total_pnl - self.daily_realized_pnl
        self._touch()

    def sync_from_stats(self, stats: Dict[str, Any]):
        try:
            if not isinstance(stats, dict):
                return

            balance = self._safe_float(stats.get("balance"), self.current_balance)
            equity = self._safe_float(stats.get("equity"), balance)
            realized = self._safe_float(stats.get("realized_pnl"), self.daily_realized_pnl)
            unrealized = self._safe_float(stats.get("unrealized_pnl"), self.daily_unrealized_pnl)

            self.update_balance(balance, equity)
            self.daily_realized_pnl = realized
            self.daily_unrealized_pnl = unrealized
            self.daily_total_pnl = realized + unrealized
            self._touch()

        except Exception as e:
            logging.error(f"KillSwitch sync_from_stats error: {e}")

    # ================= CHECKERS =================
    def _check_loss_streak(self) -> Optional[str]:
        if self.loss_streak >= int(MAX_CONSECUTIVE_LOSSES):
            return f"loss_streak_exceeded:{self.loss_streak}"
        return None

    def _check_daily_loss(self) -> Optional[str]:
        base = self.daily_start_balance if self.daily_start_balance > 0 else self.start_balance
        daily_loss_limit = base * float(MAX_DAILY_LOSS)

        if self.daily_realized_pnl <= -daily_loss_limit:
            return f"daily_realized_loss_exceeded:{self.daily_realized_pnl:.6f}"

        if self.daily_total_pnl <= -daily_loss_limit:
            return f"daily_total_loss_exceeded:{self.daily_total_pnl:.6f}"

        return None

    def _check_drawdown(self) -> Optional[str]:
        if self.peak_equity <= 0:
            return "invalid_peak_equity"

        drawdown = self._calc_drawdown()
        if drawdown >= float(MAX_DRAWDOWN):
            return f"drawdown_exceeded:{drawdown:.6f}"
        return None

    def _trigger(self, reason: str):
        if not self.triggered:
            self.triggered = True
            self.trigger_reason = str(reason)
            self.triggered_at = self._now()
            self.trigger_count += 1

        logging.error(f"KILL SWITCH TRIGGERED | reason={self.trigger_reason}")
        self._touch()

    # ================= CHECK =================
    def check(self, *args, **kwargs) -> bool:
        """
        True  = 거래 가능
        False = 거래 차단
        """
        try:
            self.last_check_at = self._now()
            self.reset_daily()

            if self.start_balance <= 0 or self.current_balance <= 0 or self.current_equity <= 0:
                self._trigger("invalid_balance_state")
                return False

            if self.triggered:
                self._touch()
                return False

            for checker in (
                self._check_loss_streak,
                self._check_daily_loss,
                self._check_drawdown,
            ):
                reason = checker()
                if reason:
                    self._trigger(reason)
                    return False

            self._touch()
            return True

        except Exception as e:
            logging.error(f"KillSwitch error: {e}")
            self._trigger(f"exception:{e}")
            return False

    # ================= INFO =================
    def status(self) -> Dict[str, Any]:
        return {
            "triggered": self.triggered,
            "trigger_reason": self.trigger_reason,
            "triggered_at": self.triggered_at,
            "trigger_count": self.trigger_count,
            "start_balance": self.start_balance,
            "daily_start_balance": self.daily_start_balance,
            "current_balance": self.current_balance,
            "current_equity": self.current_equity,
            "peak_balance": self.peak_balance,
            "peak_equity": self.peak_equity,
            "loss_streak": self.loss_streak,
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "daily_realized_pnl": self.daily_realized_pnl,
            "daily_unrealized_pnl": self.daily_unrealized_pnl,
            "daily_total_pnl": self.daily_total_pnl,
            "drawdown": self._calc_drawdown(),
            "last_reset_day": self.last_reset_day,
            "last_check_at": self.last_check_at,
            "updated_at": self.updated_at,
        }

    def snapshot(self) -> Dict[str, Any]:
        return copy.deepcopy(self.status())
