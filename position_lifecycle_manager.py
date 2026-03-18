from __future__ import annotations

from typing import Dict, Any


class PositionLifecycleManager:
    """
    포지션 진입 후 관리 전담.
    - 브레이크이븐 이동
    - 부분 익절
    - 시간 기반 청산
    - 신호 약화 청산
    """

    def __init__(
        self,
        break_even_trigger_pct: float = 0.010,
        partial_take_profit_trigger_pct: float = 0.018,
        partial_take_profit_ratio: float = 0.35,
        max_hold_seconds: int = 1200,
        weak_signal_exit_score: float = 0.40,
    ) -> None:
        self.break_even_trigger_pct = float(break_even_trigger_pct)
        self.partial_take_profit_trigger_pct = float(partial_take_profit_trigger_pct)
        self.partial_take_profit_ratio = float(partial_take_profit_ratio)
        self.max_hold_seconds = int(max_hold_seconds)
        self.weak_signal_exit_score = float(weak_signal_exit_score)

    def evaluate(
        self,
        position: Dict[str, Any],
        current_pnl_pct: float,
        holding_seconds: int,
        current_signal_score: float,
        reverse_signal_score: float,
    ) -> Dict[str, Any]:
        current_pnl_pct = float(current_pnl_pct)
        holding_seconds = int(holding_seconds)
        current_signal_score = float(current_signal_score)
        reverse_signal_score = float(reverse_signal_score)

        decision = {
            "action": "HOLD",
            "reason": "NO_ACTION",
            "move_stop_to_break_even": False,
            "partial_take_profit": False,
            "partial_take_profit_ratio": 0.0,
            "force_close": False,
        }

        if current_pnl_pct >= self.break_even_trigger_pct:
            decision["move_stop_to_break_even"] = True
            decision["reason"] = "BREAK_EVEN_TRIGGERED"

        if current_pnl_pct >= self.partial_take_profit_trigger_pct:
            decision["partial_take_profit"] = True
            decision["partial_take_profit_ratio"] = self.partial_take_profit_ratio
            decision["action"] = "PARTIAL_TP"
            decision["reason"] = "PARTIAL_TAKE_PROFIT"

        if holding_seconds >= self.max_hold_seconds and current_signal_score < 0.50:
            decision["action"] = "CLOSE"
            decision["force_close"] = True
            decision["reason"] = "TIME_BASED_EXIT"

        if current_signal_score <= self.weak_signal_exit_score:
            decision["action"] = "CLOSE"
            decision["force_close"] = True
            decision["reason"] = "SIGNAL_WEAKENED_EXIT"

        if reverse_signal_score >= 0.72:
            decision["action"] = "CLOSE"
            decision["force_close"] = True
            decision["reason"] = "REVERSE_SIGNAL_EXIT"

        return decision
