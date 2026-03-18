from __future__ import annotations

from typing import Dict, Any


class PerformanceFeedbackLoop:
    """
    최근 성과를 바탕으로 threshold / risk / execution 성향을
    아주 완만하게 자동 미세조정한다.
    과도한 자동적응을 막기 위해 변화폭을 작게 제한한다.
    """

    def __init__(
        self,
        threshold_step: float = 0.01,
        risk_step: float = 0.05,
        execution_step: float = 0.05,
        max_threshold_bias: float = 0.04,
    ) -> None:
        self.threshold_step = float(threshold_step)
        self.risk_step = float(risk_step)
        self.execution_step = float(execution_step)
        self.max_threshold_bias = float(max_threshold_bias)

    def update(
        self,
        recent_trade_count: int,
        recent_win_rate: float,
        recent_avg_pnl_pct: float,
        loss_streak: int,
        current_threshold_bias: float = 0.0,
        current_risk_bias: float = 1.0,
        current_execution_aggressiveness: float = 0.60,
    ) -> Dict[str, Any]:
        recent_trade_count = int(recent_trade_count)
        recent_win_rate = float(recent_win_rate)
        recent_avg_pnl_pct = float(recent_avg_pnl_pct)
        loss_streak = int(loss_streak)
        current_threshold_bias = float(current_threshold_bias)
        current_risk_bias = float(current_risk_bias)
        current_execution_aggressiveness = float(current_execution_aggressiveness)

        threshold_bias = current_threshold_bias
        risk_bias = current_risk_bias
        execution_aggressiveness = current_execution_aggressiveness
        reason = "NO_CHANGE"

        if recent_trade_count < 20:
            return {
                "threshold_bias": round(threshold_bias, 4),
                "risk_bias": round(risk_bias, 4),
                "execution_aggressiveness": round(execution_aggressiveness, 4),
                "reason": "INSUFFICIENT_SAMPLE",
            }

        if recent_win_rate >= 0.58 and recent_avg_pnl_pct > 0:
            threshold_bias -= self.threshold_step
            risk_bias += self.risk_step
            execution_aggressiveness += self.execution_step
            reason = "POSITIVE_FEEDBACK"

        elif recent_win_rate < 0.45 or recent_avg_pnl_pct < 0:
            threshold_bias += self.threshold_step
            risk_bias -= self.risk_step
            execution_aggressiveness -= self.execution_step
            reason = "NEGATIVE_FEEDBACK"

        if loss_streak >= 3:
            threshold_bias += self.threshold_step
            risk_bias -= self.risk_step
            execution_aggressiveness -= self.execution_step
            reason = "LOSS_STREAK_FEEDBACK"

        threshold_bias = max(-self.max_threshold_bias, min(self.max_threshold_bias, threshold_bias))
        risk_bias = max(0.70, min(1.20, risk_bias))
        execution_aggressiveness = max(0.30, min(0.90, execution_aggressiveness))

        return {
            "threshold_bias": round(threshold_bias, 4),
            "risk_bias": round(risk_bias, 4),
            "execution_aggressiveness": round(execution_aggressiveness, 4),
            "reason": reason,
        }
