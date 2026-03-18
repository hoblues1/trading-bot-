from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, Any


@dataclass
class MetaStrategyDecision:
    meta_mode: str
    risk_bias: float
    threshold_bias: float
    execution_aggressiveness: float
    allow_new_entries: bool
    prefer_trend_strategies: bool
    prefer_mean_reversion: bool
    prefer_breakout: bool
    reason: str


class MetaStrategyController:
    """
    최상위 감독관.
    자본 / 최근 성과 / 장세 / 손실 흐름을 바탕으로
    시스템 전체 운영 성향을 다시 한 번 조정한다.
    """

    def __init__(
        self,
        max_loss_streak_pause: int = 6,
        severe_drawdown_pct: float = 0.10,
        soft_drawdown_pct: float = 0.05,
    ) -> None:
        self.max_loss_streak_pause = int(max_loss_streak_pause)
        self.severe_drawdown_pct = float(severe_drawdown_pct)
        self.soft_drawdown_pct = float(soft_drawdown_pct)

    def evaluate(
        self,
        capital_mode: str,
        regime: str,
        volatility_state: str,
        recent_win_rate: float,
        recent_pnl_pct: float,
        drawdown_pct: float,
        loss_streak: int,
    ) -> Dict[str, Any]:
        capital_mode = str(capital_mode).upper().strip()
        regime = str(regime).upper().strip()
        volatility_state = str(volatility_state).upper().strip()
        recent_win_rate = float(recent_win_rate)
        recent_pnl_pct = float(recent_pnl_pct)
        drawdown_pct = abs(float(drawdown_pct))
        loss_streak = int(loss_streak)

        if loss_streak >= self.max_loss_streak_pause or drawdown_pct >= self.severe_drawdown_pct:
            decision = MetaStrategyDecision(
                meta_mode="SURVIVAL",
                risk_bias=0.60,
                threshold_bias=0.05,
                execution_aggressiveness=0.35,
                allow_new_entries=False,
                prefer_trend_strategies=False,
                prefer_mean_reversion=False,
                prefer_breakout=False,
                reason="SEVERE_DRAWDOWN_OR_LOSS_STREAK",
            )
            return asdict(decision)

        if drawdown_pct >= self.soft_drawdown_pct or recent_win_rate < 0.42:
            decision = MetaStrategyDecision(
                meta_mode="DEFENSIVE",
                risk_bias=0.80,
                threshold_bias=0.03,
                execution_aggressiveness=0.45,
                allow_new_entries=True,
                prefer_trend_strategies=(regime == "TREND"),
                prefer_mean_reversion=(regime == "RANGE"),
                prefer_breakout=(volatility_state in {"HIGH", "EXTREME"}),
                reason="SOFT_DEFENSIVE_MODE",
            )
            return asdict(decision)

        if capital_mode == "GROWTH_AGGRESSIVE" and recent_win_rate >= 0.52 and recent_pnl_pct >= 0:
            decision = MetaStrategyDecision(
                meta_mode="EXPANSION",
                risk_bias=1.10,
                threshold_bias=-0.02,
                execution_aggressiveness=0.75,
                allow_new_entries=True,
                prefer_trend_strategies=(regime in {"TREND", "HIGH_VOL"}),
                prefer_mean_reversion=(regime == "RANGE"),
                prefer_breakout=(volatility_state in {"HIGH", "EXTREME"}),
                reason="CAPITAL_GROWTH_MODE",
            )
            return asdict(decision)

        decision = MetaStrategyDecision(
            meta_mode="BALANCED",
            risk_bias=1.00,
            threshold_bias=0.00,
            execution_aggressiveness=0.60,
            allow_new_entries=True,
            prefer_trend_strategies=(regime == "TREND"),
            prefer_mean_reversion=(regime == "RANGE"),
            prefer_breakout=(volatility_state in {"HIGH", "EXTREME"}),
            reason="NORMAL_BALANCED_MODE",
        )
        return asdict(decision)
