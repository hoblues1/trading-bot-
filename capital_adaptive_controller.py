from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, Any


@dataclass
class CapitalModeConfig:
    mode: str
    leverage_cap: int
    max_positions: int
    risk_per_trade: float
    alpha_threshold: float
    min_agree_count: int
    cooldown_seconds: int
    allow_aggressive_entries: bool
    partial_take_profit_enabled: bool
    break_even_enabled: bool


class CapitalAdaptiveController:
    """
    계좌 규모 / 최근 성과 / 드로우다운 / 장 상태를 바탕으로
    시스템 전체 운영 모드를 자동 조정하는 감독관.
    """

    def __init__(
        self,
        small_capital_threshold: float = 80.0,
        medium_capital_threshold: float = 300.0,
        large_capital_threshold: float = 1500.0,
        max_defensive_drawdown_pct: float = 0.07,
        loss_streak_defensive_trigger: int = 4,
    ) -> None:
        self.small_capital_threshold = float(small_capital_threshold)
        self.medium_capital_threshold = float(medium_capital_threshold)
        self.large_capital_threshold = float(large_capital_threshold)
        self.max_defensive_drawdown_pct = float(max_defensive_drawdown_pct)
        self.loss_streak_defensive_trigger = int(loss_streak_defensive_trigger)

    def evaluate(
        self,
        capital: float,
        recent_win_rate: float = 0.50,
        recent_pnl_pct: float = 0.0,
        current_drawdown_pct: float = 0.0,
        loss_streak: int = 0,
        regime: str = "UNKNOWN",
        volatility_state: str = "NORMAL",
    ) -> Dict[str, Any]:
        capital = float(capital)
        recent_win_rate = float(recent_win_rate)
        recent_pnl_pct = float(recent_pnl_pct)
        current_drawdown_pct = abs(float(current_drawdown_pct))
        loss_streak = int(loss_streak)
        regime = str(regime).upper().strip()
        volatility_state = str(volatility_state).upper().strip()

        base_mode = self._determine_base_mode(capital)
        final_mode = self._refine_mode(
            base_mode=base_mode,
            recent_win_rate=recent_win_rate,
            recent_pnl_pct=recent_pnl_pct,
            current_drawdown_pct=current_drawdown_pct,
            loss_streak=loss_streak,
            regime=regime,
            volatility_state=volatility_state,
        )
        config = self._build_mode_config(final_mode)

        result = asdict(config)
        result.update(
            {
                "capital": capital,
                "recent_win_rate": recent_win_rate,
                "recent_pnl_pct": recent_pnl_pct,
                "current_drawdown_pct": current_drawdown_pct,
                "loss_streak": loss_streak,
                "regime": regime,
                "volatility_state": volatility_state,
                "base_mode": base_mode,
            }
        )
        return result

    def _determine_base_mode(self, capital: float) -> str:
        if capital < self.small_capital_threshold:
            return "GROWTH_AGGRESSIVE"
        if capital < self.medium_capital_threshold:
            return "BALANCED_GROWTH"
        if capital < self.large_capital_threshold:
            return "BALANCED"
        return "CAPITAL_PRESERVATION"

    def _refine_mode(
        self,
        base_mode: str,
        recent_win_rate: float,
        recent_pnl_pct: float,
        current_drawdown_pct: float,
        loss_streak: int,
        regime: str,
        volatility_state: str,
    ) -> str:
        if (
            current_drawdown_pct >= self.max_defensive_drawdown_pct
            or loss_streak >= self.loss_streak_defensive_trigger
        ):
            return "DEFENSIVE"

        if volatility_state in {"EXTREME", "PANIC"}:
            return "DEFENSIVE"

        if base_mode == "GROWTH_AGGRESSIVE":
            if regime in {"RANGE", "LOW_VOL"} and recent_win_rate < 0.45:
                return "BALANCED_GROWTH"
            return "GROWTH_AGGRESSIVE"

        if base_mode == "BALANCED_GROWTH":
            if recent_win_rate < 0.42 and recent_pnl_pct < 0:
                return "BALANCED"
            return "BALANCED_GROWTH"

        if base_mode == "BALANCED":
            if recent_win_rate < 0.40 and recent_pnl_pct < 0:
                return "DEFENSIVE"
            return "BALANCED"

        return "CAPITAL_PRESERVATION"

    def _build_mode_config(self, mode: str) -> CapitalModeConfig:
        if mode == "GROWTH_AGGRESSIVE":
            return CapitalModeConfig(
                mode=mode,
                leverage_cap=10,
                max_positions=3,
                risk_per_trade=0.025,
                alpha_threshold=0.68,
                min_agree_count=2,
                cooldown_seconds=8,
                allow_aggressive_entries=True,
                partial_take_profit_enabled=True,
                break_even_enabled=True,
            )

        if mode == "BALANCED_GROWTH":
            return CapitalModeConfig(
                mode=mode,
                leverage_cap=8,
                max_positions=3,
                risk_per_trade=0.018,
                alpha_threshold=0.72,
                min_agree_count=2,
                cooldown_seconds=12,
                allow_aggressive_entries=False,
                partial_take_profit_enabled=True,
                break_even_enabled=True,
            )

        if mode == "BALANCED":
            return CapitalModeConfig(
                mode=mode,
                leverage_cap=6,
                max_positions=4,
                risk_per_trade=0.012,
                alpha_threshold=0.77,
                min_agree_count=3,
                cooldown_seconds=18,
                allow_aggressive_entries=False,
                partial_take_profit_enabled=True,
                break_even_enabled=True,
            )

        if mode == "DEFENSIVE":
            return CapitalModeConfig(
                mode=mode,
                leverage_cap=4,
                max_positions=2,
                risk_per_trade=0.007,
                alpha_threshold=0.82,
                min_agree_count=3,
                cooldown_seconds=20,
                allow_aggressive_entries=False,
                partial_take_profit_enabled=True,
                break_even_enabled=True,
            )

        return CapitalModeConfig(
            mode="CAPITAL_PRESERVATION",
            leverage_cap=3,
            max_positions=3,
            risk_per_trade=0.005,
            alpha_threshold=0.84,
            min_agree_count=3,
            cooldown_seconds=22,
            allow_aggressive_entries=False,
            partial_take_profit_enabled=True,
            break_even_enabled=True,
        )
