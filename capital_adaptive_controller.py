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

    설계 원칙
    1) 초소액 구간에서는 공격보다 생존 우선
    2) 200 USDT 전후를 첫 번째 승격 구간으로 설정
    3) 손실 연속 / 드로우다운 / 극단 변동성에서는 자동 방어 전환
    4) 계좌가 커질수록 분산과 안정성 비중 증가
    5) capital_mode는 risk/sizing 전용
    6) alpha_threshold / agree / cooldown 값은 반환은 유지하되 main에서 전략 덮어쓰기 금지
    """

    def __init__(
        self,
        micro_capital_threshold: float = 200.0,
        small_capital_threshold: float = 1000.0,
        medium_capital_threshold: float = 10000.0,
        max_defensive_drawdown_pct: float = 0.07,
        loss_streak_defensive_trigger: int = 4,
    ) -> None:
        self.micro_capital_threshold = float(micro_capital_threshold)
        self.small_capital_threshold = float(small_capital_threshold)
        self.medium_capital_threshold = float(medium_capital_threshold)
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
            capital=capital,
            recent_win_rate=recent_win_rate,
            recent_pnl_pct=recent_pnl_pct,
            current_drawdown_pct=current_drawdown_pct,
            loss_streak=loss_streak,
            regime=regime,
            volatility_state=volatility_state,
        )
        config = self._build_mode_config(final_mode, capital=capital)

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
        if capital < self.micro_capital_threshold:
            return "SURVIVAL"
        if capital < self.small_capital_threshold:
            return "MICRO_COMPOUND"
        if capital < self.medium_capital_threshold:
            return "ADAPTIVE_GROWTH"
        return "CAPITAL_PRESERVATION"

    def _refine_mode(
        self,
        base_mode: str,
        capital: float,
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

        if base_mode == "SURVIVAL":
            if regime in {"RANGE", "LOW_VOL", "CHOPPY"} and recent_win_rate < 0.50:
                return "SURVIVAL"
            if recent_win_rate >= 0.58 and recent_pnl_pct > 0.02:
                return "MICRO_COMPOUND"
            return "SURVIVAL"

        if base_mode == "MICRO_COMPOUND":
            if recent_win_rate < 0.44 and recent_pnl_pct < 0:
                return "SURVIVAL"
            if recent_win_rate >= 0.56 and recent_pnl_pct > 0.03 and regime in {"TREND_UP", "TREND_DOWN", "TREND"}:
                return "ADAPTIVE_GROWTH"
            return "MICRO_COMPOUND"

        if base_mode == "ADAPTIVE_GROWTH":
            if recent_win_rate < 0.43 and recent_pnl_pct < 0:
                return "MICRO_COMPOUND"
            return "ADAPTIVE_GROWTH"

        return "CAPITAL_PRESERVATION"

    def _build_mode_config(self, mode: str, capital: float) -> CapitalModeConfig:
        if mode == "SURVIVAL":
            return CapitalModeConfig(
                mode=mode,
                leverage_cap=6,
                max_positions=1,
                risk_per_trade=0.0105,
                alpha_threshold=0.58,
                min_agree_count=2,
                cooldown_seconds=9,
                allow_aggressive_entries=False,
                partial_take_profit_enabled=True,
                break_even_enabled=True,
            )

        if mode == "MICRO_COMPOUND":
            return CapitalModeConfig(
                mode=mode,
                leverage_cap=7,
                max_positions=1,
                risk_per_trade=0.0125,
                alpha_threshold=0.58,
                min_agree_count=2,
                cooldown_seconds=9,
                allow_aggressive_entries=False,
                partial_take_profit_enabled=True,
                break_even_enabled=True,
            )

        if mode == "ADAPTIVE_GROWTH":
            return CapitalModeConfig(
                mode=mode,
                leverage_cap=8,
                max_positions=2,
                risk_per_trade=0.0140,
                alpha_threshold=0.58,
                min_agree_count=2,
                cooldown_seconds=9,
                allow_aggressive_entries=False,
                partial_take_profit_enabled=True,
                break_even_enabled=True,
            )

        if mode == "DEFENSIVE":
            return CapitalModeConfig(
                mode=mode,
                leverage_cap=4,
                max_positions=1,
                risk_per_trade=0.0060,
                alpha_threshold=0.58,
                min_agree_count=2,
                cooldown_seconds=9,
                allow_aggressive_entries=False,
                partial_take_profit_enabled=True,
                break_even_enabled=True,
            )

        return CapitalModeConfig(
            mode="CAPITAL_PRESERVATION",
            leverage_cap=5,
            max_positions=2,
            risk_per_trade=0.0075,
            alpha_threshold=0.58,
            min_agree_count=2,
            cooldown_seconds=9,
            allow_aggressive_entries=False,
            partial_take_profit_enabled=True,
            break_even_enabled=True,
        )