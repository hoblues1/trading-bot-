from __future__ import annotations

from typing import Dict, Any


class AdaptiveThresholdEngine:
    """
    자본 모드 / 장 상태 / 최근 성과를 반영하여
    진입 threshold를 동적으로 조정한다.
    완화형 시작값 기준.
    """

    def __init__(
        self,
        base_buy_threshold: float = 0.70,
        base_sell_threshold: float = 0.70,
        base_min_agree_count: int = 2,
    ) -> None:
        self.base_buy_threshold = float(base_buy_threshold)
        self.base_sell_threshold = float(base_sell_threshold)
        self.base_min_agree_count = int(base_min_agree_count)

    def compute(
        self,
        capital_mode: str,
        regime: str,
        volatility_state: str,
        recent_win_rate: float = 0.50,
        recent_pnl_pct: float = 0.0,
        loss_streak: int = 0,
    ) -> Dict[str, Any]:
        capital_mode = str(capital_mode).upper().strip()
        regime = str(regime).upper().strip()
        volatility_state = str(volatility_state).upper().strip()
        recent_win_rate = float(recent_win_rate)
        recent_pnl_pct = float(recent_pnl_pct)
        loss_streak = int(loss_streak)

        buy = self.base_buy_threshold
        sell = self.base_sell_threshold
        agree = self.base_min_agree_count
        cooldown = 12

        # capital mode
        if capital_mode == "GROWTH_AGGRESSIVE":
            buy -= 0.04
            sell -= 0.04
            agree = max(2, agree)
            cooldown = 8
        elif capital_mode == "BALANCED_GROWTH":
            buy -= 0.01
            sell -= 0.01
            cooldown = 10
        elif capital_mode == "BALANCED":
            cooldown = 15
        elif capital_mode in {"DEFENSIVE", "CAPITAL_PRESERVATION"}:
            buy += 0.04
            sell += 0.04
            agree += 1
            cooldown = 20

        # regime
        if regime == "TREND":
            buy -= 0.01
            sell -= 0.01
        elif regime == "RANGE":
            buy += 0.02
            sell += 0.02
        elif regime == "HIGH_VOL":
            buy += 0.01
            sell += 0.01
        elif regime == "LOW_VOL":
            buy += 0.015
            sell += 0.015

        # volatility
        if volatility_state in {"EXTREME", "PANIC"}:
            buy += 0.03
            sell += 0.03
            agree += 1
            cooldown += 5

        # performance adaptation
        if recent_win_rate >= 0.60 and recent_pnl_pct > 0:
            buy -= 0.005
            sell -= 0.005
        elif recent_win_rate < 0.45 or recent_pnl_pct < 0:
            buy += 0.015
            sell += 0.015
            cooldown += 3

        if loss_streak >= 3:
            buy += 0.02
            sell += 0.02
            agree += 1
            cooldown += 5

        buy = min(max(buy, 0.55), 0.92)
        sell = min(max(sell, 0.55), 0.92)
        agree = max(2, min(agree, 5))
        cooldown = max(5, cooldown)

        return {
            "buy_threshold": round(buy, 4),
            "sell_threshold": round(sell, 4),
            "min_agree_count": agree,
            "cooldown_seconds": cooldown,
            "capital_mode": capital_mode,
            "regime": regime,
            "volatility_state": volatility_state,
            "recent_win_rate": recent_win_rate,
            "recent_pnl_pct": recent_pnl_pct,
            "loss_streak": loss_streak,
        }
