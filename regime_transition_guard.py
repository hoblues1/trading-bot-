from __future__ import annotations

from collections import deque
from typing import Dict, Any, Deque


class RegimeTransitionGuard:
    """
    장세 전환 구간 감시기.
    regime 변화가 잦거나 신뢰도가 낮을 때
    신규 진입을 잠시 억제한다.
    """

    def __init__(
        self,
        history_size: int = 10,
        unstable_transition_threshold: int = 4,
        freeze_seconds_on_transition: int = 10,
    ) -> None:
        self.history_size = int(history_size)
        self.unstable_transition_threshold = int(unstable_transition_threshold)
        self.freeze_seconds_on_transition = int(freeze_seconds_on_transition)
        self.regime_history: Deque[str] = deque(maxlen=self.history_size)

    def update(
        self,
        regime: str,
        volatility_state: str,
        trend_strength: float,
        confidence: float,
    ) -> Dict[str, Any]:
        regime = str(regime).upper().strip()
        volatility_state = str(volatility_state).upper().strip()
        trend_strength = min(max(float(trend_strength), 0.0), 1.0)
        confidence = min(max(float(confidence), 0.0), 1.0)

        prev_regime = self.regime_history[-1] if self.regime_history else regime
        self.regime_history.append(regime)

        transition_count = self._count_transitions()
        changed = regime != prev_regime

        unstable = (
            transition_count >= self.unstable_transition_threshold
            or volatility_state in {"EXTREME", "PANIC"}
            or confidence < 0.40
        )

        block_new_entries = False
        reason = "STABLE"

        if changed and unstable:
            block_new_entries = True
            reason = "UNSTABLE_REGIME_TRANSITION"
        elif changed and trend_strength < 0.40:
            block_new_entries = True
            reason = "WEAK_TREND_DURING_TRANSITION"
        elif confidence < 0.30:
            block_new_entries = True
            reason = "LOW_REGIME_CONFIDENCE"

        return {
            "current_regime": regime,
            "previous_regime": prev_regime,
            "changed": changed,
            "transition_count": transition_count,
            "unstable": unstable,
            "block_new_entries": block_new_entries,
            "freeze_seconds": self.freeze_seconds_on_transition if block_new_entries else 0,
            "reason": reason,
        }

    def _count_transitions(self) -> int:
        if len(self.regime_history) <= 1:
            return 0

        transitions = 0
        prev = None
        for r in self.regime_history:
            if prev is not None and r != prev:
                transitions += 1
            prev = r
        return transitions
