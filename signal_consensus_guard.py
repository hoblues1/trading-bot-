from __future__ import annotations

from typing import Dict, Any


class SignalConsensusGuard:
    """
    여러 전략의 최종 의견이 너무 충돌할 때
    무리한 진입을 막는 합의도 감시기.
    alpha fusion 이전/이후 둘 다 활용 가능하다.
    """

    def __init__(
        self,
        min_consensus_score: float = 0.52,
        strong_consensus_score: float = 0.68,
        max_opposition_score: float = 0.58,
    ) -> None:
        self.min_consensus_score = float(min_consensus_score)
        self.strong_consensus_score = float(strong_consensus_score)
        self.max_opposition_score = float(max_opposition_score)

    def evaluate(
        self,
        buy_score: float,
        sell_score: float,
        agree_count: int,
        total_strategy_count: int,
    ) -> Dict[str, Any]:
        buy_score = float(buy_score)
        sell_score = float(sell_score)
        agree_count = int(agree_count)
        total_strategy_count = max(1, int(total_strategy_count))

        dominant_score = max(buy_score, sell_score)
        opposition_score = min(buy_score, sell_score)
        consensus_ratio = agree_count / total_strategy_count

        allow_entry = True
        reason = "CONSENSUS_OK"
        strength = "NORMAL"

        if dominant_score < self.min_consensus_score:
            allow_entry = False
            reason = "DOMINANT_SCORE_TOO_LOW"
            strength = "WEAK"
        elif opposition_score > self.max_opposition_score and consensus_ratio < 0.60:
            allow_entry = False
            reason = "SIGNAL_CONFLICT_TOO_HIGH"
            strength = "CONFLICT"
        elif dominant_score >= self.strong_consensus_score and consensus_ratio >= 0.60:
            strength = "STRONG"

        return {
            "allow_entry": allow_entry,
            "reason": reason,
            "strength": strength,
            "dominant_score": round(dominant_score, 4),
            "opposition_score": round(opposition_score, 4),
            "consensus_ratio": round(consensus_ratio, 4),
        }
