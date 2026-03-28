from __future__ import annotations

from typing import Dict, Any


class StrategyWeightOptimizer:
    """
    최근 전략 성과를 바탕으로
    전략별 가중치를 자동 조정한다.
    초반 과적응 방지를 위해 완만하게 반영.
    """

    def __init__(
        self,
        min_weight: float = 0.08,
        max_weight: float = 0.45,
        adjustment_strength: float = 0.06,
        min_sample_count: int = 30,
    ) -> None:
        self.min_weight = float(min_weight)
        self.max_weight = float(max_weight)
        self.adjustment_strength = float(adjustment_strength)
        self.min_sample_count = int(min_sample_count)

    def optimize(
        self,
        current_weights: Dict[str, float],
        performance_by_strategy: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        if not current_weights:
            return {
                "weights": {},
                "reason": "NO_CURRENT_WEIGHTS",
            }

        raw_scores: Dict[str, float] = {}

        for strategy_name, old_weight in current_weights.items():
            perf = performance_by_strategy.get(strategy_name.upper())
            if perf is None:
                perf = performance_by_strategy.get(strategy_name)

            if perf is None:
                raw_scores[strategy_name] = max(old_weight, self.min_weight)
                continue

            win_rate = float(perf.get("win_rate", 0.50))
            avg_pnl_pct = float(perf.get("avg_pnl_pct", 0.0))
            count = int(perf.get("count", 0))

            if count < self.min_sample_count:
                raw_scores[strategy_name] = old_weight
                continue

            sample_factor = min(count / 50.0, 1.0)
            edge_score = (
                (win_rate - 0.50) * 1.4
                + avg_pnl_pct * 6.0
            ) * sample_factor

            new_weight = old_weight * (1.0 + edge_score * self.adjustment_strength)
            raw_scores[strategy_name] = new_weight

        normalized = self._normalize(raw_scores)

        return {
            "weights": {k: round(v, 6) for k, v in normalized.items()},
            "reason": "OPTIMIZED",
        }

    def _normalize(self, weights: Dict[str, float]) -> Dict[str, float]:
        clipped = {}
        for name, value in weights.items():
            clipped[name] = min(max(float(value), self.min_weight), self.max_weight)

        total = sum(clipped.values())
        if total <= 0:
            equal_weight = 1.0 / max(1, len(clipped))
            return {k: equal_weight for k in clipped}

        return {k: v / total for k, v in clipped.items()}
