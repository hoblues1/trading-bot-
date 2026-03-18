from __future__ import annotations

from typing import Dict, Any


class ExecutionAlphaCoordinator:
    """
    진입 실행 자체를 전략화하는 보조 엔진.
    - 지금 바로 진입할지
    - 지정가/시장가 어떤 쪽이 나은지
    - 분할 진입할지
    - 잠깐 대기할지
    결정한다.
    """

    def __init__(
        self,
        max_spread_pct_for_market: float = 0.0018,
        max_spread_pct_for_limit: float = 0.0035,
        min_signal_score_for_market: float = 0.82,
        min_depth_ratio: float = 0.75,
    ) -> None:
        self.max_spread_pct_for_market = float(max_spread_pct_for_market)
        self.max_spread_pct_for_limit = float(max_spread_pct_for_limit)
        self.min_signal_score_for_market = float(min_signal_score_for_market)
        self.min_depth_ratio = float(min_depth_ratio)

    def decide(
        self,
        final_signal_score: float,
        spread_pct: float,
        depth_ratio: float,
        orderflow_strength: float,
        volatility_score: float,
        execution_aggressiveness: float,
        signal_confidence: float,
    ) -> Dict[str, Any]:
        final_signal_score = float(final_signal_score)
        spread_pct = float(spread_pct)
        depth_ratio = float(depth_ratio)
        orderflow_strength = float(orderflow_strength)
        volatility_score = float(volatility_score)
        execution_aggressiveness = float(execution_aggressiveness)
        signal_confidence = float(signal_confidence)

        urgency = (
            0.35 * final_signal_score
            + 0.20 * signal_confidence
            + 0.20 * orderflow_strength
            + 0.10 * execution_aggressiveness
            + 0.15 * volatility_score
        )

        if spread_pct > self.max_spread_pct_for_limit:
            return {
                "allow_entry": False,
                "recommended_action": "WAIT",
                "order_type": "NONE",
                "slice_count": 0,
                "reason": "SPREAD_TOO_WIDE",
            }

        if depth_ratio < self.min_depth_ratio and final_signal_score < 0.85:
            return {
                "allow_entry": False,
                "recommended_action": "WAIT",
                "order_type": "NONE",
                "slice_count": 0,
                "reason": "INSUFFICIENT_DEPTH",
            }

        if (
            final_signal_score >= self.min_signal_score_for_market
            and spread_pct <= self.max_spread_pct_for_market
            and urgency >= 0.78
        ):
            return {
                "allow_entry": True,
                "recommended_action": "ENTER_NOW",
                "order_type": "MARKET",
                "slice_count": 1 if urgency > 0.90 else 2,
                "reason": "HIGH_URGENCY_MARKET_ENTRY",
            }

        if urgency >= 0.65:
            return {
                "allow_entry": True,
                "recommended_action": "ENTER_SMART",
                "order_type": "LIMIT",
                "slice_count": 2,
                "reason": "SMART_LIMIT_ENTRY",
            }

        return {
            "allow_entry": True,
            "recommended_action": "PASSIVE_ENTRY",
            "order_type": "LIMIT",
            "slice_count": 3,
            "reason": "LOW_URGENCY_PASSIVE_ENTRY",
        }
