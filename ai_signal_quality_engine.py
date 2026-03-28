from __future__ import annotations

from typing import Dict, Any


class AISignalQualityEngine:
    """
    초기엔 하드 차단기가 아니라 점수 보정용 보조 엔진으로 쓰는 것을 권장.
    나중에 ML 추론기로 교체 가능하도록 설계.
    """

    def __init__(
        self,
        min_quality_threshold: float = 0.55,
        high_fake_signal_risk_threshold: float = 0.70,
    ) -> None:
        self.min_quality_threshold = float(min_quality_threshold)
        self.high_fake_signal_risk_threshold = float(high_fake_signal_risk_threshold)

    def evaluate(
        self,
        orderflow_score: float,
        microstructure_score: float,
        velocity_score: float,
        volatility_score: float,
        imbalance_score: float,
        spread_pct: float,
        depth_ratio: float,
        regime: str = "UNKNOWN",
    ) -> Dict[str, Any]:
        orderflow_score = self._clip(orderflow_score)
        microstructure_score = self._clip(microstructure_score)
        velocity_score = self._clip(velocity_score)
        volatility_score = self._clip(volatility_score)
        imbalance_score = self._clip(imbalance_score)
        spread_pct = max(0.0, float(spread_pct))
        depth_ratio = max(0.0, float(depth_ratio))
        regime = str(regime).upper().strip()

        core_strength = (
            0.24 * orderflow_score
            + 0.24 * microstructure_score
            + 0.16 * velocity_score
            + 0.14 * volatility_score
            + 0.22 * imbalance_score
        )

        spread_penalty = min(spread_pct * 40.0, 0.18)
        depth_bonus = min(depth_ratio * 0.08, 0.08)

        regime_bonus = 0.0
        if regime == "TREND":
            regime_bonus = 0.025
        elif regime == "RANGE":
            regime_bonus = -0.015
        elif regime == "HIGH_VOL":
            regime_bonus = -0.005

        quality_score = core_strength - spread_penalty + depth_bonus + regime_bonus
        quality_score = self._clip(quality_score)

        fake_signal_risk = 1.0 - quality_score
        if spread_pct > 0.0025:
            fake_signal_risk += 0.06
        if depth_ratio < 0.80:
            fake_signal_risk += 0.06
        fake_signal_risk = self._clip(fake_signal_risk)

        allow_signal = (
            quality_score >= self.min_quality_threshold
            and fake_signal_risk < self.high_fake_signal_risk_threshold
        )

        confidence = self._clip((quality_score * 0.7) + ((1.0 - fake_signal_risk) * 0.3))

        # 초기엔 fusion 점수 보정용으로 쓰기 쉽게 bonus/penalty 제공
        fusion_bonus = 0.0
        if quality_score >= 0.75 and fake_signal_risk <= 0.30:
            fusion_bonus = 0.04
        elif quality_score >= 0.65 and fake_signal_risk <= 0.40:
            fusion_bonus = 0.02
        elif quality_score < 0.50:
            fusion_bonus = -0.04
        elif fake_signal_risk > 0.70:
            fusion_bonus = -0.05

        return {
            "quality_score": round(quality_score, 4),
            "fake_signal_risk": round(fake_signal_risk, 4),
            "confidence": round(confidence, 4),
            "allow_signal": allow_signal,
            "fusion_bonus": round(fusion_bonus, 4),
            "reason": "ALLOW" if allow_signal else "LOW_QUALITY_OR_HIGH_RISK",
        }

    def _clip(self, value: float) -> float:
        return min(max(float(value), 0.0), 1.0)
