import time
from collections import deque
from typing import Dict, Deque, Optional, Any


class MicrostructureAlpha:
    """
    High-end microstructure alpha engine

    목표:
    1) 과거 전체 누적이 아니라 최근 체결 흐름만 반영
    2) 작은 노이즈 신호 제거
    3) 같은 방향 연속 신호 폭발 방지
    4) 직전 신호 직후 재발사 방지
    5) 체결 수가 적은 왜곡 구간 필터링
    6) 순매수/순매도 우위 + 총 거래량 + 체결 수 + 우세 비율을 동시에 반영
    7) 디버깅 가능한 메타데이터 제공

    실전 수정 포인트:
    - 왜 signal이 안 나오는지 SKIP 사유를 로그로 확인 가능
    - threshold는 소폭만 유연화
    - 쿨다운 과차단 완화
    - 품질 기반 통과는 유지
    """

    def __init__(
        self,
        window_seconds: float = 6.0,
        imbalance_threshold: float = 1.12,
        min_total_flow: float = 0.008,
        min_trade_count: int = 3,
        min_net_flow_ratio: float = 0.065,
        signal_cooldown_seconds: float = 1.2,
        same_side_rearm_seconds: float = 2.6,
        opposite_side_confirmation_ratio: float = 1.04,
        reset_on_signal: bool = False,
        max_stored_trades_per_symbol: int = 2000,
        debug: bool = False,
    ):
        self.window_seconds = float(window_seconds)
        self.imbalance_threshold = float(imbalance_threshold)
        self.min_total_flow = float(min_total_flow)
        self.min_trade_count = int(min_trade_count)
        self.min_net_flow_ratio = float(min_net_flow_ratio)
        self.signal_cooldown_seconds = float(signal_cooldown_seconds)
        self.same_side_rearm_seconds = float(same_side_rearm_seconds)
        self.opposite_side_confirmation_ratio = float(opposite_side_confirmation_ratio)
        self.reset_on_signal = bool(reset_on_signal)
        self.max_stored_trades_per_symbol = int(max_stored_trades_per_symbol)
        self.debug = bool(debug)

        # symbol -> deque[(timestamp, side, qty)]
        self.trades: Dict[str, Deque] = {}

        # signal state
        self.last_signal_ts: Dict[str, float] = {}
        self.last_signal_side: Dict[str, str] = {}

    def _log(self, msg: str) -> None:
        if self.debug:
            print(msg)

    def _get_deque(self, symbol: str) -> Deque:
        if symbol not in self.trades:
            self.trades[symbol] = deque()
        return self.trades[symbol]

    def _prune(self, symbol: str, now: float) -> None:
        dq = self._get_deque(symbol)
        cutoff = now - self.window_seconds

        while dq and dq[0][0] < cutoff:
            dq.popleft()

        while len(dq) > self.max_stored_trades_per_symbol:
            dq.popleft()

    def update(self, trade: Dict[str, Any]) -> None:
        """
        trade 예시:
        {
            "symbol": "BTCUSDT",
            "side": "BUY" or "SELL",
            "qty": 12.34,
            "timestamp": 1710000000.123   # optional
        }
        """
        symbol = trade.get("symbol")
        side = trade.get("side")

        try:
            qty = float(trade.get("qty", 0.0))
        except Exception:
            qty = 0.0

        now = float(trade.get("timestamp", time.time()))

        if not symbol or side not in ("BUY", "SELL"):
            return

        if qty <= 0:
            return

        dq = self._get_deque(symbol)
        dq.append((now, side, qty))
        self._prune(symbol, now)

    def _aggregate(self, symbol: str, now: float) -> Dict[str, Any]:
        self._prune(symbol, now)
        dq = self._get_deque(symbol)

        buy_flow = 0.0
        sell_flow = 0.0
        buy_count = 0
        sell_count = 0
        buy_sizes = []
        sell_sizes = []

        for ts, side, qty in dq:
            if side == "BUY":
                buy_flow += qty
                buy_count += 1
                buy_sizes.append(qty)
            else:
                sell_flow += qty
                sell_count += 1
                sell_sizes.append(qty)

        total_flow = buy_flow + sell_flow
        total_count = buy_count + sell_count
        net_flow = buy_flow - sell_flow
        net_flow_ratio = (net_flow / total_flow) if total_flow > 0 else 0.0

        buy_sell_ratio = buy_flow / max(sell_flow, 1e-9)
        sell_buy_ratio = sell_flow / max(buy_flow, 1e-9)

        avg_buy_size = (buy_flow / buy_count) if buy_count > 0 else 0.0
        avg_sell_size = (sell_flow / sell_count) if sell_count > 0 else 0.0
        avg_trade_size = (total_flow / total_count) if total_count > 0 else 0.0

        count_imbalance_ratio = abs(buy_count - sell_count) / max(total_count, 1)

        return {
            "symbol": symbol,
            "buy_flow": buy_flow,
            "sell_flow": sell_flow,
            "total_flow": total_flow,
            "net_flow": net_flow,
            "net_flow_ratio": net_flow_ratio,
            "buy_count": buy_count,
            "sell_count": sell_count,
            "total_count": total_count,
            "buy_sell_ratio": buy_sell_ratio,
            "sell_buy_ratio": sell_buy_ratio,
            "avg_buy_size": avg_buy_size,
            "avg_sell_size": avg_sell_size,
            "avg_trade_size": avg_trade_size,
            "count_imbalance_ratio": count_imbalance_ratio,
            "window_seconds": self.window_seconds,
        }

    def _dynamic_thresholds(self, stats: Dict[str, Any]) -> Dict[str, float]:
        """
        시장 미세구조 품질에 따라 threshold를 미세 조정.
        좋은 흐름이면 약간 완화하고,
        체결이 빈약하면 다시 보수적으로 유지.
        """
        total_flow = stats["total_flow"]
        total_count = stats["total_count"]
        avg_trade_size = stats["avg_trade_size"]
        count_imbalance_ratio = stats["count_imbalance_ratio"]

        imbalance_threshold = self.imbalance_threshold
        min_net_flow_ratio = self.min_net_flow_ratio

        # 흐름/체결이 충분하면 소폭 완화
        if total_flow >= self.min_total_flow * 1.5 and total_count >= self.min_trade_count * 1.4:
            imbalance_threshold -= 0.08
            min_net_flow_ratio -= 0.015

        # 체결 카운트가 매우 좋고 count imbalance도 어느 정도 있으면 추가 완화
        if total_count >= self.min_trade_count * 2.0 and count_imbalance_ratio >= 0.22:
            imbalance_threshold -= 0.04

        # 평균 체결 크기가 너무 작으면 다시 보수화
        baseline_avg = self.min_total_flow / max(self.min_trade_count, 1)
        if avg_trade_size > 0 and avg_trade_size < baseline_avg * 0.50:
            imbalance_threshold += 0.05
            min_net_flow_ratio += 0.015

        return {
            "imbalance_threshold": max(1.05, float(imbalance_threshold)),
            "min_net_flow_ratio": max(0.05, float(min_net_flow_ratio)),
        }

    def _flow_quality_score(self, stats: Dict[str, Any], side: str) -> float:
        """
        단순 비율이 아니라, 순우위 + 총흐름 + 체결수 + 카운트 균형을 종합 반영.
        """
        total_flow = stats["total_flow"]
        total_count = stats["total_count"]
        net_flow_ratio = abs(stats["net_flow_ratio"])
        count_imbalance_ratio = stats["count_imbalance_ratio"]

        if side == "BUY":
            dominance_ratio = stats["buy_sell_ratio"]
            avg_dom_size = stats["avg_buy_size"]
            avg_opp_size = stats["avg_sell_size"]
        else:
            dominance_ratio = stats["sell_buy_ratio"]
            avg_dom_size = stats["avg_sell_size"]
            avg_opp_size = stats["avg_buy_size"]

        flow_score = min(1.0, total_flow / max(self.min_total_flow * 2.0, 1e-9))
        count_score = min(1.0, total_count / max(self.min_trade_count * 1.8, 1e-9))
        net_score = min(1.0, net_flow_ratio / max(self.min_net_flow_ratio * 2.0, 1e-9))
        dominance_score = min(1.0, dominance_ratio / max(self.imbalance_threshold * 1.5, 1e-9))

        size_edge = 0.0
        if avg_dom_size > 0 and avg_opp_size > 0:
            size_edge = min(1.0, avg_dom_size / max(avg_opp_size, 1e-9)) - 1.0
            size_edge = max(0.0, min(size_edge, 1.0))
        elif avg_dom_size > 0 and avg_opp_size == 0:
            size_edge = 0.5

        quality = (
            dominance_score * 0.30 +
            net_score * 0.27 +
            flow_score * 0.21 +
            count_score * 0.14 +
            count_imbalance_ratio * 0.04 +
            size_edge * 0.04
        )

        return float(max(0.0, min(quality, 1.0)))

    def _blocked_by_cooldown(
        self,
        symbol: str,
        side: str,
        now: float,
        stats: Dict[str, Any],
        quality: float
    ) -> bool:
        last_ts = self.last_signal_ts.get(symbol)
        last_side = self.last_signal_side.get(symbol)

        if last_ts is None or last_side is None:
            return False

        elapsed = now - last_ts

        # 전체 쿨다운
        if elapsed < self.signal_cooldown_seconds:
            self._log(
                f"[MICRO BLOCK] {symbol} cooldown elapsed={elapsed:.3f} < {self.signal_cooldown_seconds:.3f}"
            )
            return True

        # 같은 방향 연속 신호는 quality가 좋을수록 조금 빨리 재무장 허용
        same_side_rearm = self.same_side_rearm_seconds
        if quality >= 0.82:
            same_side_rearm *= 0.62
        elif quality >= 0.72:
            same_side_rearm *= 0.76

        if side == last_side and elapsed < same_side_rearm:
            self._log(
                f"[MICRO BLOCK] {symbol} same_side_rearm elapsed={elapsed:.3f} < {same_side_rearm:.3f}"
            )
            return True

        # 반대 방향은 충분한 반전 확인이 있을 때만 허용
        if side != last_side:
            required = self.imbalance_threshold * self.opposite_side_confirmation_ratio

            if quality >= 0.85:
                required *= 0.97

            if side == "BUY":
                if stats["buy_sell_ratio"] < required:
                    self._log(
                        f"[MICRO BLOCK] {symbol} reverse_buy_block ratio={stats['buy_sell_ratio']:.6f} required={required:.6f}"
                    )
                    return True
            else:
                if stats["sell_buy_ratio"] < required:
                    self._log(
                        f"[MICRO BLOCK] {symbol} reverse_sell_block ratio={stats['sell_buy_ratio']:.6f} required={required:.6f}"
                    )
                    return True

        return False

    def signal(self, symbol: str) -> Optional[Dict[str, Any]]:
        now = time.time()
        stats = self._aggregate(symbol, now)

        buy_flow = stats["buy_flow"]
        sell_flow = stats["sell_flow"]
        total_flow = stats["total_flow"]
        total_count = stats["total_count"]
        net_flow_ratio = stats["net_flow_ratio"]
        buy_sell_ratio = stats["buy_sell_ratio"]
        sell_buy_ratio = stats["sell_buy_ratio"]

        buy_sell_ratio = stats["buy_sell_ratio"]
        sell_buy_ratio = stats["sell_buy_ratio"]

        # self._log(
        #     f"[MICRO DEBUG] {symbol} buy_flow={buy_flow:.6f} sell_flow={sell_flow:.6f} "
        #     f"total_flow={total_flow:.6f} total_cnt={total_count} "
        #     f"net_flow_ratio={net_flow_ratio:.6f} "
        #     f"buy_sell_ratio={buy_sell_ratio:.6f} sell_buy_ratio={sell_buy_ratio:.6f}"
        # )

       
        # 1) 총 거래량 부족 -> 노이즈 차단
        if total_flow < self.min_total_flow:
            self._log(
                f"[MICRO SKIP] {symbol} low_total_flow total_flow={total_flow:.6f} min_total_flow={self.min_total_flow:.6f}"
            )
            return None

        # 2) 체결 수 부족 -> 한두 건 큰 체결 왜곡 차단
        if total_count < self.min_trade_count:
            self._log(
                f"[MICRO SKIP] {symbol} low_trade_count total_count={total_count} min_trade_count={self.min_trade_count}"
            )
            return None

        thresholds = self._dynamic_thresholds(stats)
        imbalance_threshold = thresholds["imbalance_threshold"]
        min_net_flow_ratio = thresholds["min_net_flow_ratio"]

        self._log(
            f"[MICRO DEBUG] {symbol} dyn_thresholds imbalance_threshold={imbalance_threshold:.6f} "
            f"min_net_flow_ratio={min_net_flow_ratio:.6f}"
        )

        # 3) 순우위 약하면 차단
        if abs(net_flow_ratio) < min_net_flow_ratio:
            self._log(
                f"[MICRO SKIP] {symbol} weak_net_flow_ratio abs_net_flow_ratio={abs(net_flow_ratio):.6f} "
                f"required={min_net_flow_ratio:.6f}"
            )
            return None

        side = None
        raw_strength = 0.0

        # 4) 비율 + 순우위 동시 만족
        if buy_sell_ratio >= imbalance_threshold and net_flow_ratio > 0:
            side = "BUY"
            raw_strength = buy_sell_ratio
        elif sell_buy_ratio >= imbalance_threshold and net_flow_ratio < 0:
            side = "SELL"
            raw_strength = sell_buy_ratio
        else:
            self._log(
                f"[MICRO SKIP] {symbol} ratio_not_met "
                f"buy_sell_ratio={buy_sell_ratio:.6f} sell_buy_ratio={sell_buy_ratio:.6f} "
                f"imbalance_threshold={imbalance_threshold:.6f} net_flow_ratio={net_flow_ratio:.6f}"
            )
            return None

        quality = self._flow_quality_score(stats, side)

        # 5) 약신호 차단, 강신호는 우선 통과
        if quality < 0.40:
            self._log(
                f"[MICRO SKIP] {symbol} low_quality quality={quality:.6f} min_required=0.440000"
            )
            return None

        # 애매한 신호는 추가 검증
        if quality < 0.54:
            if abs(net_flow_ratio) < (min_net_flow_ratio * 1.04):
                self._log(
                    f"[MICRO SKIP] {symbol} weak_quality_net_ratio quality={quality:.6f} "
                    f"abs_net_flow_ratio={abs(net_flow_ratio):.6f} required={(min_net_flow_ratio * 1.08):.6f}"
                )
                return None
            if raw_strength < (imbalance_threshold * 1.01):
                self._log(
                    f"[MICRO SKIP] {symbol} weak_quality_strength quality={quality:.6f} "
                    f"raw_strength={raw_strength:.6f} required={(imbalance_threshold * 1.03):.6f}"
                )
                return None

        # 6) 최근 신호 재발사 / 반대방향 즉시 뒤집기 억제
        if self._blocked_by_cooldown(symbol, side, now, stats, quality):
            return None

        self.last_signal_ts[symbol] = now
        self.last_signal_side[symbol] = side

        if self.reset_on_signal:
            self.reset_symbol(symbol)

        # strength는 ratio만이 아니라 quality를 반영해서 정규화
        ratio_part = raw_strength / max(imbalance_threshold, 1e-9)
        normalized_strength = min(1.0, ratio_part * 0.58 + quality * 0.42)

        result = {
            "symbol": symbol,
            "side": side,
            "strength": round(float(normalized_strength), 6),
            "raw_strength": round(float(raw_strength), 6),
            "quality": round(float(quality), 6),
            "buy_flow": round(float(buy_flow), 6),
            "sell_flow": round(float(sell_flow), 6),
            "total_flow": round(float(total_flow), 6),
            "net_flow": round(float(stats["net_flow"]), 6),
            "net_flow_ratio": round(float(net_flow_ratio), 6),
            "buy_count": int(stats["buy_count"]),
            "sell_count": int(stats["sell_count"]),
            "total_count": int(total_count),
            "buy_sell_ratio": round(float(buy_sell_ratio), 6),
            "sell_buy_ratio": round(float(sell_buy_ratio), 6),
            "avg_buy_size": round(float(stats["avg_buy_size"]), 6),
            "avg_sell_size": round(float(stats["avg_sell_size"]), 6),
            "avg_trade_size": round(float(stats["avg_trade_size"]), 6),
            "count_imbalance_ratio": round(float(stats["count_imbalance_ratio"]), 6),
            "imbalance_threshold_used": round(float(imbalance_threshold), 6),
            "min_net_flow_ratio_used": round(float(min_net_flow_ratio), 6),
            "window_seconds": float(self.window_seconds),
            "source": "microstructure_alpha",
            "ts": now,
        }

        self._log(
            f"[MICRO FIRE] {symbol} side={side} strength={normalized_strength:.6f} "
            f"raw_strength={raw_strength:.6f} quality={quality:.6f} "
            f"net_flow_ratio={net_flow_ratio:.6f} total_flow={total_flow:.6f} total_count={total_count}"
        )

        return result

    def snapshot(self, symbol: str) -> Dict[str, Any]:
        now = time.time()
        stats = self._aggregate(symbol, now)

        return {
            "symbol": symbol,
            "buy_flow": round(float(stats["buy_flow"]), 6),
            "sell_flow": round(float(stats["sell_flow"]), 6),
            "total_flow": round(float(stats["total_flow"]), 6),
            "net_flow": round(float(stats["net_flow"]), 6),
            "net_flow_ratio": round(float(stats["net_flow_ratio"]), 6),
            "buy_count": int(stats["buy_count"]),
            "sell_count": int(stats["sell_count"]),
            "total_count": int(stats["total_count"]),
            "buy_sell_ratio": round(float(stats["buy_sell_ratio"]), 6),
            "sell_buy_ratio": round(float(stats["sell_buy_ratio"]), 6),
            "avg_buy_size": round(float(stats["avg_buy_size"]), 6),
            "avg_sell_size": round(float(stats["avg_sell_size"]), 6),
            "avg_trade_size": round(float(stats["avg_trade_size"]), 6),
            "count_imbalance_ratio": round(float(stats["count_imbalance_ratio"]), 6),
            "window_seconds": float(stats["window_seconds"]),
        }

    def reset_symbol(self, symbol: str) -> None:
        self.trades[symbol] = deque()

    def reset_all(self) -> None:
        self.trades.clear()
        self.last_signal_ts.clear()
        self.last_signal_side.clear()
