import logging
import time
from collections import deque
from typing import Dict, Deque, Any, Optional


class OrderbookImbalanceStrategy:
    """
    High-end orderbook imbalance strategy

    목표:
    1) 단일 스냅샷이 아니라 최근 imbalance 지속성 확인
    2) 너무 오래된 orderbook 데이터 차단
    3) 같은 방향 연속 발사 억제
    4) 반대 방향 즉시 뒤집기 억제
    5) 극단 스푸핑 / 순간 왜곡 필터 강화
    6) alpha / main loop 에서 바로 쓸 수 있는 score, strength 제공
    """

    def __init__(
        self,
        threshold: float = 1.8,
        min_ratio: float = 0.15,
        max_ratio: float = 8.0,
        min_liquidity: float = 120.0,
        top_levels: int = 10,
        history_seconds: float = 4.0,
        min_history_points: int = 3,
        min_persistence_ratio: float = 0.67,
        cooldown_seconds: float = 8.0,
        same_side_rearm_seconds: float = 14.0,
        flip_block_seconds: float = 16.0,
        strong_flip_multiplier: float = 1.18,
        stale_book_seconds: float = 1.5,
        max_history_points_per_symbol: int = 200,
    ):
        self.threshold = float(threshold)

        # 노이즈 / 왜곡 필터
        self.max_ratio = float(max_ratio)
        self.min_ratio = float(min_ratio)

        # 최소 유동성
        self.min_liquidity = float(min_liquidity)
        self.top_levels = int(top_levels)

        # 지속성 확인
        self.history_seconds = float(history_seconds)
        self.min_history_points = int(min_history_points)
        self.min_persistence_ratio = float(min_persistence_ratio)
        self.stale_book_seconds = float(stale_book_seconds)
        self.max_history_points_per_symbol = int(max_history_points_per_symbol)

        # timing guards
        self.cooldown_seconds = float(cooldown_seconds)
        self.same_side_rearm_seconds = float(same_side_rearm_seconds)
        self.flip_block_seconds = float(flip_block_seconds)
        self.strong_flip_multiplier = float(strong_flip_multiplier)

        self.last_signal_time: Dict[str, float] = {}
        self.last_signal_side: Dict[str, str] = {}

        # 최신 스냅샷
        self.orderbooks: Dict[str, Dict[str, Any]] = {}

        # symbol -> deque[(timestamp, bid_volume, ask_volume, imbalance)]
        self.history: Dict[str, Deque] = {}

    # ================= INTERNAL =================
    def _get_history(self, symbol: str) -> Deque:
        if symbol not in self.history:
            self.history[symbol] = deque()
        return self.history[symbol]

    def _prune_history(self, symbol: str, now: float):
        dq = self._get_history(symbol)
        cutoff = now - self.history_seconds

        while dq and dq[0][0] < cutoff:
            dq.popleft()

        while len(dq) > self.max_history_points_per_symbol:
            dq.popleft()

    def _normalize_strength(self, value: float, low: float = 0.5, high: float = 3.0) -> float:
        return max(low, min(float(value), high))

    def _blocked_by_timing(self, symbol: str, side: str, strength: float) -> bool:
        last_ts = self.last_signal_time.get(symbol)
        last_side = self.last_signal_side.get(symbol)

        if last_ts is None or last_side is None:
            return False

        now = time.time()
        elapsed = now - last_ts

        # 전체 쿨다운
        if elapsed < self.cooldown_seconds:
            return True

        # 같은 방향 재발사 억제
        if side == last_side and elapsed < self.same_side_rearm_seconds:
            return True

        # 반대 방향 즉시 뒤집기 억제
        if side != last_side and elapsed < self.flip_block_seconds:
            needed = self.threshold * self.strong_flip_multiplier
            if strength < needed:
                return True

        return False

    # ================= UPDATE ORDERBOOK =================
    def update_orderbook(self, symbol, bids, asks):
        try:
            bid_volume = 0.0
            ask_volume = 0.0

            # 상위 N호가 사용
            for price, qty in bids[: self.top_levels]:
                bid_volume += float(qty)

            for price, qty in asks[: self.top_levels]:
                ask_volume += float(qty)

            now = time.time()
            total = bid_volume + ask_volume
            imbalance = bid_volume / max(ask_volume, 1e-9)

            self.orderbooks[symbol] = {
                "bid_volume": bid_volume,
                "ask_volume": ask_volume,
                "total_volume": total,
                "imbalance": imbalance,
                "time": now,
            }

            dq = self._get_history(symbol)
            dq.append((now, bid_volume, ask_volume, imbalance))
            self._prune_history(symbol, now)

        except Exception as e:
            logging.error(f"Orderbook update error: {e}")

    # ================= SNAPSHOT / AGG =================
    def snapshot(self, symbol: str) -> Optional[Dict[str, Any]]:
        try:
            if symbol not in self.orderbooks:
                return None

            book = self.orderbooks[symbol]
            now = time.time()

            return {
                "symbol": symbol,
                "bid_volume": round(float(book["bid_volume"]), 6),
                "ask_volume": round(float(book["ask_volume"]), 6),
                "total_volume": round(float(book["total_volume"]), 6),
                "imbalance": round(float(book["imbalance"]), 6),
                "age_seconds": round(now - float(book["time"]), 6),
                "source": "orderbook_imbalance",
            }

        except Exception as e:
            logging.error(f"Orderbook snapshot error: {e}")
            return None

    def _aggregate_history(self, symbol: str) -> Optional[Dict[str, Any]]:
        if symbol not in self.orderbooks:
            return None

        now = time.time()
        self._prune_history(symbol, now)

        book = self.orderbooks[symbol]
        age = now - float(book["time"])

        # 너무 오래된 orderbook 데이터는 무효
        if age > self.stale_book_seconds:
            return None

        dq = self._get_history(symbol)

        if len(dq) < self.min_history_points:
            return None

        buy_points = 0
        sell_points = 0
        valid_points = 0

        imbalance_values = []
        bid_sum = 0.0
        ask_sum = 0.0
        total_sum = 0.0

        for ts, bid_volume, ask_volume, imbalance in dq:
            total = bid_volume + ask_volume

            # 유동성 부족 구간은 persistence 계산에서 제외
            if total < self.min_liquidity:
                continue

            # 극단치 필터
            if imbalance > self.max_ratio or imbalance < self.min_ratio:
                continue

            valid_points += 1
            imbalance_values.append(imbalance)
            bid_sum += bid_volume
            ask_sum += ask_volume
            total_sum += total

            if imbalance >= self.threshold:
                buy_points += 1
            elif imbalance <= (1.0 / self.threshold):
                sell_points += 1

        if valid_points < self.min_history_points:
            return None

        avg_bid = bid_sum / valid_points
        avg_ask = ask_sum / valid_points
        avg_total = total_sum / valid_points
        avg_imbalance = sum(imbalance_values) / max(len(imbalance_values), 1)

        buy_persistence = buy_points / valid_points
        sell_persistence = sell_points / valid_points

        return {
            "symbol": symbol,
            "valid_points": valid_points,
            "avg_bid_volume": avg_bid,
            "avg_ask_volume": avg_ask,
            "avg_total_volume": avg_total,
            "avg_imbalance": avg_imbalance,
            "buy_persistence": buy_persistence,
            "sell_persistence": sell_persistence,
            "book_age": age,
            "window_seconds": self.history_seconds,
        }

    # ================= SIGNAL =================
    def signal(self, symbol):
        try:
            stats = self._aggregate_history(symbol)
            if not stats:
                return None

            avg_total = stats["avg_total_volume"]
            avg_imbalance = stats["avg_imbalance"]
            buy_persistence = stats["buy_persistence"]
            sell_persistence = stats["sell_persistence"]

            # 평균 유동성 부족 차단
            if avg_total < self.min_liquidity:
                return None

            side = None
            strength = 0.0
            confidence = 0.0

            # ================= BUY PRESSURE =================
            if (
                avg_imbalance >= self.threshold
                and buy_persistence >= self.min_persistence_ratio
            ):
                side = "BUY"
                strength = self._normalize_strength(avg_imbalance)
                confidence = min(1.0, buy_persistence)

            # ================= SELL PRESSURE =================
            elif (
                avg_imbalance <= (1.0 / self.threshold)
                and sell_persistence >= self.min_persistence_ratio
            ):
                side = "SELL"
                strength = self._normalize_strength(1.0 / max(avg_imbalance, 1e-9))
                confidence = min(1.0, sell_persistence)

            else:
                return None

            if self._blocked_by_timing(symbol, side, strength):
                return None

            now = time.time()
            self.last_signal_time[symbol] = now
            self.last_signal_side[symbol] = side

            score = min((strength / self.threshold), 3.0)

            logging.info(
                f"IMBALANCE {side} | {symbol} | strength={strength:.2f} | conf={confidence:.2f}"
            )

            return {
                "symbol": symbol,
                "side": side,
                "score": round(float(score), 6),
                "strength": round(float(strength), 6),
                "confidence": round(float(confidence), 6),
                "avg_imbalance": round(float(avg_imbalance), 6),
                "avg_bid_volume": round(float(stats["avg_bid_volume"]), 6),
                "avg_ask_volume": round(float(stats["avg_ask_volume"]), 6),
                "avg_total_volume": round(float(avg_total), 6),
                "buy_persistence": round(float(buy_persistence), 6),
                "sell_persistence": round(float(sell_persistence), 6),
                "valid_points": int(stats["valid_points"]),
                "window_seconds": float(stats["window_seconds"]),
                "source": "orderbook_imbalance",
                "ts": now,
            }

        except Exception as e:
            logging.error(f"Imbalance signal error: {e}")
            return None
