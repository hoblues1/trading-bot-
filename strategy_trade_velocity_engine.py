import time
from collections import deque
from typing import Dict, Deque, Any, Optional


class TradeVelocityEngine:
    """
    High-end directional trade velocity engine

    목표:
    1) 최근 구간의 체결 속도 급증 감지
    2) 방향성(BUY / SELL) 포함
    3) 단순 체결 수뿐 아니라 거래량 / 순우위 / 초당 체결강도 반영
    4) 같은 방향 재발사 억제
    5) alpha_fusion에서 바로 사용할 strength 제공
    """

    def __init__(
        self,
        window_seconds: float = 3.0,
        velocity_threshold: float = 4.5,          # trades/sec
        volume_velocity_threshold: float = 45.0,  # volume/sec
        min_total_trades: int = 7,
        min_total_volume: float = 120.0,
        min_side_share: float = 0.58,
        min_net_volume_ratio: float = 0.12,
        signal_cooldown_seconds: float = 2.2,
        same_side_rearm_seconds: float = 4.8,
        opposite_side_confirmation_ratio: float = 1.06,
        max_stored_trades_per_symbol: int = 4000,
    ):
        self.window_seconds = float(window_seconds)
        self.velocity_threshold = float(velocity_threshold)
        self.volume_velocity_threshold = float(volume_velocity_threshold)
        self.min_total_trades = int(min_total_trades)
        self.min_total_volume = float(min_total_volume)
        self.min_side_share = float(min_side_share)
        self.min_net_volume_ratio = float(min_net_volume_ratio)
        self.signal_cooldown_seconds = float(signal_cooldown_seconds)
        self.same_side_rearm_seconds = float(same_side_rearm_seconds)
        self.opposite_side_confirmation_ratio = float(opposite_side_confirmation_ratio)
        self.max_stored_trades_per_symbol = int(max_stored_trades_per_symbol)

        # symbol -> deque[(timestamp, side, qty)]
        self.trades: Dict[str, Deque] = {}

        # signal state
        self.last_signal_ts: Dict[str, float] = {}
        self.last_signal_side: Dict[str, str] = {}

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
        symbol = trade.get("symbol")
        side = trade.get("side")
        qty = float(trade.get("qty", 0.0))
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

        buy_count = 0
        sell_count = 0
        buy_volume = 0.0
        sell_volume = 0.0

        for ts, side, qty in dq:
            if side == "BUY":
                buy_count += 1
                buy_volume += qty
            else:
                sell_count += 1
                sell_volume += qty

        total_trades = buy_count + sell_count
        total_volume = buy_volume + sell_volume

        trades_per_sec = total_trades / max(self.window_seconds, 1e-9)
        volume_per_sec = total_volume / max(self.window_seconds, 1e-9)

        buy_trade_share = buy_count / max(total_trades, 1)
        sell_trade_share = sell_count / max(total_trades, 1)

        buy_volume_share = buy_volume / max(total_volume, 1e-9)
        sell_volume_share = sell_volume / max(total_volume, 1e-9)

        net_volume = buy_volume - sell_volume
        net_volume_ratio = net_volume / max(total_volume, 1e-9)

        buy_sell_count_ratio = buy_count / max(sell_count, 1)
        sell_buy_count_ratio = sell_count / max(buy_count, 1)

        buy_sell_volume_ratio = buy_volume / max(sell_volume, 1e-9)
        sell_buy_volume_ratio = sell_volume / max(buy_volume, 1e-9)

        return {
            "symbol": symbol,
            "buy_count": buy_count,
            "sell_count": sell_count,
            "total_trades": total_trades,
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
            "total_volume": total_volume,
            "trades_per_sec": trades_per_sec,
            "volume_per_sec": volume_per_sec,
            "buy_trade_share": buy_trade_share,
            "sell_trade_share": sell_trade_share,
            "buy_volume_share": buy_volume_share,
            "sell_volume_share": sell_volume_share,
            "net_volume": net_volume,
            "net_volume_ratio": net_volume_ratio,
            "buy_sell_count_ratio": buy_sell_count_ratio,
            "sell_buy_count_ratio": sell_buy_count_ratio,
            "buy_sell_volume_ratio": buy_sell_volume_ratio,
            "sell_buy_volume_ratio": sell_buy_volume_ratio,
            "window_seconds": self.window_seconds,
        }

    def _blocked_by_timing(self, symbol: str, side: str, stats: Dict[str, Any]) -> bool:
        last_ts = self.last_signal_ts.get(symbol)
        last_side = self.last_signal_side.get(symbol)

        if last_ts is None or last_side is None:
            return False

        now = time.time()
        elapsed = now - last_ts

        if elapsed < self.signal_cooldown_seconds:
            return True

        if side == last_side and elapsed < self.same_side_rearm_seconds:
            return True

        if side != last_side:
            needed = self.opposite_side_confirmation_ratio

            if side == "BUY":
                if stats["buy_sell_count_ratio"] < needed and stats["buy_sell_volume_ratio"] < needed:
                    return True
            else:
                if stats["sell_buy_count_ratio"] < needed and stats["sell_buy_volume_ratio"] < needed:
                    return True

        return False

    def signal(self, symbol: str, avg_volume=None) -> Optional[Dict[str, Any]]:
        """
        avg_volume 인자는 이전 버전 호환용으로만 남김.
        실제 판단은 최근 윈도우 기반으로 자체 계산.
        """
        now = time.time()
        stats = self._aggregate(symbol, now)

        total_trades = stats["total_trades"]
        total_volume = stats["total_volume"]
        trades_per_sec = stats["trades_per_sec"]
        volume_per_sec = stats["volume_per_sec"]
        net_volume_ratio = stats["net_volume_ratio"]

        # 1) 절대 활동량 부족 차단
        if total_trades < self.min_total_trades:
            return None

        if total_volume < self.min_total_volume:
            return None

        # 2) 진짜 속도 급증이 아니면 차단
        if trades_per_sec < self.velocity_threshold:
            return None

        if volume_per_sec < self.volume_velocity_threshold:
            return None

        # 3) 방향성이 약하면 차단
        if abs(net_volume_ratio) < self.min_net_volume_ratio:
            return None

        side = None

        buy_direction_ok = (
            stats["buy_trade_share"] >= self.min_side_share
            and stats["buy_volume_share"] >= self.min_side_share
            and net_volume_ratio > 0
        )

        sell_direction_ok = (
            stats["sell_trade_share"] >= self.min_side_share
            and stats["sell_volume_share"] >= self.min_side_share
            and net_volume_ratio < 0
        )

        if buy_direction_ok:
            side = "BUY"
        elif sell_direction_ok:
            side = "SELL"
        else:
            return None

        if self._blocked_by_timing(symbol, side, stats):
            return None

        # strength = 체결 속도 + 거래량 속도 + 방향 우위의 혼합 점수
        speed_strength = trades_per_sec / max(self.velocity_threshold, 1e-9)
        volume_strength = volume_per_sec / max(self.volume_velocity_threshold, 1e-9)

        if side == "BUY":
            direction_strength = (
                stats["buy_trade_share"]
                + stats["buy_volume_share"]
                + max(stats["buy_sell_count_ratio"], stats["buy_sell_volume_ratio"])
            ) / 3.0
        else:
            direction_strength = (
                stats["sell_trade_share"]
                + stats["sell_volume_share"]
                + max(stats["sell_buy_count_ratio"], stats["sell_buy_volume_ratio"])
            ) / 3.0

        strength = (speed_strength + volume_strength + direction_strength) / 3.0

        self.last_signal_ts[symbol] = now
        self.last_signal_side[symbol] = side

        return {
            "symbol": symbol,
            "side": side,
            "strength": round(float(strength), 6),
            "velocity": round(float(trades_per_sec), 6),
            "volume_velocity": round(float(volume_per_sec), 6),
            "buy_count": int(stats["buy_count"]),
            "sell_count": int(stats["sell_count"]),
            "total_trades": int(total_trades),
            "buy_volume": round(float(stats["buy_volume"]), 6),
            "sell_volume": round(float(stats["sell_volume"]), 6),
            "total_volume": round(float(total_volume), 6),
            "buy_trade_share": round(float(stats["buy_trade_share"]), 6),
            "sell_trade_share": round(float(stats["sell_trade_share"]), 6),
            "buy_volume_share": round(float(stats["buy_volume_share"]), 6),
            "sell_volume_share": round(float(stats["sell_volume_share"]), 6),
            "net_volume": round(float(stats["net_volume"]), 6),
            "net_volume_ratio": round(float(net_volume_ratio), 6),
            "window_seconds": float(stats["window_seconds"]),
            "source": "trade_velocity",
            "ts": now,
        }

    def snapshot(self, symbol: str) -> Dict[str, Any]:
        now = time.time()
        stats = self._aggregate(symbol, now)

        return {
            "symbol": symbol,
            "buy_count": int(stats["buy_count"]),
            "sell_count": int(stats["sell_count"]),
            "total_trades": int(stats["total_trades"]),
            "buy_volume": round(float(stats["buy_volume"]), 6),
            "sell_volume": round(float(stats["sell_volume"]), 6),
            "total_volume": round(float(stats["total_volume"]), 6),
            "trades_per_sec": round(float(stats["trades_per_sec"]), 6),
            "volume_per_sec": round(float(stats["volume_per_sec"]), 6),
            "buy_trade_share": round(float(stats["buy_trade_share"]), 6),
            "sell_trade_share": round(float(stats["sell_trade_share"]), 6),
            "buy_volume_share": round(float(stats["buy_volume_share"]), 6),
            "sell_volume_share": round(float(stats["sell_volume_share"]), 6),
            "net_volume": round(float(stats["net_volume"]), 6),
            "net_volume_ratio": round(float(stats["net_volume_ratio"]), 6),
            "buy_sell_count_ratio": round(float(stats["buy_sell_count_ratio"]), 6),
            "sell_buy_count_ratio": round(float(stats["sell_buy_count_ratio"]), 6),
            "buy_sell_volume_ratio": round(float(stats["buy_sell_volume_ratio"]), 6),
            "sell_buy_volume_ratio": round(float(stats["sell_buy_volume_ratio"]), 6),
            "window_seconds": float(stats["window_seconds"]),
        }

    def reset_symbol(self, symbol: str) -> None:
        self.trades[symbol] = deque()

    def reset_all(self) -> None:
        self.trades.clear()
        self.last_signal_ts.clear()
        self.last_signal_side.clear()
