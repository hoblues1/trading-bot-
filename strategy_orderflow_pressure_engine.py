import time
from collections import deque
from typing import Dict, Deque, Any, Optional


class OrderflowPressureEngine:
    """
    High-end orderflow pressure engine

    실전 수정 포인트:
    - 과도한 total volume / ratio 기준 소폭 완화
    - 왜 신호가 안 나오는지 DEBUG / SKIP / BLOCK / FIRE 로그 제공
    - 수수료 녹는 수준의 무지성 완화는 하지 않음
    - 반대 방향 플립은 여전히 보수적으로 유지
    """

    def __init__(
        self,
        window_seconds: float = 4.0,
        pressure_ratio: float = 1.10,
        min_total_volume: float = 0.006,
        min_trade_count: int = 3,
        min_net_volume_ratio: float = 0.060,
        min_avg_trade_size_ratio: float = 0.84,
        signal_cooldown_seconds: float = 1.0,
        same_side_rearm_seconds: float = 1.9,
        opposite_side_confirmation_ratio: float = 1.05,
        max_stored_trades_per_symbol: int = 4000,
        debug: bool = False,
    ):
        self.window_seconds = float(window_seconds)
        self.pressure_ratio = float(pressure_ratio)
        self.min_total_volume = float(min_total_volume)
        self.min_trade_count = int(min_trade_count)
        self.min_net_volume_ratio = float(min_net_volume_ratio)
        self.min_avg_trade_size_ratio = float(min_avg_trade_size_ratio)
        self.signal_cooldown_seconds = float(signal_cooldown_seconds)
        self.same_side_rearm_seconds = float(same_side_rearm_seconds)
        self.opposite_side_confirmation_ratio = float(opposite_side_confirmation_ratio)
        self.max_stored_trades_per_symbol = int(max_stored_trades_per_symbol)
        self.debug = bool(debug)

        self.trades: Dict[str, Deque] = {}
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

    def _normalize_side(self, side: Any) -> Optional[str]:
        if side is None:
            return None

        if isinstance(side, bool):
            return "SELL" if side else "BUY"

        if isinstance(side, (int, float)):
            if float(side) > 0:
                return "BUY"
            if float(side) < 0:
                return "SELL"
            return None

        s = str(side).upper().strip()

        if s in ("BUY", "B", "LONG", "BID", "TAKER_BUY"):
            return "BUY"

        if s in ("SELL", "S", "SHORT", "ASK", "TAKER_SELL"):
            return "SELL"

        return None

    def _extract_trade_from_input(
        self,
        symbol: Optional[str] = None,
        trade: Optional[Dict[str, Any]] = None,
        market: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        snapshot: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Optional[Dict[str, Any]]:
        candidate = trade or market or data or snapshot or kwargs

        if not isinstance(candidate, dict):
            return None

        s = candidate.get("symbol", symbol)

        qty = (
            candidate.get("qty")
            or candidate.get("quantity")
            or candidate.get("size")
            or candidate.get("volume")
            or candidate.get("last_qty")
            or candidate.get("last_quantity")
        )

        ts = (
            candidate.get("timestamp")
            or candidate.get("ts")
            or candidate.get("time")
            or time.time()
        )

        side = (
            candidate.get("side")
            or candidate.get("taker_side")
            or candidate.get("aggressor_side")
            or candidate.get("direction")
        )

        # Binance aggTrade 스타일 대응
        if side is None and "m" in candidate:
            side = "SELL" if bool(candidate.get("m")) else "BUY"

        # bid/ask volume fallback
        if side is None:
            bid_vol = candidate.get("bid_volume") or candidate.get("bids_volume")
            ask_vol = candidate.get("ask_volume") or candidate.get("asks_volume")
            try:
                bid_vol = float(bid_vol) if bid_vol is not None else None
                ask_vol = float(ask_vol) if ask_vol is not None else None
                if bid_vol is not None and ask_vol is not None:
                    if bid_vol > ask_vol:
                        side = "BUY"
                    elif ask_vol > bid_vol:
                        side = "SELL"
            except Exception:
                pass

        side = self._normalize_side(side)

        try:
            qty = float(qty) if qty is not None else 0.0
        except Exception:
            qty = 0.0

        try:
            ts = float(ts)
        except Exception:
            ts = time.time()

        if not s or side not in ("BUY", "SELL") or qty <= 0:
            return None

        return {
            "symbol": s,
            "side": side,
            "qty": qty,
            "timestamp": ts,
        }

    def update(self, trade: Dict[str, Any]) -> None:
        if not isinstance(trade, dict):
            return

        symbol = trade.get("symbol")
        side = trade.get("side")

        try:
            qty = float(trade.get("qty", 0.0))
        except Exception:
            qty = 0.0

        try:
            now = float(trade.get("timestamp", time.time()))
        except Exception:
            now = time.time()

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

        buy_vol = 0.0
        sell_vol = 0.0
        buy_cnt = 0
        sell_cnt = 0

        for ts, side, qty in dq:
            if side == "BUY":
                buy_vol += qty
                buy_cnt += 1
            else:
                sell_vol += qty
                sell_cnt += 1

        total_vol = buy_vol + sell_vol
        total_cnt = buy_cnt + sell_cnt
        net_vol = buy_vol - sell_vol
        net_vol_ratio = (net_vol / total_vol) if total_vol > 0 else 0.0

        buy_avg = buy_vol / buy_cnt if buy_cnt > 0 else 0.0
        sell_avg = sell_vol / sell_cnt if sell_cnt > 0 else 0.0

        buy_sell_ratio = buy_vol / max(sell_vol, 1e-9)
        sell_buy_ratio = sell_vol / max(buy_vol, 1e-9)

        buy_avg_ratio = buy_avg / max(sell_avg, 1e-9)
        sell_avg_ratio = sell_avg / max(buy_avg, 1e-9)

        return {
            "symbol": symbol,
            "buy_vol": buy_vol,
            "sell_vol": sell_vol,
            "total_vol": total_vol,
            "net_vol": net_vol,
            "net_vol_ratio": net_vol_ratio,
            "buy_cnt": buy_cnt,
            "sell_cnt": sell_cnt,
            "total_cnt": total_cnt,
            "buy_avg": buy_avg,
            "sell_avg": sell_avg,
            "buy_sell_ratio": buy_sell_ratio,
            "sell_buy_ratio": sell_buy_ratio,
            "buy_avg_ratio": buy_avg_ratio,
            "sell_avg_ratio": sell_avg_ratio,
            "window_seconds": self.window_seconds,
        }

    def _dynamic_thresholds(self, stats: Dict[str, Any]) -> Dict[str, float]:
        pressure_ratio = self.pressure_ratio
        min_net_volume_ratio = self.min_net_volume_ratio
        min_avg_trade_size_ratio = self.min_avg_trade_size_ratio

        total_vol = stats["total_vol"]
        total_cnt = stats["total_cnt"]
        abs_net = abs(stats["net_vol_ratio"])

        # 흐름이 충분하면 소폭 완화
        if total_vol >= self.min_total_volume * 1.8 and total_cnt >= self.min_trade_count * 1.5:
            pressure_ratio -= 0.08
            min_net_volume_ratio -= 0.015

        # 순우위가 강하면 평균 체결 크기 조건 약간 완화
        if abs_net >= self.min_net_volume_ratio * 1.8:
            min_avg_trade_size_ratio -= 0.015

        return {
            "pressure_ratio": max(1.01, float(pressure_ratio)),
            "min_net_volume_ratio": max(0.035, float(min_net_volume_ratio)),
            "min_avg_trade_size_ratio": max(0.82, float(min_avg_trade_size_ratio)),
        }

    def _quality_score(self, stats: Dict[str, Any], side: str, dyn: Dict[str, float]) -> float:
        total_vol = stats["total_vol"]
        total_cnt = stats["total_cnt"]
        abs_net = abs(stats["net_vol_ratio"])

        if side == "BUY":
            dominance_ratio = stats["buy_sell_ratio"]
            avg_ratio = stats["buy_avg_ratio"]
        else:
            dominance_ratio = stats["sell_buy_ratio"]
            avg_ratio = stats["sell_avg_ratio"]

        vol_score = min(1.0, total_vol / max(self.min_total_volume * 2.0, 1e-9))
        count_score = min(1.0, total_cnt / max(self.min_trade_count * 1.8, 1e-9))
        net_score = min(1.0, abs_net / max(dyn["min_net_volume_ratio"] * 2.0, 1e-9))
        dominance_score = min(1.0, dominance_ratio / max(dyn["pressure_ratio"] * 1.5, 1e-9))
        avg_score = min(1.0, avg_ratio / max(dyn["min_avg_trade_size_ratio"] * 1.15, 1e-9))

        quality = (
            dominance_score * 0.32 +
            net_score * 0.26 +
            vol_score * 0.18 +
            count_score * 0.14 +
            avg_score * 0.10
        )
        return float(max(0.0, min(quality, 1.0)))

    def _blocked_by_timing(self, symbol: str, side: str, strength: float, quality: float, stats: Dict[str, Any]) -> bool:
        last_ts = self.last_signal_ts.get(symbol)
        last_side = self.last_signal_side.get(symbol)

        if last_ts is None or last_side is None:
            return False

        now = time.time()
        elapsed = now - last_ts

        if elapsed < self.signal_cooldown_seconds:
            self._log(
                f"[PRESSURE BLOCK] {symbol} cooldown elapsed={elapsed:.3f} < {self.signal_cooldown_seconds:.3f}"
            )
            return True

        same_side_rearm = self.same_side_rearm_seconds
        if quality >= 0.82:
            same_side_rearm *= 0.62
        elif quality >= 0.72:
            same_side_rearm *= 0.76

        if side == last_side and elapsed < same_side_rearm:
            self._log(
                f"[PRESSURE BLOCK] {symbol} same_side_rearm elapsed={elapsed:.3f} < {same_side_rearm:.3f}"
            )
            return True

        if side != last_side:
            needed = self.pressure_ratio * self.opposite_side_confirmation_ratio
            if quality >= 0.85:
                needed *= 0.97

            if side == "BUY":
                if stats["buy_sell_ratio"] < needed:
                    self._log(
                        f"[PRESSURE BLOCK] {symbol} reverse_buy_block ratio={stats['buy_sell_ratio']:.6f} required={needed:.6f}"
                    )
                    return True
            else:
                if stats["sell_buy_ratio"] < needed:
                    self._log(
                        f"[PRESSURE BLOCK] {symbol} reverse_sell_block ratio={stats['sell_buy_ratio']:.6f} required={needed:.6f}"
                    )
                    return True

        return False

    def signal(
        self,
        symbol: str = None,
        trade: Optional[Dict[str, Any]] = None,
        market: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        snapshot: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Optional[Dict[str, Any]]:
        incoming = self._extract_trade_from_input(
            symbol=symbol,
            trade=trade,
            market=market,
            data=data,
            snapshot=snapshot,
            **kwargs
        )

        if incoming is not None:
            self.update(incoming)
            symbol = incoming["symbol"]

        if not symbol:
            return None

        now = time.time()
        stats = self._aggregate(symbol, now)

        buy_vol = stats["buy_vol"]
        sell_vol = stats["sell_vol"]
        total_vol = stats["total_vol"]
        total_cnt = stats["total_cnt"]
        net_vol_ratio = stats["net_vol_ratio"]

          # self._log(
          #     f"[PRESSURE DEBUG] {symbol} buy_vol={buy_vol:.6f} sell_vol={sell_vol:.6f} "
          #     f"total_vol={total_vol:.6f} total_cnt={total_cnt} "
          #     f"net_vol_ratio={net_vol_ratio:.6f} "
          #     f"buy_sell_ratio={stats['buy_sell_ratio']:.6f} sell_buy_ratio={stats['sell_buy_ratio']:.6f} "
          #     f"buy_avg_ratio={stats['buy_avg_ratio']:.6f} sell_avg_ratio={stats['sell_avg_ratio']:.6f}"
          # )

        if total_vol < self.min_total_volume:
          # self._log(
          #     f"[PRESSURE SKIP] {symbol} low_total_volume total_vol={total_vol:.6f} min_total_volume={self.min_total_volume:.6f}"
          # )
            return None

        if total_cnt < self.min_trade_count:
            self._log(
                f"[PRESSURE SKIP] {symbol} low_trade_count total_cnt={total_cnt} min_trade_count={self.min_trade_count}"
            )
            return None

        dyn = self._dynamic_thresholds(stats)

        self._log(
            f"[PRESSURE DEBUG] {symbol} dyn_thresholds pressure_ratio={dyn['pressure_ratio']:.6f} "
            f"min_net_volume_ratio={dyn['min_net_volume_ratio']:.6f} "
            f"min_avg_trade_size_ratio={dyn['min_avg_trade_size_ratio']:.6f}"
        )

        if abs(net_vol_ratio) < dyn["min_net_volume_ratio"]:
            self._log(
                f"[PRESSURE SKIP] {symbol} weak_net_volume_ratio abs_net_vol_ratio={abs(net_vol_ratio):.6f} "
                f"required={dyn['min_net_volume_ratio']:.6f}"
            )
            return None

        side = None
        raw_strength = 0.0

        if (
            stats["buy_sell_ratio"] >= dyn["pressure_ratio"]
            and stats["buy_avg_ratio"] >= dyn["min_avg_trade_size_ratio"]
            and net_vol_ratio > 0
        ):
            side = "BUY"
            raw_strength = (stats["buy_sell_ratio"] * 0.78) + (stats["buy_avg_ratio"] * 0.3)

        elif (
            stats["sell_buy_ratio"] >= dyn["pressure_ratio"]
            and stats["sell_avg_ratio"] >= dyn["min_avg_trade_size_ratio"]
            and net_vol_ratio < 0
        ):
            side = "SELL"
            raw_strength = (stats["sell_buy_ratio"] * 0.78) + (stats["sell_avg_ratio"] * 0.3)

        else:
            self._log(
                f"[PRESSURE SKIP] {symbol} ratio_not_met "
                f"buy_sell_ratio={stats['buy_sell_ratio']:.6f} sell_buy_ratio={stats['sell_buy_ratio']:.6f} "
                f"buy_avg_ratio={stats['buy_avg_ratio']:.6f} sell_avg_ratio={stats['sell_avg_ratio']:.6f} "
                f"pressure_ratio={dyn['pressure_ratio']:.6f} "
                f"min_avg_trade_size_ratio={dyn['min_avg_trade_size_ratio']:.6f}"
            )
            return None

        quality = self._quality_score(stats, side, dyn)

        if quality < 0.42:
            self._log(
                f"[PRESSURE SKIP] {symbol} low_quality quality={quality:.6f}"
            )
            return None

        if quality < 0.54:
            if abs(net_vol_ratio) < (dyn["min_net_volume_ratio"] * 1.04):
                self._log(
                    f"[PRESSURE SKIP] {symbol} weak_quality_net_ratio quality={quality:.6f} "
                    f"abs_net_vol_ratio={abs(net_vol_ratio):.6f} "
                    f"required={(dyn['min_net_volume_ratio'] * 1.08):.6f}"
                )
                return None
            if side == "BUY" and stats["buy_sell_ratio"] < (dyn["pressure_ratio"] * 1.01):
                self._log(
                    f"[PRESSURE SKIP] {symbol} weak_quality_buy_strength ratio={stats['buy_sell_ratio']:.6f} "
                    f"required={(dyn['pressure_ratio'] * 1.03):.6f}"
                )
                return None
            if side == "SELL" and stats["sell_buy_ratio"] < (dyn["pressure_ratio"] * 1.01):
                self._log(
                    f"[PRESSURE SKIP] {symbol} weak_quality_sell_strength ratio={stats['sell_buy_ratio']:.6f} "
                    f"required={(dyn['pressure_ratio'] * 1.03):.6f}"
                )
                return None

        if self._blocked_by_timing(symbol, side, raw_strength, quality, stats):
            return None

        self.last_signal_ts[symbol] = now
        self.last_signal_side[symbol] = side

        normalized_strength = min(
            1.0,
            (raw_strength / max(dyn["pressure_ratio"], 1e-9)) * 0.60 + quality * 0.40
        )

        result = {
            "symbol": symbol,
            "side": side,
            "strength": round(float(normalized_strength), 6),
            "raw_strength": round(float(raw_strength), 6),
            "quality": round(float(quality), 6),
            "pressure": round(max(buy_vol, sell_vol), 6),
            "buy_volume": round(float(buy_vol), 6),
            "sell_volume": round(float(sell_vol), 6),
            "total_volume": round(float(total_vol), 6),
            "net_volume": round(float(stats["net_vol"]), 6),
            "net_volume_ratio": round(float(net_vol_ratio), 6),
            "buy_count": int(stats["buy_cnt"]),
            "sell_count": int(stats["sell_cnt"]),
            "total_count": int(total_cnt),
            "buy_avg_trade_size": round(float(stats["buy_avg"]), 6),
            "sell_avg_trade_size": round(float(stats["sell_avg"]), 6),
            "buy_sell_ratio": round(float(stats["buy_sell_ratio"]), 6),
            "sell_buy_ratio": round(float(stats["sell_buy_ratio"]), 6),
            "buy_avg_ratio": round(float(stats["buy_avg_ratio"]), 6),
            "sell_avg_ratio": round(float(stats["sell_avg_ratio"]), 6),
            "pressure_ratio_used": round(float(dyn["pressure_ratio"]), 6),
            "min_net_volume_ratio_used": round(float(dyn["min_net_volume_ratio"]), 6),
            "min_avg_trade_size_ratio_used": round(float(dyn["min_avg_trade_size_ratio"]), 6),
            "window_seconds": float(stats["window_seconds"]),
            "source": "orderflow_pressure",
            "ts": now,
        }

        self._log(
            f"[PRESSURE FIRE] {symbol} side={side} strength={normalized_strength:.6f} "
            f"raw_strength={raw_strength:.6f} quality={quality:.6f} "
            f"net_vol_ratio={net_vol_ratio:.6f} total_vol={total_vol:.6f} total_cnt={total_cnt}"
        )

        return result

    def generate_signal(self, symbol: str = None, market: Optional[Dict[str, Any]] = None, **kwargs) -> Optional[Dict[str, Any]]:
        return self.signal(symbol=symbol, market=market, **kwargs)

    def compute(self, symbol: str = None, market: Optional[Dict[str, Any]] = None, **kwargs) -> Optional[Dict[str, Any]]:
        return self.signal(symbol=symbol, market=market, **kwargs)

    def analyze(self, symbol: str = None, market: Optional[Dict[str, Any]] = None, **kwargs) -> Optional[Dict[str, Any]]:
        return self.signal(symbol=symbol, market=market, **kwargs)

    def snapshot(self, symbol: str) -> Dict[str, Any]:
        now = time.time()
        stats = self._aggregate(symbol, now)

        return {
            "symbol": symbol,
            "buy_volume": round(float(stats["buy_vol"]), 6),
            "sell_volume": round(float(stats["sell_vol"]), 6),
            "total_volume": round(float(stats["total_vol"]), 6),
            "net_volume": round(float(stats["net_vol"]), 6),
            "net_volume_ratio": round(float(stats["net_vol_ratio"]), 6),
            "buy_count": int(stats["buy_cnt"]),
            "sell_count": int(stats["sell_cnt"]),
            "total_count": int(stats["total_cnt"]),
            "buy_avg_trade_size": round(float(stats["buy_avg"]), 6),
            "sell_avg_trade_size": round(float(stats["sell_avg"]), 6),
            "buy_sell_ratio": round(float(stats["buy_sell_ratio"]), 6),
            "sell_buy_ratio": round(float(stats["sell_buy_ratio"]), 6),
            "buy_avg_ratio": round(float(stats["buy_avg_ratio"]), 6),
            "sell_avg_ratio": round(float(stats["sell_avg_ratio"]), 6),
            "window_seconds": float(stats["window_seconds"]),
        }

    def reset_symbol(self, symbol: str) -> None:
        self.trades[symbol] = deque()

    def reset_all(self) -> None:
        self.trades.clear()
        self.last_signal_ts.clear()
        self.last_signal_side.clear()
