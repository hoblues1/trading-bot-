import logging
import time
from typing import Dict, Any, Optional


class SpreadFilter:
    """
    High-end spread filter

    목표:
    1) symbol별 best bid/ask 저장
    2) 메인 루프의 allow(symbol=..., price=...) 호출과 호환
    3) 기존 allow_trade(bids, asks) 방식도 호환
    4) stale orderbook 차단
    5) spread 값 snapshot 제공
    """

    def __init__(
        self,
        max_spread: float = 0.0005,     # 0.05%
        stale_book_seconds: float = 1.5
    ):
        self.max_spread = float(max_spread)
        self.stale_book_seconds = float(stale_book_seconds)

        # symbol -> {"best_bid": float, "best_ask": float, "mid": float, "spread": float, "ts": float}
        self.books: Dict[str, Dict[str, Any]] = {}

    # ================= INTERNAL =================
    def _safe_float(self, value, default=0.0):
        try:
            return float(value)
        except Exception:
            return default

    def _now(self) -> float:
        return time.time()

    def _calc_from_bid_ask(self, best_bid: float, best_ask: float) -> Optional[Dict[str, float]]:
        if best_bid <= 0 or best_ask <= 0:
            return None

        if best_ask < best_bid:
            return None

        mid = (best_bid + best_ask) / 2.0
        if mid <= 0:
            return None

        spread = (best_ask - best_bid) / mid

        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid": mid,
            "spread": spread,
        }

    # ================= UPDATE =================
    def update(self, symbol, bids, asks):
        """
        symbol별 오더북 상태 저장
        """
        try:
            if not symbol or not bids or not asks:
                return False

            best_bid = self._safe_float(bids[0][0], 0.0)
            best_ask = self._safe_float(asks[0][0], 0.0)

            data = self._calc_from_bid_ask(best_bid, best_ask)
            if not data:
                return False

            self.books[str(symbol).upper()] = {
                **data,
                "ts": self._now(),
            }
            return True

        except Exception as e:
            logging.error(f"Spread filter update error: {e}")
            return False

    # ================= LEGACY =================
    def allow_trade(self, bids, asks):
        """
        기존 방식 호환
        """
        try:
            if not bids or not asks:
                return False

            best_bid = self._safe_float(bids[0][0], 0.0)
            best_ask = self._safe_float(asks[0][0], 0.0)

            data = self._calc_from_bid_ask(best_bid, best_ask)
            if not data:
                return False

            spread = data["spread"]

            if spread > self.max_spread:
                logging.info(f"SPREAD BLOCKED | spread={spread:.6f}")
                return False

            return True

        except Exception as e:
            logging.error(f"Spread filter error: {e}")
            return False

    # ================= MAIN LOOP FRIENDLY =================
    def allow(self, symbol=None, price=None, **kwargs):
        """
        메인 루프 호환:
        allow(symbol=symbol, price=price)
        """
        try:
            if not symbol:
                return True

            symbol = str(symbol).upper()
            book = self.books.get(symbol)

            # 오더북 정보 없으면 너무 공격적으로 막지 않고 통과
            # 단, 원하면 False로 바꿀 수 있음
            if not book:
                return True

            age = self._now() - float(book["ts"])
            if age > self.stale_book_seconds:
                logging.info(f"SPREAD BLOCKED | symbol={symbol} | reason=stale_book | age={age:.3f}")
                return False

            spread = float(book["spread"])

            if spread > self.max_spread:
                logging.info(
                    f"SPREAD BLOCKED | symbol={symbol} | spread={spread:.6f} | max={self.max_spread:.6f}"
                )
                return False

            return True

        except Exception as e:
            logging.error(f"Spread allow error: {e}")
            return False

    def check(self, symbol=None, price=None, **kwargs):
        """
        allow와 동일 동작. 메인 호환용.
        """
        return self.allow(symbol=symbol, price=price, **kwargs)

    # ================= SNAPSHOT =================
    def snapshot(self, symbol: str) -> Optional[Dict[str, Any]]:
        symbol = str(symbol).upper()
        book = self.books.get(symbol)
        if not book:
            return None

        age = self._now() - float(book["ts"])

        return {
            "symbol": symbol,
            "best_bid": float(book["best_bid"]),
            "best_ask": float(book["best_ask"]),
            "mid": float(book["mid"]),
            "spread": float(book["spread"]),
            "age_seconds": age,
            "max_spread": self.max_spread,
            "source": "spread_filter",
        }

    def reset_symbol(self, symbol: str):
        self.books.pop(str(symbol).upper(), None)

    def reset_all(self):
        self.books.clear()
