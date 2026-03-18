import asyncio
import json
import logging
import time
import websockets


class BinanceWebSocket:

    def __init__(self, symbols, system):
        self.symbols = symbols
        self.system = system

        self.running = False
        self.last_price = {}
        self.latest_price = self.last_price
        self.latest_orderbook = {}

        self.reconnect_delay = 3
        self.max_reconnect_delay = 30

    # ================= STREAM URL =================
    def build_stream_url(self):
        streams = []

        for s in self.symbols:
            s = s.lower()
            streams.append(f"{s}@trade")
            streams.append(f"{s}@depth50@100ms")

        stream = "/".join(streams)
        return f"wss://fstream.binance.com/stream?streams={stream}"

    # ================= START =================
    async def start(self):
        self.running = True

        while self.running:
            try:
                url = self.build_stream_url()

                logging.info("Connecting Binance WebSocket...")
                logging.info(url)

                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_size=None,
                ) as ws:

                    logging.info("WebSocket CONNECTED")
                    self.reconnect_delay = 3

                    await self.listen(ws)

            except Exception as e:
                logging.error(f"WS connection error: {e}")
                logging.info(f"Reconnecting in {self.reconnect_delay}s")

                await asyncio.sleep(self.reconnect_delay)

                self.reconnect_delay = min(
                    self.reconnect_delay * 2,
                    self.max_reconnect_delay,
                )

    # ================= LISTEN =================
    async def listen(self, ws):
        while self.running:
            try:
                msg = await ws.recv()
                raw = json.loads(msg)

                stream = raw.get("stream")
                data = raw.get("data")

                if not stream or not data:
                    continue

                # ================= TRADE =================
                if "@trade" in stream:
                    await self.handle_trade(data)

                # ================= ORDERBOOK =================
                elif "@depth" in stream:
                    await self.handle_orderbook(stream, data)

            except websockets.ConnectionClosed:
                logging.warning("WebSocket closed")
                break

            except Exception as e:
                logging.error(f"WS processing error: {e}")

    # ================= INTERNAL FEED HELPERS =================
    def _feed_trade_to_engines(self, trade):
        """
        micro / pressure / velocity 엔진에 체결을 직접 적재.
        system.on_trade 내부 배선이 빠져 있어도 엔진 deque가 쌓이도록 보강.
        """
        try:
            if hasattr(self.system, "micro") and hasattr(self.system.micro, "update"):
                self.system.micro.update(trade)
        except Exception as e:
            logging.error(f"Micro feed error: {e}")

        try:
            if hasattr(self.system, "pressure") and hasattr(self.system.pressure, "update"):
                self.system.pressure.update(trade)
        except Exception as e:
            logging.error(f"Pressure feed error: {e}")

        try:
            if hasattr(self.system, "velocity") and hasattr(self.system.velocity, "update"):
                self.system.velocity.update(trade)
        except Exception as e:
            logging.error(f"Velocity feed error: {e}")

    def _touch_market(self, symbol: str, ts: float):
        """
        트레이딩 루프가 stale 체크에 쓰는 시장 타임스탬프를 best-effort로 직접 갱신.
        기존 구조는 유지하고, 여러 이름의 내부 필드를 폭넓게 보강한다.
        """
        try:
            if hasattr(self.system, "last_market_ts"):
                cur = getattr(self.system, "last_market_ts")
                if isinstance(cur, dict):
                    cur[symbol] = ts
                else:
                    setattr(self.system, "last_market_ts", ts)
        except Exception as e:
            logging.error(f"touch last_market_ts error: {e}")

        try:
            if hasattr(self.system, "latest_market_ts"):
                cur = getattr(self.system, "latest_market_ts")
                if isinstance(cur, dict):
                    cur[symbol] = ts
                else:
                    setattr(self.system, "latest_market_ts", ts)
        except Exception as e:
            logging.error(f"touch latest_market_ts error: {e}")

        try:
            if hasattr(self.system, "market_timestamps"):
                cur = getattr(self.system, "market_timestamps")
                if isinstance(cur, dict):
                    cur[symbol] = ts
        except Exception as e:
            logging.error(f"touch market_timestamps error: {e}")

        try:
            if hasattr(self.system, "last_data_ts"):
                cur = getattr(self.system, "last_data_ts")
                if isinstance(cur, dict):
                    cur[symbol] = ts
                else:
                    setattr(self.system, "last_data_ts", ts)
        except Exception as e:
            logging.error(f"touch last_data_ts error: {e}")

        try:
            if hasattr(self.system, "market_data_ts"):
                cur = getattr(self.system, "market_data_ts")
                if isinstance(cur, dict):
                    cur[symbol] = ts
                else:
                    setattr(self.system, "market_data_ts", ts)
        except Exception as e:
            logging.error(f"touch market_data_ts error: {e}")

    def _normalize_orderbook_levels(self, levels):
        normalized = []
        for level in levels or []:
            try:
                if isinstance(level, (list, tuple)) and len(level) >= 2:
                    px = float(level[0])
                    qty = float(level[1])
                    normalized.append((px, qty))
                elif isinstance(level, dict):
                    px = float(level.get("price", level.get("p", 0.0)))
                    qty = float(level.get("qty", level.get("q", 0.0)))
                    normalized.append((px, qty))
            except Exception:
                continue
        return normalized

    def _build_orderbook_snapshot(self, symbol, bids, asks, event_ts):
        bids_n = self._normalize_orderbook_levels(bids)
        asks_n = self._normalize_orderbook_levels(asks)

        best_bid = bids_n[0][0] if bids_n else None
        best_ask = asks_n[0][0] if asks_n else None

        spread = None
        mid_price = None
        spread_ratio = None

        if best_bid is not None and best_ask is not None and best_ask > 0:
            spread = max(0.0, best_ask - best_bid)
            mid_price = (best_bid + best_ask) / 2.0
            if mid_price and mid_price > 0:
                spread_ratio = spread / mid_price

        bid_qty_sum = sum(float(q) for _, q in bids_n)
        ask_qty_sum = sum(float(q) for _, q in asks_n)
        top_bid_qty = float(bids_n[0][1]) if bids_n else 0.0
        top_ask_qty = float(asks_n[0][1]) if asks_n else 0.0

        total_top = top_bid_qty + top_ask_qty
        top_book_ratio = ((top_bid_qty - top_ask_qty) / total_top) if total_top > 0 else 0.0

        total_depth = bid_qty_sum + ask_qty_sum
        depth_ratio = ((bid_qty_sum - ask_qty_sum) / total_depth) if total_depth > 0 else 0.0
        book_pressure = (bid_qty_sum - ask_qty_sum) / (bid_qty_sum + ask_qty_sum + 1e-9)

        snapshot = {
            "symbol": symbol,
            "bids": bids_n,
            "asks": asks_n,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": spread,
            "mid_price": mid_price,
            "spread_ratio": spread_ratio,
            "spread_bps": (spread_ratio * 10000.0) if spread_ratio is not None else 0.0,
            "top_bid_qty": top_bid_qty,
            "top_ask_qty": top_ask_qty,
            "bid_qty_sum": bid_qty_sum,
            "ask_qty_sum": ask_qty_sum,
            "top_book_ratio": top_book_ratio,
            "depth_ratio": depth_ratio,
            "book_pressure": book_pressure,
            "timestamp": event_ts,
            "ts": event_ts,
            "source": "binance_ws_depth",
        }
        return snapshot

    def _call_best_effort(self, targets, payloads):
        for target in targets:
            try:
                obj = self.system
                for attr in target.split("."):
                    obj = getattr(obj, attr)

                if callable(obj):
                    for payload in payloads:
                        try:
                            obj(*payload)
                            return True
                        except TypeError:
                            continue
                return True
            except AttributeError:
                continue
            except Exception as e:
                logging.error(f"Feed error [{target}]: {e}")
                return False
        return False

    def _feed_orderbook_to_engines(self, symbol, bids, asks, snapshot):
        """
        orderbook 관련 엔진/모듈에 호가를 직접 적재.
        기존 system.on_orderbook 배선이 불완전해도 imbalance / slippage / analyzer 계층이
        최소한의 최신 book을 보유하도록 보강한다.
        """
        # imbalance 전략 계층
        self._call_best_effort(
            [
                "imbalance.update_orderbook",
                "imbalance.update",
                "strategy_orderbook_imbalance.update_orderbook",
                "strategy_orderbook_imbalance.update",
            ],
            [
                (symbol, bids, asks),
                (symbol, snapshot),
                (snapshot,),
            ],
        )

        # 슬리피지 모듈 계층
        self._call_best_effort(
            [
                "slippage.update_orderbook",
                "slippage.on_orderbook",
                "slippage.update_book",
            ],
            [
                (symbol, bids, asks),
                (symbol, snapshot),
                (snapshot,),
            ],
        )

        # orderbook analyzer / heatmap / depth 계층
        self._call_best_effort(
            [
                "orderbook_analyzer.update",
                "orderbook_analyzer.on_orderbook",
                "heatmap.update",
                "heatmap.on_orderbook",
                "depth_map.update",
                "depth_map.on_orderbook",
            ],
            [
                (symbol, bids, asks),
                (symbol, snapshot),
                (snapshot,),
            ],
        )

        # 시스템에 최신 orderbook 캐시 보관
        try:
            if hasattr(self.system, "latest_orderbook") and isinstance(self.system.latest_orderbook, dict):
                self.system.latest_orderbook[symbol] = snapshot
            elif not hasattr(self.system, "latest_orderbook"):
                self.system.latest_orderbook = {symbol: snapshot}
            else:
                self.system.latest_orderbook[symbol] = snapshot
        except Exception as e:
            logging.error(f"latest_orderbook cache error: {e}")

        # websocket 계층 로컬 캐시
        self.latest_orderbook[symbol] = snapshot

    # ================= TRADE =================
    async def handle_trade(self, data):
        try:
            symbol = data["s"]
            price = float(data["p"])
            qty = float(data["q"])

            # Binance trade event time (ms)
            event_ts_ms = data.get("T", int(time.time() * 1000))
            event_ts = float(event_ts_ms) / 1000.0

            # m=True  -> buyer is maker -> taker SELL
            # m=False -> taker BUY
            if "m" in data:
                side = "SELL" if bool(data["m"]) else "BUY"
            else:
                last = self.last_price.get(symbol)
                if last is None:
                    side = "BUY"
                else:
                    side = "BUY" if price >= last else "SELL"

            self.last_price[symbol] = price
            self.latest_price[symbol] = price

            trade = {
                "symbol": symbol,
                "price": price,
                "qty": qty,
                "size": qty,
                "volume": qty,
                "notional": price * qty,
                "amount": price * qty,
                "timestamp": event_ts,
                "ts": event_ts,
                "time": event_ts_ms,
                "side": side,
                "source": "binance_ws_trade",
                "raw": data,
            }

            self._touch_market(symbol, event_ts)

            if hasattr(self.system, "slippage"):
                try:
                    self.system.slippage.update_price(symbol, price)
                except Exception as e:
                    logging.error(f"Slippage update error: {e}")

            self._feed_trade_to_engines(trade)

            if hasattr(self.system, "on_trade"):
                if asyncio.iscoroutinefunction(self.system.on_trade):
                    await self.system.on_trade(trade)
                else:
                    self.system.on_trade(trade)

        except Exception as e:
            logging.error(f"Trade handler error: {e}")

    # ================= ORDERBOOK =================
    async def handle_orderbook(self, stream, data):
        try:
            symbol = stream.split("@")[0].upper()

            bids = data.get("b", data.get("bids", []))
            asks = data.get("a", data.get("asks", []))

            event_ts_ms = data.get("E", int(time.time() * 1000))
            event_ts = float(event_ts_ms) / 1000.0

            snapshot = self._build_orderbook_snapshot(symbol, bids, asks, event_ts)
            self._touch_market(symbol, event_ts)
            self._feed_orderbook_to_engines(symbol, bids, asks, snapshot)

            if hasattr(self.system, "on_orderbook"):
                if asyncio.iscoroutinefunction(self.system.on_orderbook):
                    await self.system.on_orderbook(symbol, bids, asks)
                else:
                    self.system.on_orderbook(symbol, bids, asks)

        except Exception as e:
            logging.error(f"Orderbook handler error: {e}")
