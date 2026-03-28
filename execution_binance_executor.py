from core_module import Module
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException
import asyncio
import logging
import time
from decimal import Decimal, ROUND_DOWN, ROUND_UP


class BinanceExecutor(Module):

    def __init__(self, api_key, api_secret):

        super().__init__()

        self.client = Client(api_key, api_secret)

        self.max_retries = 3
        self.retry_delay = 0.35
        self.order_timeout = 5

        self.last_order_time = 0
        self.min_order_interval = 0.25

        self.symbol_filters = {}

    def set_event_bus(self, bus):

        super().set_event_bus(bus)

        bus.subscribe("execute_order", self.execute)

    def get_market_price(self, symbol):

        try:

            ticker = self.client.futures_mark_price(symbol=symbol)

            return float(ticker["markPrice"])

        except Exception:

            try:

                ticker = self.client.futures_symbol_ticker(symbol=symbol)

                return float(ticker["price"])

            except Exception:

                return 0.0

    def get_symbol_filters(self, symbol):

        if symbol in self.symbol_filters:
            return self.symbol_filters[symbol]

        info = self.client.futures_exchange_info()

        for s in info["symbols"]:

            if s["symbol"] == symbol:

                lot_size = None
                min_qty = None
                min_notional = 5.0
                tick_size = 0.0

                for f in s["filters"]:

                    if f["filterType"] == "LOT_SIZE":

                        lot_size = float(f["stepSize"])
                        min_qty = float(f["minQty"])

                    if f["filterType"] == "MIN_NOTIONAL":

                        min_notional = float(f.get("notional", 5.0))

                    if f["filterType"] == "PRICE_FILTER":

                        tick_size = float(f.get("tickSize", 0.0))

                self.symbol_filters[symbol] = (
                    lot_size or 0.001,
                    min_qty or 0.001,
                    min_notional,
                    tick_size or 0.01,
                )

                return self.symbol_filters[symbol]

        return 0.001, 0.001, 5.0, 0.01

    def adjust_qty(self, symbol, qty, price):

        step, min_qty, min_notional, _ = self.get_symbol_filters(symbol)

        if price <= 0:
            price = self.get_market_price(symbol)

        if price <= 0:
            return 0.0

        if step <= 0:
            step = 0.001

        step_dec = Decimal(str(step))
        qty_dec = Decimal(str(qty))

        qty_adj = (qty_dec // step_dec) * step_dec
        qty_adj = qty_adj.quantize(step_dec, rounding=ROUND_DOWN)

        qty_adj = float(qty_adj)

        if qty_adj < min_qty:
            return 0.0

        notional = qty_adj * price

        if notional < min_notional:
            return 0.0

        return qty_adj

    def adjust_price(self, symbol, price, side):

        _, _, _, tick_size = self.get_symbol_filters(symbol)

        if price <= 0:
            return 0.0

        if tick_size <= 0:
            return float(price)

        tick_dec = Decimal(str(tick_size))
        price_dec = Decimal(str(price))

        if str(side).upper() == "BUY":
            adjusted = ((price_dec / tick_dec).to_integral_value(rounding=ROUND_UP)) * tick_dec
        else:
            adjusted = ((price_dec / tick_dec).to_integral_value(rounding=ROUND_DOWN)) * tick_dec

        return float(adjusted.quantize(tick_dec))

    async def execute(self, order):

        try:

            symbol = str(order["symbol"]).upper().strip()
            side = str(order["side"]).upper().strip()

            raw_qty = abs(round(float(order["qty"]), 8))
            raw_price = float(order.get("price", 0) or 0)

            order_type = str(order.get("type", "MARKET")).upper().strip()
            if order_type not in ("MARKET", "LIMIT"):
                order_type = "MARKET"

            reduce_only = bool(
                order.get("reduce_only", order.get("reduceOnly", False))
            )

            position_side = order.get("positionSide", None)
            time_in_force = order.get("time_in_force", order.get("timeInForce", "GTC"))

            if raw_qty <= 0:

                logging.error("Invalid order size")
                return

            market_price = raw_price if raw_price > 0 else self.get_market_price(symbol)
            qty = self.adjust_qty(symbol, raw_qty, market_price)

            if qty <= 0:

                logging.warning(
                    f"Order blocked (invalid size/notional) | {symbol} | raw={raw_qty}"
                )

                return

            now = time.time()

            elapsed = now - self.last_order_time
            if elapsed < self.min_order_interval:
                await asyncio.sleep(self.min_order_interval - elapsed)

            self.last_order_time = time.time()

            params = {
                "symbol": symbol,
                "side": side,
                "type": order_type,
                "quantity": qty,
            }

            if reduce_only:
                params["reduceOnly"] = True

            if position_side in ("LONG", "SHORT"):
                params["positionSide"] = position_side

            if order_type == "LIMIT":
                limit_price = self.adjust_price(symbol, raw_price, side)

                if limit_price <= 0:
                    logging.error(f"Invalid limit price | {symbol} | raw_price={raw_price}")
                    return

                params["price"] = limit_price
                params["timeInForce"] = time_in_force

            logging.info(
                f"EXECUTE | {symbol} | {side} | qty={qty} | type={order_type}"
                + (f" | price={params['price']}" if "price" in params else "")
                + (f" | reduceOnly={params['reduceOnly']}" if "reduceOnly" in params else "")
                + (f" | positionSide={params['positionSide']}" if "positionSide" in params else "")
            )

            for attempt in range(self.max_retries):

                try:

                    result = self.client.futures_create_order(**params)

                    order_id = result.get("orderId")

                    verified = self.verify_order(symbol, order_id)

                    if verified:

                        if self.event_bus:

                            pub = self.event_bus.publish(
                                "order_filled",
                                result
                            )

                            if asyncio.iscoroutine(pub):
                                await pub

                        return result

                    else:

                        logging.warning(
                            f"Order not verified | attempt={attempt+1} | symbol={symbol}"
                        )

                except BinanceAPIException as e:

                    logging.error(f"Binance API error: {e}")

                    if e.code == -2019:
                        logging.error("Insufficient margin")
                        break

                    if e.code == -1111:
                        logging.error("Precision error")

                    if e.code == -4164:
                        logging.error("Min notional error")

                    if e.code == -2022:
                        logging.error("ReduceOnly rejected")

                except BinanceOrderException as e:

                    logging.error(f"Order error: {e}")

                except Exception as e:

                    logging.error(
                        f"Unknown execution error: {e}"
                    )

                await asyncio.sleep(
                    self.retry_delay * (attempt + 1)
                )

            logging.error(
                f"ORDER FAILED | {symbol} | {side} | type={order_type}"
            )

            if self.event_bus:

                pub = self.event_bus.publish(
                    "order_failed",
                    {
                        "symbol": symbol,
                        "side": side,
                        "qty": qty,
                        "type": order_type,
                    }
                )

                if asyncio.iscoroutine(pub):
                    await pub

        except Exception as e:

            logging.error(
                f"Execution fatal error: {e}"
            )

    def verify_order(self, symbol, order_id):

        try:

            order = self.client.futures_get_order(
                symbol=symbol,
                orderId=order_id
            )

            status = order.get("status")

            if status in ["FILLED", "PARTIALLY_FILLED", "NEW"]:
                return True

            return False

        except Exception as e:

            logging.error(
                f"Order verification error: {e}"
            )

            return False
