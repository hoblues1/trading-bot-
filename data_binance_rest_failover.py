import logging
import requests
import time


class BinanceRESTFallback:

    def __init__(self):

        self.base = "https://fapi.binance.com"
        self.timeout = 3

        self.last_price = {}
        self.last_orderbook = {}

    # ================= PRICE =================
    def get_price(self, symbol):

        try:

            url = f"{self.base}/fapi/v1/ticker/price"

            r = requests.get(
                url,
                params={"symbol": symbol},
                timeout=self.timeout
            )

            # 🔧 추가 (HTTP 상태 확인)
            if r.status_code != 200:
                raise Exception(f"HTTP ERROR {r.status_code}")

            data = r.json()

            # 🔧 추가 (응답 검증)
            if "price" not in data:
                raise Exception(f"Invalid response: {data}")

            price = float(data["price"])

            self.last_price[symbol] = price

            return price

        except Exception as e:

            logging.error(f"REST price error: {e}")

            return self.last_price.get(symbol)

    # ================= ORDERBOOK =================
    def get_orderbook(self, symbol, limit=20):

        try:

            url = f"{self.base}/fapi/v1/depth"

            r = requests.get(
                url,
                params={
                    "symbol": symbol,
                    "limit": limit
                },
                timeout=self.timeout
            )

            # 🔧 추가 (HTTP 상태 확인)
            if r.status_code != 200:
                raise Exception(f"HTTP ERROR {r.status_code}")

            data = r.json()

            bids = data.get("bids", [])
            asks = data.get("asks", [])

            self.last_orderbook[symbol] = (bids, asks)

            return bids, asks

        except Exception as e:

            logging.error(f"REST orderbook error: {e}")

            return self.last_orderbook.get(symbol, ([], []))

    # ================= HEALTH CHECK =================
    def ping(self):

        try:

            url = f"{self.base}/fapi/v1/ping"

            r = requests.get(url, timeout=self.timeout)

            if r.status_code == 200:

                return True

        except Exception as e:

            logging.error(f"REST ping failed: {e}")

        return False

    # ================= FAILOVER =================
    def fallback_price(self, symbol):

        logging.warning(f"REST FAILOVER PRICE | {symbol}")

        return self.get_price(symbol)

    def fallback_orderbook(self, symbol):

        logging.warning(f"REST FAILOVER ORDERBOOK | {symbol}")

        return self.get_orderbook(symbol)
