import time
import logging


class LatencyMonitor:

    def __init__(self):

        self.ws_latency = 0
        self.execution_latency = 0
        self.last_trade_time = 0

        self.max_latency = 1.0  # seconds

    # ================= TRADE LATENCY =================
    def record_trade(self):

        now = time.time()

        if self.last_trade_time != 0:

            self.ws_latency = now - self.last_trade_time

            if self.ws_latency > self.max_latency:

                logging.warning(
                    f"WS LATENCY HIGH | {self.ws_latency:.3f}s"
                )

        self.last_trade_time = now

    # ================= EXECUTION LATENCY =================
    def record_execution(self, start_time):

        latency = time.time() - start_time

        self.execution_latency = latency

        if latency > self.max_latency:

            logging.warning(
                f"EXECUTION LATENCY HIGH | {latency:.3f}s"
            )

    # ================= STATUS =================
    def stats(self):

        return {

            "ws_latency": self.ws_latency,
            "execution_latency": self.execution_latency

        }
