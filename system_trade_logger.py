import csv
import datetime
import json
import logging
import os
import threading
from typing import Any, Dict, Optional


class TradeLogger:
    """
    Institutional / hedge-fund style trade journal

    역할:
    1) 진입 / 부분청산 / 전체청산 / 실패 이벤트 저장
    2) jsonl + csv 동시 기록
    3) runtime event / kill switch / sync event 저장
    4) 일자별 파일 분리
    5) main.py 에서 바로 붙일 수 있는 단순 API 제공
    """

    def __init__(
        self,
        base_dir: str = "logs",
        enable_jsonl: bool = True,
        enable_csv: bool = True,
        enable_runtime_log: bool = True,
    ):
        self.base_dir = base_dir
        self.enable_jsonl = bool(enable_jsonl)
        self.enable_csv = bool(enable_csv)
        self.enable_runtime_log = bool(enable_runtime_log)
        self.lock = threading.RLock()

        os.makedirs(self.base_dir, exist_ok=True)

    # ================= INTERNAL =================
    def _now(self) -> datetime.datetime:
        return datetime.datetime.now()

    def _today_str(self) -> str:
        return self._now().strftime("%Y-%m-%d")

    def _ts_str(self) -> str:
        return self._now().strftime("%Y-%m-%d %H:%M:%S")

    def _jsonl_path(self) -> str:
        return os.path.join(self.base_dir, f"trade_journal_{self._today_str()}.jsonl")

    def _csv_path(self) -> str:
        return os.path.join(self.base_dir, f"trade_journal_{self._today_str()}.csv")

    def _runtime_path(self) -> str:
        return os.path.join(self.base_dir, f"runtime_{self._today_str()}.log")

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except Exception:
            return default

    def _normalize_payload(self, trade: Dict[str, Any], event_type: str) -> Dict[str, Any]:
        payload = dict(trade or {})
        payload.setdefault("timestamp", self._ts_str())
        payload.setdefault("event_type", event_type)
        payload.setdefault("symbol", payload.get("symbol"))
        payload.setdefault("side", payload.get("side"))
        payload.setdefault("price", self._safe_float(payload.get("price"), 0.0))
        payload.setdefault("qty", self._safe_float(payload.get("qty"), 0.0))
        payload.setdefault("reason", payload.get("reason"))
        payload.setdefault("status", payload.get("status"))
        payload.setdefault("pnl", self._safe_float(payload.get("pnl"), 0.0))
        return payload

    def _append_jsonl(self, payload: Dict[str, Any]):
        with open(self._jsonl_path(), "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def _append_csv(self, payload: Dict[str, Any]):
        path = self._csv_path()
        file_exists = os.path.exists(path)
        fieldnames = [
            "timestamp",
            "event_type",
            "symbol",
            "side",
            "price",
            "qty",
            "reason",
            "status",
            "pnl",
        ]
        with open(path, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow({k: payload.get(k) for k in fieldnames})

    def _append_runtime(self, message: str):
        with open(self._runtime_path(), "a", encoding="utf-8") as f:
            f.write(f"{self._ts_str()} | {message}\n")

    # ================= PUBLIC =================
    def log(self, trade: Dict[str, Any], event_type: str = "TRADE"):
        payload = self._normalize_payload(trade, event_type)

        with self.lock:
            if self.enable_jsonl:
                self._append_jsonl(payload)
            if self.enable_csv:
                self._append_csv(payload)

        print(
            f"{payload['timestamp']} | {payload['event_type']} | {payload['symbol']} | "
            f"{payload['side']} | {payload['price']} | {payload['qty']} | "
            f"reason={payload.get('reason')} | status={payload.get('status')} | pnl={payload.get('pnl')}"
        )

    def log_entry(self, symbol: str, side: str, price: float, qty: float, reason: str = "entry"):
        self.log(
            {
                "symbol": symbol,
                "side": side,
                "price": price,
                "qty": qty,
                "reason": reason,
                "status": "OPEN",
            },
            event_type="ENTRY",
        )

    def log_partial_close(self, symbol: str, side: str, price: float, qty: float, pnl: float = 0.0, reason: str = "partial_close"):
        self.log(
            {
                "symbol": symbol,
                "side": side,
                "price": price,
                "qty": qty,
                "reason": reason,
                "status": "PARTIAL_CLOSE",
                "pnl": pnl,
            },
            event_type="PARTIAL_CLOSE",
        )

    def log_close(self, symbol: str, side: str, price: float, qty: float, pnl: float = 0.0, reason: str = "close"):
        self.log(
            {
                "symbol": symbol,
                "side": side,
                "price": price,
                "qty": qty,
                "reason": reason,
                "status": "CLOSE",
                "pnl": pnl,
            },
            event_type="CLOSE",
        )

    def log_reject(self, symbol: str, side: str, price: float, qty: float, reason: str = "rejected"):
        self.log(
            {
                "symbol": symbol,
                "side": side,
                "price": price,
                "qty": qty,
                "reason": reason,
                "status": "REJECTED",
            },
            event_type="REJECT",
        )

    def log_runtime_event(self, message: str, level: str = "INFO"):
        with self.lock:
            if self.enable_runtime_log:
                self._append_runtime(f"{level.upper()} | {message}")

        getattr(logging, level.lower(), logging.info)(f"[JOURNAL] {message}")
