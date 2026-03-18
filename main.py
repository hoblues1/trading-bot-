import asyncio
import inspect
import logging
import os
import time
from typing import Optional, Dict, Any, Tuple

from config import (
    SYMBOLS,
    INITIAL_CAPITAL,
    USE_PAPER_TRADING,
    API_KEY,
    API_SECRET,
    LEVERAGE,
    MAX_DAILY_TRADES,
    MICRO_WINDOW_SECONDS,
    MICRO_IMBALANCE_THRESHOLD,
    MICRO_MIN_TOTAL_FLOW,
    MICRO_MIN_TRADE_COUNT,
    MICRO_MIN_NET_FLOW_RATIO,
    MICRO_SIGNAL_COOLDOWN_SECONDS,
    MICRO_SAME_SIDE_REARM_SECONDS,
    MICRO_OPPOSITE_SIDE_CONFIRM_RATIO,
    MICRO_RESET_ON_SIGNAL,
    MICRO_MAX_STORED_TRADES,
    ALPHA_BUY_THRESHOLD,
    ALPHA_SELL_THRESHOLD,
    ALPHA_MIN_AGREE_COUNT,
    ALPHA_SIGNAL_COOLDOWN_SECONDS,
    ALPHA_SAME_SIDE_REARM_SECONDS,
    ALPHA_FLIP_BLOCK_SECONDS,
    ALPHA_STRONG_FLIP_MULTIPLIER,
    EXECUTOR_MIN_ORDER_INTERVAL_SECONDS,
    EXECUTOR_SYMBOL_LOCK_SECONDS,
    EXECUTOR_RETRY_ATTEMPTS,
    EXECUTOR_RETRY_SLEEP_SECONDS,
    EXECUTOR_QTY_PRECISION,
    EXECUTOR_MIN_QTY,
    MIN_SIGNAL_INTERVAL_SECONDS,
    PRESSURE_WINDOW_SECONDS,
    PRESSURE_RATIO,
    PRESSURE_MIN_TOTAL_VOLUME,
    PRESSURE_MIN_TRADE_COUNT,
    PRESSURE_MIN_NET_VOLUME_RATIO,
    PRESSURE_MIN_AVG_TRADE_SIZE_RATIO,
    PRESSURE_SIGNAL_COOLDOWN_SECONDS,
    PRESSURE_SAME_SIDE_REARM_SECONDS,
    PRESSURE_OPPOSITE_SIDE_CONFIRM_RATIO,
    PRESSURE_MAX_STORED_TRADES,
)

from binance.client import Client

from data_binance_ws import BinanceWebSocket
from data_binance_rest_failover import BinanceRESTFallback

# SIGNAL
from strategy_microstructure_alpha import MicrostructureAlpha
from strategy_orderflow_pressure_engine import OrderflowPressureEngine
from strategy_trade_velocity_engine import TradeVelocityEngine
from strategy_volatility_engine import VolatilityEngine
from strategy_orderbook_imbalance import OrderbookImbalanceStrategy

# STRATEGY
from strategy_alpha_fusion_engine import AlphaFusionEngine
from strategy_trade_filter_engine import TradeFilterEngine

# MARKET
from ai_market_regime_engine import MarketRegimeEngine

# PORTFOLIO
from portfolio_position_engine import PositionEngine
from portfolio_dynamic_sizing import DynamicSizing
from portfolio_pnl_engine import PnLEngine

# EXECUTION
from execution_slippage_control import SlippageControl
from execution_smart_order_router import SmartOrderRouter
from execution_smart_executor import SmartExecutor

# SAFETY
from system_kill_switch import KillSwitch

# CORE
from core_latency_monitor import LatencyMonitor
from core_spread_filter import SpreadFilter

# SYNC
from exchange_position_sync import ExchangePositionSync
from system_trade_logger import TradeLogger

# ===== ADVANCED ENGINES =====
from capital_adaptive_controller import CapitalAdaptiveController
from adaptive_threshold_engine import AdaptiveThresholdEngine
from position_lifecycle_manager import PositionLifecycleManager
from ai_signal_quality_engine import AISignalQualityEngine
from regime_transition_guard import RegimeTransitionGuard
from strategy_weight_optimizer import StrategyWeightOptimizer

from meta_strategy_controller import MetaStrategyController
from execution_alpha_coordinator import ExecutionAlphaCoordinator
from performance_feedback_loop import PerformanceFeedbackLoop
from signal_consensus_guard import SignalConsensusGuard


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def boot_log(msg: str):
    logging.info(f"[BOOT] {msg}")


def engine_log(name: str, obj):
    cls = obj.__class__.__name__ if obj is not None else "None"
    mod = obj.__class__.__module__ if obj is not None else "None"
    logging.info(f"[ENGINE] {name} initialized | class={cls} | module={mod}")


class TradingSystem:
    """
    Institutional / hedge-fund style main orchestration layer

    핵심 보강:
    1) CapitalAdaptiveController 결과를 실전 엔진에 실제 반영
    2) 초소액에서는 max_positions=1 / risk_per_trade 축소 자동 적용
    3) NEW / ACCEPTED 를 체결 성공과 분리
    4) 진입 실패 후 즉시 재진입 반복 방지
    5) 막 청산된 심볼 즉시 재진입 완화
    6) 신호 품질 점수화 후 저질 진입 차단
    7) regime / signal consensus / execution quality를 sizing에 더 깊게 반영
    8) post execution reconcile 시 실제 fill 정보 우선 사용
    9) kill / spread / stale market / duplicate entry 방어 강화
    10) paper/live 공용 유지
    11) capital_mode는 risk/sizing 레이어 중심으로 반영하고,
        alpha 엔진 자체를 과도하게 덮어써서 거래가 죽지 않도록 유지
    12) 일일 거래 횟수 제한(MAX_DAILY_TRADES) 적용
    """

    FINAL_SUCCESS_STATUSES = {"FILLED", "PARTIALLY_FILLED", "SUCCESS", "EXECUTED"}
    ACCEPTED_ONLY_STATUSES = {"NEW", "ACCEPTED"}
    FINAL_FAILURE_STATUSES = {"FAILED", "REJECTED", "CANCELED", "CANCELLED", "EXPIRED", "ERROR"}

    def __init__(self):
        logging.info("===== TRADING SYSTEM BOOT =====")
        boot_log(f"main file = {os.path.abspath(__file__)}")
        boot_log("TradingSystem __init__ entered")

        self.symbols = [str(s).upper().strip() for s in SYMBOLS]
        self.start_time = time.time()
        self.updated_at = self.start_time

        self.last_signal_ts: Dict[str, float] = {}
        self.last_trade_ts_by_symbol: Dict[str, float] = {}
        self.last_order_result_by_symbol: Dict[str, Any] = {}
        self.last_market_ready_ts: Dict[str, float] = {}
        self.last_position_manage_ts: Dict[str, float] = {}
        self.last_failed_entry_ts: Dict[str, float] = {}
        self.last_flatten_ts: Dict[str, float] = {}
        self.last_exec_ok_ts: Dict[str, float] = {}
        self.last_regime_by_symbol: Dict[str, Any] = {}
        self.last_volatility_by_symbol: Dict[str, float] = {}
        self.loop_iterations = 0

        self.min_signal_interval_seconds = MIN_SIGNAL_INTERVAL_SECONDS
        self.max_market_staleness_seconds = 5.0
        self.position_manage_interval_seconds = 0.35
        self.balance_refresh_interval_seconds = 20.0
        self.failed_entry_cooldown_seconds = 2.8
        self.post_close_reentry_cooldown_seconds = 1.6
        self.last_balance_refresh_ts = 0.0

        self.latest_trade_by_symbol: Dict[str, Dict[str, Any]] = {}

        self.loop_sleep_seconds = 2.0
        self.last_trade_loop_log_ts: Dict[str, float] = {}
        self.trade_loop_log_interval = 5.0

        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_requested = False

        self.current_trade_day_key = time.strftime("%Y-%m-%d")
        self.daily_trade_count = 0
        self.max_daily_trades = int(MAX_DAILY_TRADES)

        self.client = Client(API_KEY, API_SECRET)
        boot_log("Binance Client initialized")

        self.balance = self._fetch_initial_balance()
        logging.info(f"Initial Balance (Futures USDT): {self.balance}")

        # DATA
        self.ws = BinanceWebSocket(self.symbols, self)
        engine_log("ws", self.ws)

        self.rest = BinanceRESTFallback()
        engine_log("rest", self.rest)

        # LATENCY
        self.latency = LatencyMonitor()
        engine_log("latency", self.latency)

        # SIGNAL
        self.micro = MicrostructureAlpha(
            window_seconds=MICRO_WINDOW_SECONDS,
            imbalance_threshold=MICRO_IMBALANCE_THRESHOLD,
            min_total_flow=MICRO_MIN_TOTAL_FLOW,
            min_trade_count=MICRO_MIN_TRADE_COUNT,
            min_net_flow_ratio=MICRO_MIN_NET_FLOW_RATIO,
            signal_cooldown_seconds=MICRO_SIGNAL_COOLDOWN_SECONDS,
            same_side_rearm_seconds=MICRO_SAME_SIDE_REARM_SECONDS,
            opposite_side_confirmation_ratio=MICRO_OPPOSITE_SIDE_CONFIRM_RATIO,
            reset_on_signal=MICRO_RESET_ON_SIGNAL,
            max_stored_trades_per_symbol=MICRO_MAX_STORED_TRADES,
        )
        engine_log("micro", self.micro)

        self.pressure = OrderflowPressureEngine(
            window_seconds=PRESSURE_WINDOW_SECONDS,
            pressure_ratio=PRESSURE_RATIO,
            min_total_volume=PRESSURE_MIN_TOTAL_VOLUME,
            min_trade_count=PRESSURE_MIN_TRADE_COUNT,
            min_net_volume_ratio=PRESSURE_MIN_NET_VOLUME_RATIO,
            min_avg_trade_size_ratio=PRESSURE_MIN_AVG_TRADE_SIZE_RATIO,
            signal_cooldown_seconds=PRESSURE_SIGNAL_COOLDOWN_SECONDS,
            same_side_rearm_seconds=PRESSURE_SAME_SIDE_REARM_SECONDS,
            opposite_side_confirmation_ratio=PRESSURE_OPPOSITE_SIDE_CONFIRM_RATIO,
        )
        engine_log("pressure", self.pressure)

        self.velocity = TradeVelocityEngine()
        engine_log("velocity", self.velocity)

        self.volatility = VolatilityEngine()
        engine_log("volatility", self.volatility)

        self.imbalance = OrderbookImbalanceStrategy()
        engine_log("imbalance", self.imbalance)

        # STRATEGY
        self.alpha = AlphaFusionEngine(
            self.micro,
            self.pressure,
            self.velocity,
            self.volatility,
            buy_threshold=abs(ALPHA_BUY_THRESHOLD),
            sell_threshold=abs(ALPHA_SELL_THRESHOLD),
            min_agree_count=ALPHA_MIN_AGREE_COUNT,
            signal_cooldown_seconds=ALPHA_SIGNAL_COOLDOWN_SECONDS,
            same_side_rearm_seconds=ALPHA_SAME_SIDE_REARM_SECONDS,
            flip_block_seconds=ALPHA_FLIP_BLOCK_SECONDS,
            strong_flip_multiplier=ALPHA_STRONG_FLIP_MULTIPLIER,
            use_volatility_filter=True,
            min_volatility=0.0,
            max_volatility=999999.0,
        )
        engine_log("alpha", self.alpha)

        self.filter = TradeFilterEngine()
        engine_log("filter", self.filter)

        # MARKET
        self.regime = MarketRegimeEngine()
        engine_log("regime", self.regime)

        # PORTFOLIO
        self.position = PositionEngine()
        engine_log("position", self.position)

        self.sizing = DynamicSizing()
        engine_log("sizing", self.sizing)

        self.pnl = PnLEngine()
        engine_log("pnl", self.pnl)

        try:
            self.pnl.set_balance(self.balance)
        except Exception:
            pass

        # POSITION SYNC
        self.sync = ExchangePositionSync(self.client, self.position, self.pnl)
        engine_log("sync", self.sync)

        # EXECUTION
        self.slippage = SlippageControl()
        engine_log("slippage", self.slippage)

        self.router = SmartOrderRouter()
        engine_log("router", self.router)

        self.executor = SmartExecutor(
            self.client,
            position_engine=self.position,
            min_order_interval_seconds=EXECUTOR_MIN_ORDER_INTERVAL_SECONDS,
            symbol_lock_seconds=EXECUTOR_SYMBOL_LOCK_SECONDS,
            retry_attempts=EXECUTOR_RETRY_ATTEMPTS,
            retry_sleep_seconds=EXECUTOR_RETRY_SLEEP_SECONDS,
            qty_precision=EXECUTOR_QTY_PRECISION,
            min_qty=EXECUTOR_MIN_QTY,
        )
        engine_log("executor", self.executor)

        # CORE
        self.spread = SpreadFilter()
        engine_log("spread", self.spread)

        # SAFETY
        self.kill = KillSwitch()
        engine_log("kill", self.kill)
        try:
            self.kill.set_start_balance(self.balance)
        except Exception:
            pass

        self.trade_logger = TradeLogger()
        engine_log("trade_logger", self.trade_logger)

        # ===== ADVANCED CONTROL ENGINES =====
        self.capital_controller = CapitalAdaptiveController()
        engine_log("capital_controller", self.capital_controller)

        self.threshold_engine = AdaptiveThresholdEngine()
        engine_log("threshold_engine", self.threshold_engine)

        self.position_lifecycle = PositionLifecycleManager()
        engine_log("position_lifecycle", self.position_lifecycle)

        self.signal_quality = AISignalQualityEngine()
        engine_log("signal_quality", self.signal_quality)

        self.regime_guard = RegimeTransitionGuard()
        engine_log("regime_guard", self.regime_guard)

        self.weight_optimizer = StrategyWeightOptimizer()
        engine_log("weight_optimizer", self.weight_optimizer)

        self.meta_controller = MetaStrategyController()
        engine_log("meta_controller", self.meta_controller)

        self.execution_coordinator = ExecutionAlphaCoordinator()
        engine_log("execution_coordinator", self.execution_coordinator)

        self.performance_loop = PerformanceFeedbackLoop()
        engine_log("performance_loop", self.performance_loop)

        self.signal_consensus = SignalConsensusGuard()
        engine_log("signal_consensus", self.signal_consensus)

        # ===== RUNTIME CAPITAL MODE =====
        self.runtime_mode: Dict[str, Any] = {}
        self.runtime_max_positions: int = 1
        self.runtime_risk_per_trade: float = 0.008
        self.runtime_alpha_threshold: float = abs(ALPHA_BUY_THRESHOLD)
        self.runtime_min_agree_count: int = ALPHA_MIN_AGREE_COUNT
        self.runtime_cooldown_seconds: int = ALPHA_SIGNAL_COOLDOWN_SECONDS
        self.runtime_leverage_cap: int = LEVERAGE

        self._apply_capital_mode(force=True)

        boot_log("System Initialized")

    # ================= INTERNAL =================
    def _touch(self):
        self.updated_at = time.time()

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except Exception:
            return default

    def _safe_int(self, value: Any, default: int = 0) -> int:
        try:
            if value is None:
                return default
            return int(value)
        except Exception:
            return default

    def _reset_daily_trade_counter_if_needed(self):
        day_key = time.strftime("%Y-%m-%d")
        if day_key != self.current_trade_day_key:
            self.current_trade_day_key = day_key
            self.daily_trade_count = 0

    def _can_trade_today(self) -> bool:
        self._reset_daily_trade_counter_if_needed()
        return self.daily_trade_count < self.max_daily_trades

    def _mark_trade_count(self, count: int = 1):
        self._reset_daily_trade_counter_if_needed()
        self.daily_trade_count += max(0, int(count))

    def _fetch_initial_balance(self) -> float:
        try:
            balances = self.client.futures_account_balance()
            usdt_balance = next((b for b in balances if b["asset"] == "USDT"), None)
            if usdt_balance:
                return float(usdt_balance["balance"])
            return float(INITIAL_CAPITAL)
        except Exception as e:
            logging.error(f"Balance fetch failed: {e}")
            return float(INITIAL_CAPITAL)

    def _refresh_balance_safe(self, force: bool = False):
        try:
            now = time.time()
            if not force and (now - self.last_balance_refresh_ts) < self.balance_refresh_interval_seconds:
                return

            new_balance = self._fetch_initial_balance()
            if new_balance > 0:
                self.balance = new_balance
                try:
                    self.pnl.set_balance(new_balance)
                except Exception:
                    pass
                self.last_balance_refresh_ts = now
                self._touch()
        except Exception as e:
            logging.error(f"Balance refresh failed: {e}")

    def _sync_kill_switch_from_pnl(self):
        try:
            stats = self.pnl.stats()
            if hasattr(self.kill, "sync_from_stats"):
                self.kill.sync_from_stats(stats)
            else:
                balance = self._safe_float(stats.get("balance"), self.balance)
                equity = self._safe_float(stats.get("equity"), balance)
                self.kill.update_balance(balance, equity)
                self.kill.set_daily_total_pnl(self._safe_float(stats.get("total_pnl"), 0.0))
            self._touch()
        except Exception as e:
            logging.error(f"Kill switch sync error: {e}")

    def _apply_symbol_leverage_cap(self):
        applied = min(int(LEVERAGE), int(self.runtime_leverage_cap))
        for s in self.symbols:
            try:
                self.client.futures_change_leverage(symbol=s, leverage=applied)
                logging.info(f"[CAPITAL_MODE] leverage applied | {s} | {applied}x")
            except Exception as e:
                logging.error(f"[CAPITAL_MODE] leverage set failed | {s} | {e}")

    def _estimate_recent_win_rate(self) -> float:
        try:
            stats = self.pnl.stats()
            wins = self._safe_float(stats.get("win_count", stats.get("wins", 0.0)), 0.0)
            losses = self._safe_float(stats.get("loss_count", stats.get("losses", 0.0)), 0.0)
            total = wins + losses
            if total <= 0:
                return 0.50
            return wins / total
        except Exception:
            return 0.50

    def _estimate_recent_pnl_pct(self) -> float:
        try:
            stats = self.pnl.stats()
            balance = self._safe_float(stats.get("balance"), self.balance)
            total_pnl = self._safe_float(stats.get("total_pnl", stats.get("realized_pnl", 0.0)), 0.0)
            if balance <= 0:
                return 0.0
            return total_pnl / balance
        except Exception:
            return 0.0

    def _estimate_loss_streak(self) -> int:
        try:
            if hasattr(self.kill, "loss_streak"):
                return self._safe_int(getattr(self.kill, "loss_streak"), 0)
            status = self.kill.status() if hasattr(self.kill, "status") else {}
            return self._safe_int(status.get("loss_streak"), 0)
        except Exception:
            return 0

    def _estimate_drawdown_pct(self) -> float:
        try:
            status = self.kill.status() if hasattr(self.kill, "status") else {}
            return abs(self._safe_float(status.get("drawdown", status.get("drawdown_ratio", 0.0)), 0.0))
        except Exception:
            return 0.0

    def _current_regime_for_controller(self) -> str:
        try:
            for _, value in self.last_regime_by_symbol.items():
                if isinstance(value, dict):
                    regime = value.get("regime") or value.get("state") or value.get("market_regime")
                    if regime:
                        return str(regime).upper().strip()
                elif value is not None:
                    return str(value).upper().strip()
        except Exception:
            pass
        return "UNKNOWN"

    def _current_volatility_state_for_controller(self) -> str:
        vals = [v for v in self.last_volatility_by_symbol.values() if self._safe_float(v, 0.0) > 0]
        if not vals:
            return "NORMAL"

        avg_vol = sum(vals) / max(len(vals), 1)
        if avg_vol >= 0.025:
            return "PANIC"
        if avg_vol >= 0.015:
            return "EXTREME"
        if avg_vol <= 0.003:
            return "LOW"
        return "NORMAL"

    def _apply_capital_mode(self, force: bool = False):
        try:
            recent_win_rate = self._estimate_recent_win_rate()
            recent_pnl_pct = self._estimate_recent_pnl_pct()
            current_drawdown_pct = self._estimate_drawdown_pct()
            loss_streak = self._estimate_loss_streak()
            regime = self._current_regime_for_controller()
            volatility_state = self._current_volatility_state_for_controller()

            mode = self.capital_controller.evaluate(
                capital=self.balance,
                recent_win_rate=recent_win_rate,
                recent_pnl_pct=recent_pnl_pct,
                current_drawdown_pct=current_drawdown_pct,
                loss_streak=loss_streak,
                regime=regime,
                volatility_state=volatility_state,
            )

            changed = force or (mode.get("mode") != self.runtime_mode.get("mode"))

            self.runtime_mode = mode
            self.runtime_max_positions = self._safe_int(mode.get("max_positions"), 1)
            self.runtime_risk_per_trade = self._safe_float(mode.get("risk_per_trade"), 0.008)
            self.runtime_alpha_threshold = self._safe_float(mode.get("alpha_threshold"), abs(ALPHA_BUY_THRESHOLD))
            self.runtime_min_agree_count = self._safe_int(mode.get("min_agree_count"), ALPHA_MIN_AGREE_COUNT)
            self.runtime_cooldown_seconds = self._safe_int(mode.get("cooldown_seconds"), ALPHA_SIGNAL_COOLDOWN_SECONDS)
            self.runtime_leverage_cap = self._safe_int(mode.get("leverage_cap"), LEVERAGE)

            if hasattr(self.position, "max_positions"):
                self.position.max_positions = self.runtime_max_positions

            if hasattr(self.position, "risk_per_trade"):
                self.position.risk_per_trade = self.runtime_risk_per_trade

            if hasattr(self.sizing, "risk_per_trade"):
                self.sizing.risk_per_trade = self.runtime_risk_per_trade

            # 중요:
            # alpha의 threshold / agree / cooldown을 capital_mode로 계속 덮어쓰면
            # 실전에서 거래가 죽을 수 있으므로 여기서는 관측용/런타임 보관만 하고
            # 실제 전략 파라미터는 기본 config 중심 유지한다.
            # 즉 risk/sizing/positions/leverage 위주 반영.

            if changed:
                logging.warning(
                    f"[CAPITAL_MODE] mode={mode.get('mode')} | base={mode.get('base_mode')} | "
                    f"capital={self.balance:.4f} | max_positions={self.runtime_max_positions} | "
                    f"risk_per_trade={self.runtime_risk_per_trade:.4f} | "
                    f"alpha_threshold(observe)={self.runtime_alpha_threshold:.4f} | "
                    f"agree(observe)={self.runtime_min_agree_count} | "
                    f"cooldown(observe)={self.runtime_cooldown_seconds} | "
                    f"lev_cap={self.runtime_leverage_cap}"
                )
                self._apply_symbol_leverage_cap()

        except Exception as e:
            logging.error(f"[CAPITAL_MODE] apply failed | {e}")

    def _normalize_signal(self, value: Any) -> int:
        if value is None:
            return 0
        if isinstance(value, bool):
            return 1 if value else 0
        if isinstance(value, (int, float)):
            if value > 0:
                return 1
            if value < 0:
                return -1
            return 0
        if isinstance(value, str):
            s = value.strip().upper()
            if s in ("BUY", "LONG", "BULL", "UP", "1", "+1"):
                return 1
            if s in ("SELL", "SHORT", "BEAR", "DOWN", "-1"):
                return -1
            return 0
        if isinstance(value, dict):
            for key in ("signal", "side", "action", "direction", "final_signal"):
                if key in value:
                    return self._normalize_signal(value[key])
        return 0

    def _extract_result_status(self, result: Any) -> str:
        if not isinstance(result, dict):
            return ""
        return str(result.get("execution_status") or result.get("status") or "").upper().strip()

    def _extract_result_ok(self, result: Any) -> bool:
        if isinstance(result, bool):
            return result
        if isinstance(result, dict):
            status = self._extract_result_status(result)
            if status in self.FINAL_FAILURE_STATUSES:
                return False
            if status in self.FINAL_SUCCESS_STATUSES:
                return True
            if status in self.ACCEPTED_ONLY_STATUSES:
                return False
            if result.get("ok") is True or result.get("success") is True:
                if status:
                    return status not in self.ACCEPTED_ONLY_STATUSES
                return bool(result.get("orderId") or result.get("order_id"))
            return False
        return bool(result)

    def _extract_fill_qty(self, result: Any, fallback_qty: float = 0.0) -> float:
        if isinstance(result, dict):
            if result.get("split") and isinstance(result.get("results"), list):
                total = 0.0
                for r in result.get("results", []):
                    if isinstance(r, dict):
                        total += self._safe_float(
                            r.get("executedQty", r.get("executed_qty", r.get("filled_qty", 0.0))),
                            0.0,
                        )
                if total > 0:
                    return total
            return self._safe_float(
                result.get("executedQty", result.get("executed_qty", result.get("filled_qty", fallback_qty))),
                fallback_qty,
            )
        return fallback_qty

    def _extract_fill_price(self, result: Any, fallback_price: float = 0.0) -> float:
        if isinstance(result, dict):
            if result.get("split") and isinstance(result.get("results"), list):
                weighted_notional = 0.0
                weighted_qty = 0.0
                for r in result.get("results", []):
                    if isinstance(r, dict):
                        qty = self._safe_float(
                            r.get("executedQty", r.get("executed_qty", r.get("filled_qty", 0.0))),
                            0.0,
                        )
                        px = self._safe_float(
                            r.get("avgPrice", r.get("avg_price", r.get("price", 0.0))),
                            0.0,
                        )
                        if qty > 0 and px > 0:
                            weighted_notional += qty * px
                            weighted_qty += qty
                if weighted_qty > 0:
                    return weighted_notional / weighted_qty
            return self._safe_float(
                result.get("avgPrice", result.get("avg_price", result.get("price", fallback_price))),
                fallback_price,
            )
        return fallback_price

    def _extract_execution_quality(self, symbol: str) -> Optional[Dict[str, Any]]:
        try:
            monitor = getattr(self.router, "execution_quality_monitor", None)
            if monitor is None:
                return None
            return monitor.get_health_flag(symbol)
        except Exception:
            return None

    async def _call_maybe_async(self, fn, *args, **kwargs):
        result = fn(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    async def _try_engine_methods(
        self,
        engine: Any,
        candidate_methods: Tuple[str, ...],
        symbol: str,
        market: Dict[str, Any],
        extras: Optional[Dict[str, Any]] = None,
    ) -> Any:
        if engine is None:
            return None

        extras = extras or {}
        for name in candidate_methods:
            fn = getattr(engine, name, None)
            if fn is None or not callable(fn):
                continue

            call_patterns = [
                {"symbol": symbol, "market": market, **extras},
                {"symbol": symbol, **market, **extras},
                {"symbol": symbol, "data": market, **extras},
                {"symbol": symbol, "snapshot": market, **extras},
                {"symbol": symbol, **extras},
                {**market, **extras},
                {"market": market, **extras},
                {"data": market, **extras},
                {"snapshot": market, **extras},
                {},
            ]

            for kwargs in call_patterns:
                try:
                    return await self._call_maybe_async(fn, **kwargs)
                except TypeError:
                    continue
                except Exception as e:
                    logging.error(f"{engine.__class__.__name__}.{name} failed | {symbol} | {e}")
                    return None
        return None

    def _score_signal_bundle(self, signals: Dict[str, int]) -> Dict[str, Any]:
        weights = {
            "micro": 1.35,
            "pressure": 1.20,
            "velocity": 0.85,
            "volatility": 0.55,
            "imbalance": 1.05,
        }
        weighted_score = 0.0
        positive = 0
        negative = 0
        non_zero = 0

        for key, value in signals.items():
            v = self._safe_float(value, 0.0)
            if v > 0:
                positive += 1
                non_zero += 1
            elif v < 0:
                negative += 1
                non_zero += 1
            weighted_score += weights.get(key, 1.0) * v

        dominant_side = 1 if weighted_score > 0 else -1 if weighted_score < 0 else 0
        consensus = positive if dominant_side > 0 else negative if dominant_side < 0 else 0
        conflict = min(positive, negative)
        confidence = 0.0
        if non_zero > 0:
            confidence = min(1.0, max(0.0, (abs(weighted_score) / 4.0) * (consensus / max(non_zero, 1))))

        return {
            "weighted_score": weighted_score,
            "positive_count": positive,
            "negative_count": negative,
            "non_zero_count": non_zero,
            "dominant_side": dominant_side,
            "consensus_count": consensus,
            "conflict_count": conflict,
            "confidence": confidence,
        }

    def _regime_allows_entry(self, regime: Any, alpha_signal: int) -> bool:
        if regime is None:
            return True
        if isinstance(regime, bool):
            return regime
        if isinstance(regime, dict):
            regime_label = str(regime.get("regime", regime.get("state", ""))).upper().strip()
            if regime_label in {"WARMUP", "SHOCK", "LOW_LIQUIDITY"}:
                return False

            for key in ("allow_trade", "allow", "allowed", "ok", "pass"):
                if key in regime and regime[key] is False:
                    if regime_label not in {"TREND_UP", "TREND_DOWN", "TREND", "RANGE"}:
                        return False

            trend_bias = regime.get("trend_bias") or regime.get("bias") or regime.get("direction")
            if trend_bias is not None:
                bias = self._normalize_signal(trend_bias)
                if bias != 0 and alpha_signal != 0 and bias != alpha_signal:
                    strength = self._safe_float(regime.get("trend_strength", regime.get("strength", 0.0)), 0.0)
                    if strength >= 0.65:
                        return False
        return True

    def _entry_quality_pass(self, symbol: str, alpha_signal: int, bundle_score: Dict[str, Any], regime: Any) -> bool:
        if alpha_signal == 0:
            return False

        dominant_side = bundle_score.get("dominant_side", 0)
        if dominant_side != 0 and dominant_side != alpha_signal:
            logging.info(f"[ENTRY_BLOCK] {symbol} | dominant_side mismatch | alpha={alpha_signal} dominant={dominant_side}")
            return False

        consensus = self._safe_int(bundle_score.get("consensus_count"), 0)
        conflict = self._safe_int(bundle_score.get("conflict_count"), 0)
        confidence = self._safe_float(bundle_score.get("confidence"), 0.0)
        weighted_score = self._safe_float(bundle_score.get("weighted_score"), 0.0)

        if consensus < 2:
            logging.info(f"[ENTRY_BLOCK] {symbol} | low consensus={consensus}")
            return False
        if abs(weighted_score) < 1.0:
            logging.info(f"[ENTRY_BLOCK] {symbol} | weighted_score too weak={weighted_score:.4f}")
            return False
        if confidence < 0.28:
            logging.info(f"[ENTRY_BLOCK] {symbol} | confidence too weak={confidence:.4f}")
            return False
        if conflict >= 2 and confidence < 0.25:
            logging.info(f"[ENTRY_BLOCK] {symbol} | conflict too high={conflict} confidence={confidence:.4f}")
            return False
        if not self._regime_allows_entry(regime, alpha_signal):
            logging.info(f"[ENTRY_BLOCK] {symbol} | regime veto")
            return False

        return True

    def _has_open_position(self, symbol: str) -> bool:
        for method_name in ("has_position", "has_open_position", "is_open", "exists"):
            fn = getattr(self.position, method_name, None)
            if callable(fn):
                try:
                    return bool(fn(symbol))
                except TypeError:
                    try:
                        return bool(fn(symbol=symbol))
                    except Exception:
                        pass
                except Exception:
                    pass

        for attr in ("positions", "open_positions", "position_map", "state"):
            obj = getattr(self.position, attr, None)
            if isinstance(obj, dict):
                val = obj.get(symbol)
                if val:
                    return True
        return False

    def _throttle_signal(self, symbol: str) -> bool:
        now = time.time()
        last_ts = self.last_signal_ts.get(symbol, 0.0)
        if now - last_ts < self.min_signal_interval_seconds:
            return True
        self.last_signal_ts[symbol] = now
        return False

    def _should_log_symbol(self, symbol: str) -> bool:
        now = time.time()
        last = self.last_trade_loop_log_ts.get(symbol, 0.0)
        if now - last >= self.trade_loop_log_interval:
            self.last_trade_loop_log_ts[symbol] = now
            return True
        return False

    def _should_manage_position(self, symbol: str) -> bool:
        now = time.time()
        last = self.last_position_manage_ts.get(symbol, 0.0)
        if now - last >= self.position_manage_interval_seconds:
            self.last_position_manage_ts[symbol] = now
            return True
        return False

    def _can_attempt_new_entry(self, symbol: str) -> bool:
        now = time.time()

        if self._has_open_position(symbol):
            logging.warning(f"[BLOCK] already in position | {symbol}")
            return False

        try:
            if hasattr(self.position, "position_count"):
                if self.position.position_count() >= self.runtime_max_positions:
                    return False
        except Exception:
            pass

        if not self._can_trade_today():
            logging.warning(f"[BLOCK] daily trade limit reached | current={self.daily_trade_count} max={self.max_daily_trades}")
            return False

        if now - self._safe_float(self.last_exec_ok_ts.get(symbol), 0.0) < 10:
            return False

        if now - self._safe_float(self.last_failed_entry_ts.get(symbol), 0.0) < self.failed_entry_cooldown_seconds:
            return False

        if now - self._safe_float(self.last_flatten_ts.get(symbol), 0.0) < self.post_close_reentry_cooldown_seconds:
            return False

        return True

    # ================= OPTIONAL CALLBACKS =================
    def on_trade(self, trade: Dict[str, Any]) -> None:
        try:
            symbol = str(trade.get("symbol", "")).upper().strip()
            if symbol:
                self.latest_trade_by_symbol[symbol] = trade
                self.last_trade_ts_by_symbol[symbol] = time.time()
                self._touch()
        except Exception as e:
            logging.error(f"on_trade error: {e}")

    def on_orderbook(self, symbol: str, bids: Any, asks: Any) -> None:
        try:
            symbol = str(symbol).upper().strip()
            if not hasattr(self.ws, "best_bids") or not isinstance(getattr(self.ws, "best_bids"), dict):
                self.ws.best_bids = {}
            if not hasattr(self.ws, "best_asks") or not isinstance(getattr(self.ws, "best_asks"), dict):
                self.ws.best_asks = {}

            best_bid = None
            best_ask = None
            if bids and len(bids) > 0:
                try:
                    best_bid = float(bids[0][0])
                except Exception:
                    pass
            if asks and len(asks) > 0:
                try:
                    best_ask = float(asks[0][0])
                except Exception:
                    pass

            if best_bid is not None:
                self.ws.best_bids[symbol] = best_bid
            if best_ask is not None:
                self.ws.best_asks[symbol] = best_ask
            if best_bid is not None or best_ask is not None:
                self.last_market_ready_ts[symbol] = time.time()
                self._touch()
        except Exception as e:
            logging.error(f"on_orderbook error: {e}")

    # ================= STARTUP FLATTEN =================
    async def startup_flatten_positions(self):
        try:
            positions = self.client.futures_position_information()
            for p in positions:
                symbol = str(p.get("symbol", "")).upper().strip()
                if symbol not in self.symbols:
                    continue

                amt = float(p.get("positionAmt", 0.0))
                if abs(amt) < 1e-8:
                    continue

                position_side = str(p.get("positionSide", "BOTH")).upper().strip()
                side = "SELL" if amt > 0 else "BUY"
                qty = round(abs(amt), 3)
                if qty <= 0:
                    continue

                logging.warning(f"BOOT FLATTEN | {symbol} | qty={qty} | posSide={position_side}")
                if USE_PAPER_TRADING:
                    continue

                try:
                    params = {
                        "symbol": symbol,
                        "side": side,
                        "type": "MARKET",
                        "quantity": qty,
                        "reduceOnly": True,
                    }
                    if position_side in ("LONG", "SHORT"):
                        params["positionSide"] = position_side
                        params.pop("reduceOnly", None)

                    result = self.client.futures_create_order(**params)
                    logging.info(f"BOOT CLOSE OK | {symbol} | {result}")
                except Exception as e:
                    logging.error(f"BOOT CLOSE FAIL {symbol} | {e}")

            await asyncio.sleep(3)
        except Exception as e:
            logging.error(f"Startup flatten error: {e}")

    # ================= HEARTBEAT =================
    async def heartbeat(self):
        while not self.shutdown_requested:
            try:
                self._refresh_balance_safe()
                self._sync_kill_switch_from_pnl()
                self._apply_capital_mode()
                self._reset_daily_trade_counter_if_needed()

                stats = self.pnl.stats()
                kill_status = self.kill.status() if hasattr(self.kill, "status") else {}
                logging.info(
                    f"BALANCE={stats['balance']} | REALIZED={stats['realized_pnl']} | "
                    f"UNREALIZED={stats['unrealized_pnl']} | OPEN={stats['open_positions']} | "
                    f"POS_ENGINE={self.position.position_count()} | MODE={self.runtime_mode.get('mode')} | "
                    f"TRADES_TODAY={self.daily_trade_count}/{self.max_daily_trades} | "
                    f"KILL={kill_status.get('triggered', False)}"
                )
            except Exception as e:
                logging.error(f"Heartbeat error: {e}")
            await asyncio.sleep(10)

    # ================= MARKET DATA ACCESS =================
    def _extract_ws_market(self, symbol: str) -> Dict[str, Any]:
        symbol = str(symbol).upper().strip()

        if hasattr(self.ws, "latest_price") and isinstance(self.ws.latest_price, dict) and symbol in self.ws.latest_price:
            return {
                "symbol": symbol,
                "last_price": self.ws.latest_price.get(symbol),
                "best_bid": getattr(self.ws, "best_bids", {}).get(symbol) if hasattr(self.ws, "best_bids") else None,
                "best_ask": getattr(self.ws, "best_asks", {}).get(symbol) if hasattr(self.ws, "best_asks") else None,
                "spread": None,
                "orderbook": None,
                "trades": None,
                "raw": {"source": "ws.latest_price"},
            }

        if hasattr(self.ws, "latest_prices") and isinstance(self.ws.latest_prices, dict) and symbol in self.ws.latest_prices:
            return {
                "symbol": symbol,
                "last_price": self.ws.latest_prices.get(symbol),
                "best_bid": getattr(self.ws, "best_bids", {}).get(symbol) if hasattr(self.ws, "best_bids") else None,
                "best_ask": getattr(self.ws, "best_asks", {}).get(symbol) if hasattr(self.ws, "best_asks") else None,
                "spread": None,
                "orderbook": None,
                "trades": None,
                "raw": {"source": "ws.latest_prices"},
            }

        if hasattr(self.ws, "prices") and isinstance(self.ws.prices, dict) and symbol in self.ws.prices:
            return {
                "symbol": symbol,
                "last_price": self.ws.prices.get(symbol),
                "best_bid": getattr(self.ws, "best_bids", {}).get(symbol) if hasattr(self.ws, "best_bids") else None,
                "best_ask": getattr(self.ws, "best_asks", {}).get(symbol) if hasattr(self.ws, "best_asks") else None,
                "spread": None,
                "orderbook": None,
                "trades": None,
                "raw": {"source": "ws.prices"},
            }

        market = {
            "symbol": symbol,
            "last_price": None,
            "best_bid": None,
            "best_ask": None,
            "spread": None,
            "orderbook": None,
            "trades": None,
            "raw": None,
        }

        try:
            candidate_map_names = (
                "latest_market_by_symbol",
                "market_data",
                "latest_market",
                "symbol_state",
                "state_by_symbol",
                "latest_by_symbol",
                "books",
                "data",
            )

            raw = None
            for attr in candidate_map_names:
                obj = getattr(self.ws, attr, None)
                if isinstance(obj, dict) and symbol in obj:
                    raw = obj[symbol]
                    break

            if raw is None:
                for getter_name in ("get_market_snapshot", "get_symbol_state", "get_latest", "get_snapshot"):
                    getter = getattr(self.ws, getter_name, None)
                    if callable(getter):
                        try:
                            raw = getter(symbol)
                            if raw is not None:
                                break
                        except Exception:
                            pass

            market["raw"] = raw

            if isinstance(raw, dict):
                market["last_price"] = raw.get("last_price") or raw.get("price") or raw.get("mark_price") or raw.get("close")
                market["best_bid"] = raw.get("best_bid") or raw.get("bid") or raw.get("b")
                market["best_ask"] = raw.get("best_ask") or raw.get("ask") or raw.get("a")
                market["orderbook"] = raw.get("orderbook") or raw.get("book") or raw.get("depth")
                market["trades"] = raw.get("trades") or raw.get("recent_trades")

            if market["best_bid"] is None:
                for attr in ("best_bids", "bids", "top_bids"):
                    obj = getattr(self.ws, attr, None)
                    if isinstance(obj, dict):
                        market["best_bid"] = obj.get(symbol)
                        if market["best_bid"] is not None:
                            break

            if market["best_ask"] is None:
                for attr in ("best_asks", "asks", "top_asks"):
                    obj = getattr(self.ws, attr, None)
                    if isinstance(obj, dict):
                        market["best_ask"] = obj.get(symbol)
                        if market["best_ask"] is not None:
                            break

            if market["last_price"] is None:
                for attr in ("last_prices", "prices", "mark_prices"):
                    obj = getattr(self.ws, attr, None)
                    if isinstance(obj, dict):
                        market["last_price"] = obj.get(symbol)
                        if market["last_price"] is not None:
                            break

            bid = self._safe_float(market["best_bid"], 0.0)
            ask = self._safe_float(market["best_ask"], 0.0)
            if bid > 0 and ask > 0:
                market["spread"] = ask - bid
                self.last_market_ready_ts[symbol] = time.time()
            elif self._safe_float(market.get("last_price"), 0.0) > 0:
                self.last_market_ready_ts[symbol] = time.time()

        except Exception as e:
            logging.error(f"WS market extract failed | {symbol} | {e}")

        return market

    def _market_is_ready(self, market: Dict[str, Any]) -> bool:
        price = self._safe_float(market.get("last_price"), 0.0)
        bid = self._safe_float(market.get("best_bid"), 0.0)
        ask = self._safe_float(market.get("best_ask"), 0.0)
        return price > 0 or (bid > 0 and ask > 0)

    def _market_is_fresh(self, symbol: str) -> bool:
        last_ready = self._safe_float(self.last_market_ready_ts.get(symbol), 0.0)
        if last_ready <= 0:
            return False
        return (time.time() - last_ready) <= self.max_market_staleness_seconds

    def _build_market_context(self, market: Dict[str, Any]) -> Dict[str, Any]:
        bid = self._safe_float(market.get("best_bid"), 0.0)
        ask = self._safe_float(market.get("best_ask"), 0.0)
        last_price = self._safe_float(market.get("last_price"), 0.0)
        mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else last_price
        spread_bps = ((ask - bid) / mid) * 10000.0 if bid > 0 and ask > 0 and mid > 0 else 0.0
        return {
            "bid_price": bid,
            "ask_price": ask,
            "mid_price": mid,
            "last_price": last_price,
            "spread_bps": spread_bps,
            "book_pressure": 0.0,
        }

    # ================= RISK / FILTER HELPERS =================
    async def _check_kill_switch(self, symbol: str, market: Dict[str, Any]) -> bool:
        self._sync_kill_switch_from_pnl()

        result = await self._try_engine_methods(
            self.kill,
            ("check", "allow", "is_safe", "validate"),
            symbol,
            market,
        )

        if result is None:
            return True

        if isinstance(result, bool):
            return result

        if isinstance(result, dict):
            if "triggered" in result:
                return not bool(result["triggered"])
            for key in ("allowed", "ok", "safe", "pass", "passed"):
                if key in result:
                    return bool(result[key])

        return True

    async def _check_spread_filter(self, symbol: str, market: Dict[str, Any]) -> bool:
        result = await self._try_engine_methods(self.spread, ("check", "allow", "validate", "filter"), symbol, market)
        if result is None:
            return True
        if isinstance(result, dict):
            for key in ("allow", "allowed", "ok", "pass", "passed"):
                if key in result:
                    return bool(result[key])
        if isinstance(result, bool):
            return result
        return True

    async def _compute_regime(self, symbol: str, market: Dict[str, Any]) -> Any:
        px = self._safe_float(market.get("last_price") or market.get("price") or market.get("px"), 0.0)
        market_context = self._build_market_context(market)
        regime = await self._call_maybe_async(self.regime.signal, symbol=symbol, price=px, market_context=market_context)
        self.last_regime_by_symbol[symbol] = regime
        return regime

    async def _compute_signal_bundle(self, symbol: str, market: Dict[str, Any]) -> Dict[str, int]:
        micro_res = None
        pressure_res = None
        velocity_res = None
        volatility_res = None
        imbalance_res = None

        try:
            micro_res = await self._call_maybe_async(self.micro.signal, symbol)
        except Exception as e:
            logging.error(f"MicrostructureAlpha.signal failed | {symbol} | {e}")

        try:
            pressure_res = await self._call_maybe_async(self.pressure.signal, symbol=symbol)
        except Exception as e:
            logging.error(f"OrderflowPressureEngine.signal failed | {symbol} | {e}")

        try:
            last_trade = self.latest_trade_by_symbol.get(symbol)
            qty = 0.0
            if isinstance(last_trade, dict):
                qty = self._safe_float(last_trade.get("qty"), 0.0)
            velocity_res = await self._call_maybe_async(self.velocity.signal, symbol, qty)
        except Exception as e:
            logging.error(f"TradeVelocityEngine failed | {symbol} | {e}")

        try:
            volatility_res = await self._try_engine_methods(
                self.volatility, ("generate_signal", "compute", "analyze", "signal"), symbol, market
            )
        except Exception as e:
            logging.error(f"VolatilityEngine failed | {symbol} | {e}")

        try:
            imbalance_res = await self._try_engine_methods(
                self.imbalance, ("generate_signal", "compute", "analyze", "signal"), symbol, market
            )
        except Exception as e:
            logging.error(f"OrderbookImbalanceStrategy failed | {symbol} | {e}")

        vol_value = 0.0
        if isinstance(volatility_res, dict):
            for key in ("value", "volatility", "range_ratio", "atr_ratio", "current_volatility"):
                if key in volatility_res:
                    vol_value = self._safe_float(volatility_res.get(key), 0.0)
                    if vol_value > 0:
                        break
        elif isinstance(volatility_res, (int, float)):
            vol_value = self._safe_float(volatility_res, 0.0)
        self.last_volatility_by_symbol[symbol] = vol_value

        return {
            "micro": self._normalize_signal(micro_res),
            "pressure": self._normalize_signal(pressure_res),
            "velocity": self._normalize_signal(velocity_res),
            "volatility": self._normalize_signal(volatility_res),
            "imbalance": self._normalize_signal(imbalance_res),
        }

    async def _compute_alpha_signal(self, symbol: str, market: Dict[str, Any], signals: Dict[str, int], regime: Any) -> Any:
        last_trade = self.latest_trade_by_symbol.get(symbol, {})
        trade_payload = {
            "qty": self._safe_float(last_trade.get("qty"), 0.0),
            "price": self._safe_float(last_trade.get("price"), 0.0),
            "side": last_trade.get("side"),
            "timestamp": last_trade.get("timestamp"),
        }

        alpha_res = await self._try_engine_methods(
            self.alpha,
            ("generate_signal", "fuse", "compute", "analyze"),
            symbol,
            market,
            extras={
                "signals": signals,
                "regime": regime,
                "micro_signal": signals.get("micro", 0),
                "pressure_signal": signals.get("pressure", 0),
                "velocity_signal": signals.get("velocity", 0),
                "volatility_signal": signals.get("volatility", 0),
                "imbalance_signal": signals.get("imbalance", 0),
                "trade": trade_payload,
                "account_context": {
                     "capital_mode": self.runtime_mode.get("mode"),
                     "drawdown_ratio": self._estimate_drawdown_pct(),
                },
                "execution_quality": self._extract_execution_quality(symbol),
            },
        )

        if alpha_res is None:
            try:
                alpha_res = await self._call_maybe_async(self.alpha.signal, symbol, trade_payload)
            except Exception as e:
                logging.error(f"AlphaFusionEngine.signal failed | {symbol} | {e}")
                alpha_res = None

        return alpha_res

    async def _check_trade_filter(self, symbol: str, market: Dict[str, Any], signal=None, regime=None) -> bool:
        trade_filter_engine = self.filter
        if trade_filter_engine is None:
            return True

        volatility = self.last_volatility_by_symbol.get(symbol)
        price = 0.0
        if isinstance(market, dict):
            price = market.get("last_price") or market.get("price") or market.get("px") or 0.0

        result = await self._try_engine_methods(
            trade_filter_engine,
            ("allow", "filter", "check", "validate"),
            symbol,
            market,
            extras={
                "signal": signal,
                "price": price,
                "trade": market,
                "regime": regime,
                "volatility": volatility,
            },
        )

        if result is None:
            return True
        if isinstance(result, bool):
            return result
        if isinstance(result, dict):
            for key in ("allowed", "ok", "safe", "pass", "passed"):
                if key in result:
                    return bool(result[key])
        return True

    async def _extract_volatility_value(self, symbol: str, market: Dict[str, Any]) -> float:
        vol_value = self._safe_float(self.last_volatility_by_symbol.get(symbol), 0.0)
        if vol_value > 0:
            return vol_value

        try:
            vol_raw = await self._try_engine_methods(self.volatility, ("generate_signal", "compute", "analyze", "signal"), symbol, market)
            if isinstance(vol_raw, dict):
                for key in ("value", "volatility", "range_ratio", "atr_ratio", "current_volatility"):
                    if key in vol_raw:
                        vol_value = self._safe_float(vol_raw.get(key), 0.0)
                        if vol_value > 0:
                            break
            elif isinstance(vol_raw, (int, float)):
                vol_value = self._safe_float(vol_raw, 0.0)
        except Exception:
            pass

        self.last_volatility_by_symbol[symbol] = vol_value
        return vol_value

    async def _compute_order_qty(
        self,
        symbol: str,
        market: Dict[str, Any],
        alpha_signal: int,
        regime: Any,
        bundle_score: Dict[str, Any],
    ) -> float:
        price = self._safe_float(market.get("last_price"), 0.0)
        if price <= 0:
            price = self._safe_float(market.get("best_ask"), 0.0) or self._safe_float(market.get("best_bid"), 0.0)
        if price <= 0:
            logging.warning(f"[SIZING_SKIP] {symbol} | invalid price={price}")
            return 0.0

        vol_value = await self._extract_volatility_value(symbol, market)
        signal_strength = min(1.0, max(0.0, self._safe_float(bundle_score.get("confidence"), 0.0)))
        market_context = self._build_market_context(market)
        execution_quality = self._extract_execution_quality(symbol)

        kill_status = self.kill.status() if hasattr(self.kill, "status") else {}
        drawdown_ratio = self._safe_float(kill_status.get("drawdown", kill_status.get("drawdown_ratio", 0.0)), 0.0)
        equity = self._safe_float(self.pnl.equity() if hasattr(self.pnl, "equity") else self.balance, self.balance)

        account_context = {
            "equity": equity,
            "available_balance": self.balance,
            "free_balance": self.balance,
            "drawdown_ratio": drawdown_ratio,
            "recent_exec_ok_ts": self.last_exec_ok_ts.get(symbol),
            "recent_exec_fail_ts": self.last_failed_entry_ts.get(symbol),
            "capital_mode": self.runtime_mode.get("mode"),
            "risk_per_trade": self.runtime_risk_per_trade,
            "leverage_cap": self.runtime_leverage_cap,
        }

        regime_context = regime if isinstance(regime, dict) else {"value": regime}

        try:
            qty = self.sizing.size(
                symbol=symbol,
                balance=self.balance,
                price=price,
                volatility=vol_value,
                signal_strength=signal_strength,
                regime=regime_context,
                account_context=account_context,
                execution_quality=execution_quality,
                confidence=signal_strength,
                current_position=self.position.get_position(symbol) if hasattr(self.position, "get_position") else None,
                action="OPEN",
                side="BUY" if alpha_signal > 0 else "SELL",
                market_context=market_context,
                signal_meta=bundle_score,
            )
            logging.info(
                f"[SIZING_MAIN] {symbol} | balance={self.balance:.4f} | price={price:.6f} "
                f"| vol={vol_value:.6f} | confidence={signal_strength:.4f} | qty={qty}"
            )
            return round(max(float(qty), 0.0), 8)
        except Exception as e:
            logging.error(f"[SIZING_MAIN_ERROR] {symbol} | {e}")
            return 0.0

    async def _execute_order(
        self,
        symbol: str,
        market: Dict[str, Any],
        alpha_signal: int,
        qty: float,
        bundle_score: Dict[str, Any],
        regime: Any,
        alpha_meta: Optional[Dict[str, Any]] = None,
    ) -> Any:
        now = time.time()
        last_exec = self.last_exec_ok_ts.get(symbol, 0)

        if now - last_exec < 10:
            logging.warning(f"[BLOCK] execution cooldown | {symbol}")
            return {"ok": False, "reason": "execution_cooldown", "status": "FAILED"}

        side = "BUY" if alpha_signal > 0 else "SELL"

        price = self._safe_float(market.get("last_price"), 0.0)
        if price <= 0:
            price = self._safe_float(market.get("best_ask"), 0.0) if side == "BUY" else self._safe_float(market.get("best_bid"), 0.0)

        vol_value = await self._extract_volatility_value(symbol, market)
        market_context = self._build_market_context(market)

        alpha_meta = alpha_meta or {}

        order = self.router.route(
            symbol=symbol,
            side=side,
            qty=qty,
            price=price,
            volatility=vol_value,
            action="OPEN",
            reason="alpha_entry",
            reduce_only=False,
            market_context=market_context,
            extra_meta={
                "alpha_signal": alpha_signal,
                "signal_confidence": self._safe_float(bundle_score.get("confidence"), 0.0),
                "signal_weighted_score": self._safe_float(bundle_score.get("weighted_score"), 0.0),
                "signal_consensus_count": self._safe_int(bundle_score.get("consensus_count"), 0),
                "regime": regime,
                "capital_mode": self.runtime_mode.get("mode"),
                "loop_ts": time.time(),
            },
        )

        if order is None:
            logging.warning(f"[EXEC_BLOCKED] {symbol} | side={side} | qty={qty} | reason=router_blocked")
            return {"ok": False, "reason": "router_blocked", "status": "FAILED"}

        if price > 0:
            order["reference_price"] = price
            order["last_price"] = price
            if "price" not in order:
                order["price"] = price

        # alpha 메타를 executor 기대값 체크용으로 주문 payload에 직접 반영
        order["confidence"] = self._safe_float(alpha_meta.get("confidence", bundle_score.get("confidence", 0.0)), 0.0)
        order["quality"] = self._safe_float(alpha_meta.get("quality", alpha_meta.get("confidence", bundle_score.get("confidence", 0.0))), 0.0)
        order["score"] = self._safe_float(alpha_meta.get("score", abs(bundle_score.get("weighted_score", 0.0))), 0.0)
        order["edge_ratio"] = self._safe_float(alpha_meta.get("adjusted_edge_ratio", alpha_meta.get("edge_ratio", 0.0)), 0.0)
        order["adjusted_edge_ratio"] = self._safe_float(alpha_meta.get("adjusted_edge_ratio", 0.0), 0.0)

        if hasattr(self.slippage, "update_price") and price > 0:
            try:
                self.slippage.update_price(symbol, price)
            except Exception:
                pass

        if hasattr(self.slippage, "check_slippage") and order.get("type") == "LIMIT":
            try:
                if not self.slippage.check_slippage(symbol, order.get("price"), order_type=order.get("type", "LIMIT")):
                    return {"ok": False, "reason": "slippage_blocked", "status": "FAILED"}
            except TypeError:
                if not self.slippage.check_slippage(symbol, order.get("price")):
                    return {"ok": False, "reason": "slippage_blocked", "status": "FAILED"}
            except Exception:
                return {"ok": False, "reason": "slippage_blocked", "status": "FAILED"}

        logging.info(
            f"[EXEC_PREPARED] {symbol} | side={side} | qty={order.get('qty')} | type={order.get('type')} "
            f"| price={order.get('price')} | ref={order.get('reference_price')} | vol={vol_value}"
        )

        result = self.router.retry_split(order, self.executor.execute)
        self.last_order_result_by_symbol[symbol] = result
        self._touch()

        if self._extract_result_ok(result):
            self.last_exec_ok_ts[symbol] = time.time()
            success_count = 1
            if isinstance(result, dict) and result.get("split"):
                success_count = max(1, self._safe_int(result.get("success_count"), 1))
            self._mark_trade_count(success_count)
        else:
            self.last_failed_entry_ts[symbol] = time.time()

        logging.info(f"[EXEC_RESULT] {symbol} | side={side} | qty={order.get('qty')} | result={result}")
        return result

    async def _manage_open_position(self, symbol: str, market: Dict[str, Any]):
        try:
            if not self._has_open_position(symbol):
                return False
            if not self._should_manage_position(symbol):
                return

            price = self._safe_float(market.get("last_price"), 0.0)
            if price <= 0:
                price = self._safe_float(market.get("best_bid"), 0.0) or self._safe_float(market.get("best_ask"), 0.0)
            if price <= 0:
                return

            action = self.position.update(symbol, price)
            if not action or not isinstance(action, dict):
                return

            pos = self.position.get_position(symbol) if hasattr(self.position, "get_position") else None
            if not pos:
                return

            side = pos.get("side")
            close_side = "SELL" if str(side).upper() == "BUY" else "BUY"
            market_context = self._build_market_context(market)
            vol_value = await self._extract_volatility_value(symbol, market)

            if action.get("action") == "CLOSE":
                qty = self.sizing.size(
                    symbol=symbol,
                    balance=self.balance,
                    price=price,
                    volatility=vol_value,
                    signal_strength=1.0,
                    current_position={**pos, "close_qty": pos.get("size", 0.0)},
                    action="CLOSE",
                    side=close_side,
                    market_context=market_context,
                )
                if qty <= 0:
                    return

                order = self.router.route(
                    symbol=symbol,
                    side=close_side,
                    qty=qty,
                    price=price,
                    volatility=vol_value,
                    action="CLOSE",
                    reason=action.get("reason", "position_close"),
                    reduce_only=True,
                    market_context=market_context,
                    urgency="HIGH",
                    extra_meta={"capital_mode": self.runtime_mode.get("mode")},
                )
                if order is None:
                    return

                result = self.router.retry(order, self.executor.execute)
                if self._extract_result_ok(result):
                    fill_qty = self._extract_fill_qty(result, self._safe_float(pos.get("size"), qty))
                    fill_price = self._extract_fill_price(result, price)
                    self.position.close(symbol, reason=action.get("reason", "position_close"))
                    self.pnl.close_position(symbol, fill_price, fee=0.0)
                    pnl_value = 0.0
                    if isinstance(result, dict):
                        pnl_value = self._safe_float(result.get("realized_pnl"), 0.0)
                    if hasattr(self.kill, "update_trade_result"):
                        self.kill.update_trade_result(pnl_value)
                    self.last_flatten_ts[symbol] = time.time()
                    logging.info(f"[POSITION_CLOSE_OK] {symbol} | fill_qty={fill_qty} | fill_price={fill_price}")
                return

            if action.get("action") == "PARTIAL_CLOSE":
                qty = self.sizing.size(
                    symbol=symbol,
                    balance=self.balance,
                    price=price,
                    volatility=vol_value,
                    signal_strength=1.0,
                    current_position={**pos, "close_qty": action.get("size", 0.0)},
                    action="PARTIAL_CLOSE",
                    side=close_side,
                    market_context=market_context,
                )
                if qty <= 0:
                    return

                order = self.router.route(
                    symbol=symbol,
                    side=close_side,
                    qty=qty,
                    price=price,
                    volatility=vol_value,
                    action="PARTIAL_CLOSE",
                    reason=action.get("reason", "partial_close"),
                    reduce_only=True,
                    market_context=market_context,
                    urgency="HIGH",
                    extra_meta={"capital_mode": self.runtime_mode.get("mode")},
                )
                if order is None:
                    return

                result = self.router.retry(order, self.executor.execute)
                if self._extract_result_ok(result):
                    fill_qty = self._extract_fill_qty(result, qty)
                    fill_price = self._extract_fill_price(result, price)
                    if hasattr(self.position, "apply_partial_close"):
                        self.position.apply_partial_close(symbol, fill_qty)
                    self.pnl.partial_close_position(symbol, fill_price, fill_qty, fee=0.0)
                    logging.info(f"[POSITION_PARTIAL_OK] {symbol} | fill_qty={fill_qty} | fill_price={fill_price}")
        except Exception as e:
            logging.error(f"Position management error | {symbol} | {e}")

    def _post_execution_reconcile(self, symbol: str, alpha_signal: int, market: Dict[str, Any], qty: float, exec_res: Any):
        try:
            if not self._extract_result_ok(exec_res):
                return

            if self._has_open_position(symbol):
                return

            entry_price = self._extract_fill_price(exec_res, self._safe_float(market.get("last_price"), 0.0))
            if entry_price <= 0:
                entry_price = self._safe_float(market.get("best_ask"), 0.0) or self._safe_float(market.get("best_bid"), 0.0)
            if entry_price <= 0:
                return

            fill_qty = self._extract_fill_qty(exec_res, qty)
            if fill_qty <= 0:
                fill_qty = qty
            if fill_qty <= 0:
                return

            side = "BUY" if alpha_signal > 0 else "SELL"

            try:
                if hasattr(self.position, "sync_from_exchange_position"):
                    self.position.sync_from_exchange_position(symbol, side, entry_price, fill_qty)
                elif hasattr(self.position, "open_position"):
                    self.position.open_position(symbol, side, entry_price, fill_qty, source="exchange")
            except Exception:
                pass

            try:
                self.pnl.open_position(symbol, side, entry_price, fill_qty, fee=0.0, overwrite=False)
            except TypeError:
                try:
                    self.pnl.open_position(symbol, side, entry_price, fill_qty, fee=0.0)
                except Exception:
                    pass
            except Exception:
                pass

            try:
                self.trade_logger.log({
                    "symbol": symbol,
                    "side": side,
                    "price": entry_price,
                    "qty": fill_qty,
                })
            except Exception:
                pass
        except Exception as e:
            logging.error(f"Post execution reconcile failed | {symbol} | {e}")

    # ================= MAIN TRADING LOOP =================
    async def trading_loop(self):
        logging.info("Trading loop started")

        while not self.shutdown_requested:
            try:
                self.loop_iterations += 1
                self._refresh_balance_safe()
                self._sync_kill_switch_from_pnl()
                self._apply_capital_mode()
                self._reset_daily_trade_counter_if_needed()

                for symbol in self.symbols:
                    log_now = self._should_log_symbol(symbol)

                    market = self._extract_ws_market(symbol)
                    if not self._market_is_ready(market):
                        if log_now:
                            logging.info(f"[TREE] {symbol} | waiting for market data")
                        continue

                    self.last_market_ready_ts[symbol] = time.time()

                    if not self._market_is_fresh(symbol):
                        if log_now:
                            logging.warning(f"[TREE] {symbol} | market stale -> skip")
                        continue

                    px = self._safe_float(market.get("last_price"), 0.0)
                    if px > 0:
                        try:
                            self.pnl.update_price(symbol, px)
                        except Exception:
                            pass

                    if self._has_open_position(symbol):
                        await self._manage_open_position(symbol, market)
                        if log_now:
                            logging.info(f"[TREE] {symbol} | MANAGED | existing position")
                        continue

                    if not self._can_attempt_new_entry(symbol):
                        if log_now:
                            logging.info(f"[TREE] {symbol} | BLOCKED | entry cooldown or max_positions or daily limit")
                        continue

                    if log_now:
                        logging.info(f"[TREE] {symbol} | DATA OK | px={market.get('last_price')}")

                    if not await self._check_kill_switch(symbol, market):
                        logging.warning(f"[TREE] {symbol} | BLOCKED | kill switch")
                        continue

                    if not await self._check_spread_filter(symbol, market):
                        logging.warning(f"[TREE] {symbol} | BLOCKED | spread filter")
                        continue

                    if self._has_open_position(symbol):
                        if log_now:
                            logging.info(f"[TREE] {symbol} | SKIP | existing position")
                        continue

                    regime = await self._compute_regime(symbol, market)
                    if log_now:
                        logging.info(f"[TREE] {symbol} | REGIME | {regime}")

                    signals = await self._compute_signal_bundle(symbol, market)
                    bundle_score = self._score_signal_bundle(signals)
                    if log_now:
                        logging.info(
                            f"[TREE] {symbol} | SIGNALS | micro={signals['micro']} pressure={signals['pressure']} "
                            f"velocity={signals['velocity']} volatility={signals['volatility']} imbalance={signals['imbalance']} "
                            f"| score={bundle_score['weighted_score']:.4f} confidence={bundle_score['confidence']:.4f}"
                        )

                    alpha_meta = await self._compute_alpha_signal(symbol, market, signals, regime)
                    alpha_signal = self._normalize_signal(alpha_meta)
                    if log_now:
                        logging.info(f"[TREE] {symbol} | ALPHA | {alpha_meta}")

                    if alpha_signal == 0:
                        continue

                    if not self._entry_quality_pass(symbol, alpha_signal, bundle_score, regime):
                        continue

                    if self._throttle_signal(symbol):
                        if log_now:
                            logging.info(f"[TREE] {symbol} | BLOCKED | signal throttle")
                        continue

                    filter_signal = alpha_meta if isinstance(alpha_meta, dict) else {
                        "side": "BUY" if alpha_signal > 0 else "SELL",
                        "signal": alpha_signal,
                        "alpha": alpha_signal,
                        "weighted_score": bundle_score.get("weighted_score", 0.0),
                        "confidence": bundle_score.get("confidence", 0.0),
                        "micro": signals.get("micro", 0),
                        "pressure": signals.get("pressure", 0),
                        "velocity": signals.get("velocity", 0),
                        "imbalance": signals.get("imbalance", 0),
                        "regime": regime,
                        "source": "alpha_fallback",
                    }

                    allowed = await self._check_trade_filter(symbol, market, signal=filter_signal, regime=regime)
                    logging.info(f"[DBG 3 FILTER] {symbol} | allowed={allowed}")
                    if not allowed:
                        logging.warning(f"[TREE] {symbol} | BLOCKED | trade filter")
                        continue

                    qty = await self._compute_order_qty(symbol, market, alpha_signal, regime, bundle_score)
                    logging.info(f"[DBG 4 SIZE_FINAL] {symbol} | qty={qty} | px={market.get('last_price')} | balance={self.balance}")
                    if qty <= 0:
                        logging.warning(f"[TREE] {symbol} | BLOCKED | invalid qty={qty}")
                        continue
                    if log_now:
                        logging.info(f"[TREE] {symbol} | SIZE | qty={qty}")

                    logging.info(
                        f"[DBG 5 EXECUTE] {symbol} | side={'BUY' if alpha_signal > 0 else 'SELL'} | "
                        f"qty={qty} | px={market.get('last_price')}"
                    )
                    exec_res = await self._execute_order(symbol, market, alpha_signal, qty, bundle_score, regime, alpha_meta=alpha_meta if isinstance(alpha_meta, dict) else None)
                    self._post_execution_reconcile(symbol, alpha_signal, market, qty, exec_res)

                    logging.info(f"[DBG 6 EXEC_RESULT] {symbol} | exec_res={exec_res}")
                    logging.warning(
                        f"[TREE] ORDER SENT | {symbol} | side={'BUY' if alpha_signal > 0 else 'SELL'} | "
                        f"qty={qty} | px={market.get('last_price')} | result={exec_res}"
                    )

            except Exception as e:
                logging.error(f"[TREE] Trading loop error: {e}")

            await asyncio.sleep(self.loop_sleep_seconds)

    # ================= RUNTIME =================
    def snapshot(self) -> Dict[str, Any]:
        return {
            "symbols": list(self.symbols),
            "balance": self.balance,
            "capital_mode": dict(self.runtime_mode),
            "uptime_seconds": max(0.0, time.time() - self.start_time),
            "loop_iterations": self.loop_iterations,
            "latest_trade_symbols": list(self.latest_trade_by_symbol.keys()),
            "last_signal_ts": dict(self.last_signal_ts),
            "last_market_ready_ts": dict(self.last_market_ready_ts),
            "last_order_result_by_symbol": dict(self.last_order_result_by_symbol),
            "last_failed_entry_ts": dict(self.last_failed_entry_ts),
            "last_flatten_ts": dict(self.last_flatten_ts),
            "daily_trade_count": self.daily_trade_count,
            "max_daily_trades": self.max_daily_trades,
            "shutdown_requested": self.shutdown_requested,
            "updated_at": self.updated_at,
        }

    async def shutdown(self):
        self.shutdown_requested = True
        for name, task in list(self.tasks.items()):
            if task and not task.done():
                task.cancel()
                logging.info(f"Task cancel requested | {name}")
        await asyncio.sleep(0)


async def main():
    boot_log("async main() entered")
    system = TradingSystem()
    boot_log("TradingSystem instance created")

    await system.startup_flatten_positions()
    boot_log("startup_flatten_positions() completed")

    system._apply_capital_mode(force=True)

    boot_log("creating async tasks")
    system.tasks["ws"] = asyncio.create_task(system.ws.start())
    system.tasks["heartbeat"] = asyncio.create_task(system.heartbeat())
    system.tasks["sync"] = asyncio.create_task(system.sync.start())
    system.tasks["trade"] = asyncio.create_task(system.trading_loop())
    boot_log("all async tasks created")

    try:
        await asyncio.gather(*system.tasks.values())
    finally:
        await system.shutdown()


if __name__ == "__main__":
    try:
        boot_log("__main__ entry detected")
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("System Shutdown")