"""个股监控引擎 - 核心入口"""

import os
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

import pandas as pd

from .config import load_config, WatchConfig, WatchEntry, StrategyConfig, NotifyChannel
from .state import load as state_load, save as state_save, append_tick
from .notify import send_alert
from .strategies.base import BaseStrategy, Alert

# 日志：print + 写文件
LOG_FILE = os.path.join(os.path.dirname(__file__), "monitor.log")

logger = logging.getLogger("monitor")
logger.setLevel(logging.INFO)

if not logger.handlers:
    _fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    _fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(_fh)

    _ch = logging.StreamHandler()
    _ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(_ch)


class MonitorEngine:
    def __init__(self, config_path: Optional[str] = None):
        """初始化监控引擎，默认读取同目录下 watches.yaml"""
        self.config: WatchConfig = load_config(config_path)
        self.strategy_registry: Dict[str, type] = {}
        self._register_strategies()

    def _register_strategies(self):
        """注册策略，用 try import 防止策略文件不存在时报错"""
        # stabilization
        try:
            from .strategies.stabilization import StabilizationStrategy
            self.strategy_registry["stabilization"] = StabilizationStrategy
        except ImportError:
            pass

        # pullback_zone
        try:
            from .strategies.pullback_zone import PullbackZoneStrategy
            self.strategy_registry["pullback_zone"] = PullbackZoneStrategy
        except ImportError:
            pass

        # ma_proximity
        try:
            from .strategies.ma_proximity import MAProximityStrategy
            self.strategy_registry["ma_proximity"] = MAProximityStrategy
        except ImportError:
            pass

        # pos_threshold
        try:
            from .strategies.pos_threshold import PosThresholdStrategy
            self.strategy_registry["pos_threshold"] = PosThresholdStrategy
        except ImportError:
            pass

        # bottom_fractal_yang
        try:
            from .strategies.bottom_fractal_yang import BottomFractalYangStrategy
            self.strategy_registry["bottom_fractal_yang"] = BottomFractalYangStrategy
        except ImportError:
            pass

    def run(self, df: pd.DataFrame):
        """
        主入口。realtime_scheduler拉完全市场DataFrame后调用。

        流程：
        1. 获取今天日期
        2. 遍历 config.watches
        3. 从df中filter出该股的行（df["ts_code"] == ts_code）
        4. 提取 realtime_row dict
        5. StateManager.load 加载状态（自动处理每日重置）
        6. 追加tick到state
        7. 从CSV读历史日线（/root/.openclaw/workspace/data/raw/stock_daily/{ts_code}.csv）
           - 只读最近30行
           - 如果文件不存在，跳过（log warning）
        8. 遍历该股的strategies，逐个check
        9. 有Alert → notify（检查strategy_state["notified"]避免重复通知）
        10. 标记notified = True
        11. StateManager.save
        """
        today_str = datetime.now().strftime("%Y%m%d")
        logger.info(f"MonitorEngine run: 检查 {len(self.config.watches)} 只监控股")

        for watch in self.config.watches:
            ts_code = watch.ts_code
            name = watch.name

            # 3. 从 df 中 filter 出该股
            matched = df[df["ts_code"] == ts_code]
            if matched.empty:
                logger.debug(f"{ts_code} {name} 在本次快照中未找到，跳过")
                continue

            # 4. 提取 realtime_row
            row = matched.iloc[0]
            realtime_row = self._extract_row(row)

            # 5. 加载状态
            state = state_load(ts_code, today_str)

            # 6. 追加 tick
            append_tick(state, realtime_row)

            # 7. 读历史日线
            history_rows = self._load_history(ts_code)

            # 8. 遍历策略
            for strat_cfg in watch.strategies:
                strat_type = strat_cfg.type
                if strat_type not in self.strategy_registry:
                    logger.warning(f"{ts_code} 策略 {strat_type} 未注册，跳过")
                    continue

                # 获取或初始化 strategy_state
                if strat_type not in state.get("strategy_states", {}):
                    state.setdefault("strategy_states", {})[strat_type] = {"notified": False}
                strategy_state = state["strategy_states"][strat_type]

                # 如果已经通知过，跳过
                if strategy_state.get("notified", False):
                    continue

                # 实例化策略并检查
                try:
                    strategy_cls = self.strategy_registry[strat_type]
                    strategy: BaseStrategy = strategy_cls(strat_cfg.params)
                    alerts = strategy.check(
                        ts_code=ts_code,
                        name=name,
                        realtime_row=realtime_row,
                        ticks=state.get("ticks", []),
                        history_rows=history_rows,
                        strategy_state=strategy_state,
                    )
                except Exception as e:
                    logger.error(f"{ts_code} 策略 {strat_type} 执行异常: {e}", exc_info=True)
                    continue

                # 9. 有 Alert → notify
                if alerts:
                    for alert in alerts:
                        logger.info(f"告警触发: {alert.title}")
                        # 发送通知
                        channels = self._get_notify_channels(strat_cfg.notify)
                        for ch in channels:
                            try:
                                send_alert(alert.message, ch)
                            except Exception as e:
                                logger.error(f"通知发送失败 ({ch}): {e}")

                    # 10. 标记 notified
                    strategy_state["notified"] = True

            # 11. 保存状态
            state_save(state)

        logger.info("MonitorEngine run: 完成")

    def _extract_row(self, row) -> dict:
        """从DataFrame的一行提取成dict，统一key名"""
        return {
            "time": datetime.now().strftime("%H:%M:%S"),
            "ts_code": str(row.get("ts_code", "")),
            "price": float(row.get("close", row.get("price", 0))),
            "high": float(row.get("high", 0)),
            "low": float(row.get("low", 0)),
            "open": float(row.get("open", 0)),
            "pre_close": float(row.get("pre_close", 0)),
            "vol": float(row.get("vol", 0)),
            "amount": float(row.get("amount", 0)),
            "pct_chg": float(row.get("pct_chg", 0)),
        }

    def _load_history(self, ts_code: str) -> Optional[pd.DataFrame]:
        """从CSV读最近30天日线（CSV按日期降序，取head再升序排序供rolling使用）"""
        csv_path = f"/root/.openclaw/workspace/data/raw/stock_daily/{ts_code}.csv"
        if not os.path.exists(csv_path):
            logger.warning(f"历史日线不存在: {csv_path}")
            return None
        try:
            df = pd.read_csv(csv_path, encoding="utf-8-sig")
            # CSV按日期降序，head(30)取最新30条，再按日期升序排列供rolling计算
            recent = df.head(60).sort_values("trade_date").reset_index(drop=True)
            return recent
        except Exception as e:
            logger.warning(f"读取历史日线失败 {csv_path}: {e}")
            return None

    def _get_notify_channels(self, channel_names: List[str]) -> List[NotifyChannel]:
        """根据名字列表从config中找channel配置"""
        channels = []
        for name in channel_names:
            ch = self.config.notify_channels.get(name)
            if ch:
                channels.append(ch)
            else:
                logger.warning(f"通知渠道未配置: {name}")
        return channels
