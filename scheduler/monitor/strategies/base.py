from dataclasses import dataclass
from typing import List, Optional, Dict, Any


@dataclass
class Alert:
    ts_code: str
    name: str
    strategy_type: str
    title: str          # 短标题，如 "🚨 中利集团 止跌企稳信号！"
    message: str        # 完整通知文本
    level: str = "info" # info/warning/critical


class BaseStrategy:
    strategy_type: str = "base"

    def __init__(self, params: dict):
        self.params = params

    def check(self, ts_code: str, name: str,
              realtime_row: dict,   # 本次快照（从DataFrame提取的一行dict）
              ticks: list,          # 日内tick序列（今天攒的所有快照点dict列表）
              history_rows,         # pandas DataFrame，最近20-30天日线
              strategy_state: dict  # 该策略的持久状态（如 {"notified": false}）
             ) -> List[Alert]:
        raise NotImplementedError
