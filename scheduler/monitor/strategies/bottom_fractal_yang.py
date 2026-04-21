"""
底分型阳线确认监控策略 — 监控K线出现底分型+阳线时触发告警。
用于策略A候选股在pos到位后，等待形态确认信号。

底分型定义：连续3根K线，中间那根的最低价 < 前后两根的最低价。
阳线定义：当天收盘价 > 开盘价。

参数:
  min_body_pct: float, 阳线实体占价格的比例阈值（如 0.5%），过滤十字星，默认0
"""

import numpy as np
from typing import List
from .base import BaseStrategy, Alert


class BottomFractalYangStrategy(BaseStrategy):
    strategy_type: str = "bottom_fractal_yang"

    def __init__(self, params: dict):
        super().__init__(params)
        self.min_body_pct = float(params.get("min_body_pct", 0))

    def check(self, ts_code, name, realtime_row, ticks, history_rows, strategy_state) -> List[Alert]:
        if history_rows is None or len(history_rows) < 3:
            return []

        lows = history_rows["low"].values.astype(float)
        n = len(lows)

        # 检查底分型：倒数第2根K线的low < 倒数第3根和倒数第1根的low
        # 倒数第1根是今天（盘中可能还在变化），用实时价更新
        has_bottom = False
        if n >= 3:
            if lows[-2] < lows[-3] and lows[-2] < lows[-1]:
                has_bottom = True

        # 检查当天是否阳线（用实时价格判断盘中状态）
        current_price = realtime_row["price"]
        open_today = realtime_row.get("open", 0)

        is_yang = False
        if open_today > 0 and current_price > open_today:
            # 检查实体是否满足最小比例
            body_pct = (current_price - open_today) / open_today * 100
            if body_pct >= self.min_body_pct:
                is_yang = True

        # 两个条件同时满足才触发
        if has_bottom and is_yang:
            # 额外信息：涨跌幅
            pct_chg = realtime_row.get("pct_chg", 0)

            msg = (
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔥 策略A底分型阳线确认！\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 {ts_code} {name}\n"
                f"📊 当前价: {current_price:.2f} ({pct_chg:+.2f}%)\n"
                f"✅ 底分型确认 — 3根K线中间最低\n"
                f"✅ 阳线确认 — 收({current_price:.2f}) > 开({open_today:.2f})\n"
                f"⚡ 策略A入场形态已确认！\n"
                f"━━━━━━━━━━━━━━━━━━━━━━"
            )

            return [Alert(
                ts_code=ts_code,
                name=name,
                strategy_type=self.strategy_type,
                title=f"🔥 {name} 底分型+阳线确认！策略A入场信号",
                message=msg,
                level="critical",
            )]

        # 未触发，记录状态用于日志
        bottom_str = "✅" if has_bottom else "⬜"
        yang_str = "✅" if is_yang else "⬜"
        strategy_state["_bottom_fractal"] = bottom_str
        strategy_state["_yang_line"] = yang_str
        strategy_state["_last_price"] = round(current_price, 2)
        return []
