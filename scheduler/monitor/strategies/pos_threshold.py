"""
POS位置阈值监控策略 — 监控20日区间位置降到指定阈值以下时触发告警。
用于策略A放宽条件候选股的跟踪，等待pos降到标准线时提醒入场。

参数:
  threshold: float, pos_20d 阈值（如 0.5），低于此值触发告警
  window: int, 计算区间天数，默认20
"""

import numpy as np
from typing import List
from .base import BaseStrategy, Alert


class PosThresholdStrategy(BaseStrategy):
    strategy_type: str = "pos_threshold"

    def __init__(self, params: dict):
        super().__init__(params)
        self.threshold = float(params.get("threshold", 0.5))
        self.window = int(params.get("window", 20))

    def check(self, ts_code, name, realtime_row, ticks, history_rows, strategy_state) -> List[Alert]:
        if history_rows is None or len(history_rows) < self.window:
            return []

        closes = history_rows["close"].values.astype(float)

        # 用实时价格替换最后一根K线的收盘价（盘中实时）
        current_price = realtime_row["price"]
        if current_price > 0:
            closes[-1] = current_price

        # 取最近 window 天
        window_closes = closes[-self.window:]
        high_w = float(np.max(window_closes))
        low_w = float(np.min(window_closes))

        if high_w <= low_w:
            pos = 0.5
        else:
            pos = (current_price - low_w) / (high_w - low_w)

        if pos < self.threshold:
            # 计算MA10
            ma10 = float(np.mean(closes[-10:])) if len(closes) >= 10 else 0
            ma10_dev = (current_price - ma10) / ma10 * 100 if ma10 > 0 else 0

            # 检查底分型（最近3天日线）
            lows = history_rows["low"].values.astype(float)
            opens = history_rows["open"].values.astype(float)
            n = len(lows)
            has_bottom = False
            if n >= 3:
                # 用历史日线的倒数第3、2、1根判断底分型
                # （倒数第1根是今天的日线close，但盘中可能还没确定）
                if lows[-2] < lows[-3] and lows[-2] < lows[-1]:
                    has_bottom = True

            # 检查当天是否阳线
            open_today = realtime_row.get("open", 0)
            is_yang = current_price > open_today if open_today > 0 else False

            bottom_str = "✅ 底分型确认" if has_bottom else "⬜ 底分型未确认"
            yang_str = "✅ 阳线" if is_yang else "⬜ 非阳线"

            msg = (
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 策略A位置触发！\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 {ts_code} {name}\n"
                f"📊 当前价: {current_price:.2f}\n"
                f"📏 pos_20d: {pos:.2f} < {self.threshold} ✅\n"
                f"📐 20日区间: {low_w:.2f} ~ {high_w:.2f}\n"
                f"📈 MA10偏离: {ma10_dev:+.2f}%\n"
                f"🔢 {bottom_str} | {yang_str}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━"
            )

            return [Alert(
                ts_code=ts_code,
                name=name,
                strategy_type=self.strategy_type,
                title=f"🎯 {name} pos降到{pos:.2f}！触发标准A入场信号",
                message=msg,
                level="critical",
            )]

        # 未触发，记录pos用于日志
        logger_key = "_last_pos"
        strategy_state[logger_key] = round(pos, 4)
        return []
