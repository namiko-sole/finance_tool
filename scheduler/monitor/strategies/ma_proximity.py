from typing import List
import pandas as pd
from .base import BaseStrategy, Alert


class MAProximityStrategy(BaseStrategy):
    """MA回踩策略：检测当前价格是否在指定均线的proximity_pct%范围内"""

    strategy_type: str = "ma_proximity"

    def check(self, ts_code: str, name: str,
              realtime_row: dict,
              ticks: list,
              history_rows,
              strategy_state: dict
             ) -> List[Alert]:

        # --- 防御：history_rows 可能为 None 或数据不足 ---
        if history_rows is None or not isinstance(history_rows, pd.DataFrame):
            return []
        if len(history_rows) < 5:
            return []

        # --- 参数读取 ---
        mas = self.params.get("mas", [5, 10, 20])
        proximity_pct = self.params.get("proximity_pct", 1.5)

        # --- 当前价格 ---
        current_price = realtime_row.get("price")
        if current_price is None or current_price <= 0:
            return []

        # --- 计算各MA值 ---
        # history_rows 的收盘价列名为 close，按日期升序排列
        close_series = pd.to_numeric(history_rows["close"], errors="coerce")
        if close_series.isna().all():
            return []

        ma_values = {}
        for period in mas:
            if len(close_series) >= period:
                ma_val = close_series.rolling(window=period).mean().iloc[-1]
                if pd.notna(ma_val):
                    ma_values[period] = ma_val

        if not ma_values:
            return []

        # --- 检测价格是否在某条MA的proximity_pct%范围内 ---
        triggered_ma = None
        triggered_ma_val = None
        min_distance_pct = float("inf")

        for period, ma_val in ma_values.items():
            distance_pct = abs(current_price - ma_val) / ma_val * 100
            if distance_pct <= proximity_pct and distance_pct < min_distance_pct:
                min_distance_pct = distance_pct
                triggered_ma = period
                triggered_ma_val = ma_val

        if triggered_ma is None:
            return []

        # --- 方向判断：用 ticks 序列判断趋势 ---
        direction = self._judge_direction(current_price, triggered_ma_val, ticks)

        # 构建方向描述
        if direction == "pullback":
            direction_desc = "从上方回踩MA（接近中）"
            direction_emoji = "📉"
        elif direction == "bounce":
            direction_desc = "从下方反弹到MA"
            direction_emoji = "📈"
        else:
            direction_desc = "价格贴近MA"
            direction_emoji = "〰️"

        # --- 计算辅助信息 ---
        pct_chg = realtime_row.get("pct_chg", 0)
        distance_pct = (current_price - triggered_ma_val) / triggered_ma_val * 100

        # 汇总所有MA值用于展示
        ma_lines = []
        for period in sorted(ma_values.keys()):
            val = ma_values[period]
            marker = " ◀" if period == triggered_ma else ""
            ma_lines.append(f"MA{period}：{val:.2f}元{marker}")

        ma_text = "\n".join(ma_lines)

        message = (
            f"{direction_emoji} {name}({ts_code}) MA{triggered_ma}回踩信号！\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📊 实时行情\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"现价：{current_price:.2f}元\n"
            f"涨跌幅：{pct_chg:+.2f}%\n"
            f"今开：{realtime_row.get('open', current_price):.2f}元\n"
            f"最高：{realtime_row.get('high', current_price):.2f}元\n"
            f"最低：{realtime_row.get('low', current_price):.2f}元\n"
            f"昨收：{realtime_row.get('pre_close', current_price):.2f}元\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📐 MA回踩分析\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"触发均线：MA{triggered_ma} = {triggered_ma_val:.2f}元\n"
            f"偏离幅度：{distance_pct:+.2f}%\n"
            f"方向判断：{direction_desc}\n"
            f"检测阈值：±{proximity_pct}%\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📋 均线一览\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"{ma_text}\n"
            f"\n"
            f"⚡ 关注MA{triggered_ma}支撑/压力位！\n"
            f"时间：{realtime_row.get('time', '')}"
        )

        return [Alert(
            ts_code=ts_code,
            name=name,
            strategy_type=self.strategy_type,
            title=f"{direction_emoji} {name}({ts_code}) MA{triggered_ma}回踩信号！",
            message=message,
            level="warning",
        )]

    def _judge_direction(self, current_price: float, ma_val: float, ticks: list) -> str:
        """
        判断价格相对MA的运动方向。

        - pullback: 价格从上方回踩MA（看跌趋势，价格在MA上方但正在靠近）
        - bounce: 价格从下方反弹到MA（看涨趋势，价格在MA下方但正在靠近）
        - neutral: 无法判断
        """
        if not ticks or len(ticks) < 3:
            # ticks不足，用当前价格与MA的位置关系判断
            if current_price >= ma_val:
                return "pullback"
            else:
                return "bounce"

        # 取最近5个tick的价格
        recent_ticks = ticks[-5:]
        recent_prices = [t.get("price", 0) for t in recent_ticks]

        # 过滤无效价格
        recent_prices = [p for p in recent_prices if p > 0]
        if len(recent_prices) < 2:
            if current_price >= ma_val:
                return "pullback"
            else:
                return "bounce"

        # 判断tick趋势方向：最近价格在MA上方→回踩，在MA下方→反弹
        # 同时结合价格趋势（上升/下降）
        avg_price = sum(recent_prices) / len(recent_prices)
        first_tick_price = recent_prices[0]
        last_tick_price = recent_prices[-1]
        trending_up = last_tick_price > first_tick_price

        # 判断最近tick价格主要在MA的哪一侧
        above_count = sum(1 for p in recent_prices if p >= ma_val)
        below_count = len(recent_prices) - above_count

        if above_count > below_count:
            # 价格主要在MA上方 → 回踩
            return "pullback"
        elif below_count > above_count:
            # 价格主要在MA下方 → 反弹
            return "bounce"
        else:
            # 持平时看趋势方向
            if trending_up:
                return "bounce"  # 价格在上升，从下方靠近
            else:
                return "pullback"  # 价格在下降，从上方靠近
