from typing import List
from .base import BaseStrategy, Alert


class StabilizationStrategy(BaseStrategy):
    strategy_type: str = "stabilization"

    def check(self, ts_code: str, name: str,
              realtime_row: dict,
              ticks: list,
              history_rows,
              strategy_state: dict
             ) -> List[Alert]:

        min_ticks = 5
        if len(ticks) < min_ticks:
            return []

        # 可配置参数
        min_decline_pct = self.params.get("min_decline_pct", 0.5)
        recent_points = self.params.get("recent_points", 3)
        max_range_pct = self.params.get("max_range_pct", 0.5)

        # 取最近10个tick（与原逻辑一致）
        recent = ticks[-10:]
        price_list = [t["price"] for t in recent]

        # 基准价：第一个tick的昨收价
        first_price = ticks[0].get("pre_close", ticks[0]["price"])
        current_price = price_list[-1]
        min_price = min(price_list)

        decline_pct = (first_price - min_price) / first_price * 100

        last_n = price_list[-recent_points:]
        recent_range = (max(last_n) - min(last_n)) / min(last_n) * 100
        last_n_avg = sum(last_n) / len(last_n)

        has_declined = decline_pct > min_decline_pct
        stabilized = recent_range < max_range_pct
        no_new_low = current_price >= min(price_list[-recent_points:])
        bouncing = last_n_avg > min_price * 1.001
        rising_n = (len(last_n) >= 3
                    and last_n[1] >= last_n[0]
                    and last_n[2] >= last_n[1])

        triggered = False
        signal_description = ""

        if has_declined and (stabilized or rising_n) and no_new_low:
            triggered = True
            signal_description = (
                f"止跌企稳信号：跌幅{decline_pct:.2f}%，"
                f"近{recent_points}点波动{recent_range:.3f}%，"
                f"当前{current_price}元"
            )

        if has_declined and bouncing and stabilized:
            triggered = True
            signal_description = (
                f"反弹企稳信号：跌幅{decline_pct:.2f}%，"
                f"近{recent_points}点波动{recent_range:.3f}%，"
                f"当前{current_price}元"
            )

        if not triggered:
            return []

        # 计算反弹幅度
        bounce_pct = ((current_price - min_price) / min_price) * 100

        message = (
            f"🚨 {name}({ts_code}) 止跌企稳信号！\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📊 实时行情\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"现价：{current_price}元\n"
            f"涨跌幅：{realtime_row.get('pct_chg', 0):+.2f}%\n"
            f"今开：{realtime_row.get('open', current_price)}元\n"
            f"最高：{realtime_row.get('high', current_price)}元\n"
            f"最低：{realtime_row.get('low', current_price)}元\n"
            f"昨收：{realtime_row.get('pre_close', current_price)}元\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📈 企稳信号分析\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"日内最低：{min_price}元\n"
            f"距最低反弹：{bounce_pct:+.2f}%\n"
            f"{signal_description}\n"
            f"检测时间：{realtime_row.get('time', '')}\n"
            f"\n"
            f"⚡ 建议关注是否可以介入！"
        )

        return [Alert(
            ts_code=ts_code,
            name=name,
            strategy_type=self.strategy_type,
            title=f"🚨 {name}({ts_code}) 止跌企稳信号！",
            message=message,
            level="critical",
        )]
