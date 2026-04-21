from typing import List
from .base import BaseStrategy, Alert


class PullbackZoneStrategy(BaseStrategy):
    strategy_type: str = "pullback_zone"

    def check(self, ts_code: str, name: str,
              realtime_row: dict,
              ticks: list,
              history_rows,
              strategy_state: dict
             ) -> List[Alert]:

        zone_low = self.params.get("zone_low")
        zone_high = self.params.get("zone_high")
        stop_loss = self.params.get("stop_loss")
        caution_price = self.params.get("caution_price")

        if zone_low is None or zone_high is None:
            return []

        price = realtime_row["price"]

        if not (zone_low <= price <= zone_high):
            return []

        # 构建止损提示行（caution_price和stop_loss都是可选的）
        caution_parts = []
        if caution_price is not None:
            caution_parts.append(f"若跌破{caution_price:.2f}需谨慎")
        if stop_loss is not None:
            caution_parts.append(f"跌破{stop_loss:.2f}止损")
        caution_line = "⚠️ 注意：" + "，".join(caution_parts) if caution_parts else ""

        message = (
            f"🎯 {name}({ts_code}) 回踩到目标区间！\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📊 实时行情\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"现价：{price}元\n"
            f"涨跌幅：{realtime_row.get('pct_chg', 0):+.2f}%\n"
            f"今开：{realtime_row.get('open', price)}元\n"
            f"最高：{realtime_row.get('high', price)}元\n"
            f"最低：{realtime_row.get('low', price)}元\n"
            f"昨收：{realtime_row.get('pre_close', price)}元\n"
            f"\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📍 提醒条件触发\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"目标区间：{zone_low}-{zone_high}元\n"
            f"当前价格：{price}元 ✅ 已进入区间\n"
            f"\n"
            f"⚡ MA20支撑位附近，短线买点区域！\n"
            f"{caution_line}\n"
            f"\n"
            f"时间：{realtime_row.get('time', '')}"
        )

        return [Alert(
            ts_code=ts_code,
            name=name,
            strategy_type=self.strategy_type,
            title=f"🎯 {name}({ts_code}) 回踩到目标区间！",
            message=message,
            level="warning",
        )]
