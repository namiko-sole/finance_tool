import os
import time
import tushare as ts
from tqdm import tqdm
from scheduler_utils import is_fresh, update_csv, fetch_with_retry

data_dir = "/root/.openclaw/workspace/data/raw/"

def main():
    pro = ts.pro_api("da8b0417e689bd72a3e5b3312fb6094f9405b06b667f7033eebecf83")

    # 保存交易日历数据到CSV文件
    trade_calendar_path = os.path.join(data_dir, "trade_calendar.csv")
    trade_calendar = update_csv(
        trade_calendar_path,
        lambda: pro.trade_cal(exchange='', start_date='20180101'),
        "交易日历",
        read_existing=True,
    )
    # 保存更新日期, 当前交易日, 上一个交易日, 下一个交易日到一个单独的json文件
    trade_calendar_info_path = os.path.join(data_dir, "trade_calendar_info.json")
    today_str = time.strftime("%Y%m%d")
    trade_calendar['is_open'] = trade_calendar['is_open'].astype(int)
    trade_calendar = trade_calendar[trade_calendar['is_open'] == 1].copy()
    trade_calendar['cal_date'] = trade_calendar['cal_date'].astype(str).str.zfill(8)
    trade_calendar['pretrade_date'] = trade_calendar['pretrade_date'].astype(str).str.zfill(8)
    trade_calendar = trade_calendar.sort_values('cal_date').reset_index(drop=True)

    past_or_today = trade_calendar[trade_calendar['cal_date'] <= today_str]
    future = trade_calendar[trade_calendar['cal_date'] > today_str]

    if past_or_today.empty:
        raise ValueError("交易日历中不存在小于等于今天的日期，请检查交易日历数据是否完整")
    if future.empty:
        raise ValueError("交易日历中不存在大于今天的日期，请检查交易日历数据是否完整")

    current_trade_day = past_or_today.iloc[-1]
    next_trade_day = future.iloc[0]
    trade_calendar_info = {
        "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "current_trade_day": current_trade_day['cal_date'],
        "previous_trade_day": current_trade_day['pretrade_date'],
        "next_trade_day": next_trade_day['cal_date']
    }
    with open(trade_calendar_info_path, 'w', encoding='utf-8') as f:
        import json
        json.dump(trade_calendar_info, f, ensure_ascii=False, indent=4)
    print(f"交易日历信息已保存到 {os.path.basename(trade_calendar_info_path)}")

if __name__ == "__main__":
    main()