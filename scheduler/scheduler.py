import os
import time
import json
import pandas as pd
import tushare as ts
from tqdm import tqdm
from scheduler_utils import is_fresh, update_csv, fetch_with_retry

data_dir = "/root/.openclaw/workspace/data/raw/"

# Tushare Pro per-minute limit: 800 calls/min (~0.075s per call).
MIN_CALL_INTERVAL_SECONDS = 0.1


def main():
    # 查看今日是否是交易日，如果不是交易日则不更新数据
    trade_calendar_info_path = "/root/.openclaw/workspace/data/raw/trade_calendar_info.json"
    if not os.path.exists(trade_calendar_info_path):
        raise FileNotFoundError(f"交易日历信息文件不存在: {trade_calendar_info_path}")
    with open(trade_calendar_info_path, 'r', encoding='utf-8') as f:
        trade_calendar_info = json.load(f)
    current_trade_day = trade_calendar_info.get("current_trade_day")
    if not current_trade_day:
        raise ValueError("交易日历信息中缺少 current_trade_day 字段")
    current_trade_day = str(current_trade_day)
    today_str = time.strftime("%Y%m%d")
    if current_trade_day != today_str:
        print(f"今天 {today_str} 不是交易日，已跳过数据更新")
        return

    pro = ts.pro_api("da8b0417e689bd72a3e5b3312fb6094f9405b06b667f7033eebecf83")

    # 保存板块列表数据到CSV文件
    stock_board_path = os.path.join(data_dir, "stock_board.csv")
    stock_board = update_csv(
        stock_board_path,
        lambda: pro.ths_index(),
        "板块列表",
        read_existing=True,
    )

    # 保存股票列表数据到CSV文件
    stock_list_path = os.path.join(data_dir, "stock_list.csv")
    df_stock_list = update_csv(
        stock_list_path,
        lambda: pro.stock_basic(list_status='L', fields=["ts_code", "symbol", "name", "area", "industry", "cnspell", "market", "list_date", "act_name", "act_ent_type", "exchange", "fullname", "enname"]),
        "股票列表",
        read_existing=True,
    )

    # 保存所有股票的日线历史数据到CSV文件
    os.makedirs(os.path.join(data_dir, "stock_daily"), exist_ok=True)
    for _, row in tqdm(df_stock_list.iterrows(), total=len(df_stock_list), desc="更新股票日线历史数据"):
        ts_code = row["ts_code"]
        daily_path = os.path.join(data_dir, "stock_daily", f"{ts_code}.csv")
        if is_fresh(daily_path):
            # print(f"{ts_code} 的日线历史数据文件较新，已跳过更新")
            continue
        df_daily = fetch_with_retry(
            lambda: pro.daily(
                ts_code=ts_code,
                start_date='20220101'
            ),
            f"{ts_code} 日线历史数据",
        )
        df_daily.to_csv(daily_path, index=False, encoding="utf-8-sig")
        # print(f"{ts_code} 的日线历史数据已保存到 stock_daily/{ts_code}.csv")
        time.sleep(MIN_CALL_INTERVAL_SECONDS)
    
    # 保存所有板块概念的成分股数据到CSV文件
    os.makedirs(os.path.join(data_dir, "board_stocks"), exist_ok=True)
    for _, row in tqdm(stock_board.iterrows(), total=len(stock_board), desc="更新板块成分股数据"):
        code = row["ts_code"]
        name = row["name"]
        board_path = os.path.join(data_dir, "board_stocks", f"{name}.csv")
        if is_fresh(board_path):
            # print(f"{name} 的板块成分股数据文件较新，已跳过更新")
            continue
        df_board_stocks = fetch_with_retry(
            lambda: pro.ths_member(ts_code=code),
            f"{name} 板块成分股",
        )
        try:
            df_board_stocks.to_csv(board_path, index=False, encoding="utf-8-sig")
        except Exception as exc:
            os.makedirs(os.path.dirname(board_path), exist_ok=True)
            df_board_stocks.to_csv(board_path, index=False, encoding="utf-8-sig")
        time.sleep(MIN_CALL_INTERVAL_SECONDS)

    # 创建一个映射, 用于个股->板块的快速查询, 并保存到json中, 以便后续使用
    stock_to_boards_path = os.path.join(data_dir, "stock_to_boards.json")
    stock_to_boards = {}
    for _, row in stock_board.iterrows():
        code = row["ts_code"]
        name = row["name"]
        board_path = os.path.join(data_dir, "board_stocks", f"{name}.csv")
        if not os.path.exists(board_path):
            print(f"警告: 板块成分股数据文件不存在: {board_path}")
            continue
        df_board_stocks = pd.read_csv(board_path)
        for _, stock_row in df_board_stocks.iterrows():
            stock_code = stock_row["ts_code"]
            if stock_code not in stock_to_boards:
                stock_to_boards[stock_code] = []
            stock_to_boards[stock_code].append(name)
    with open(stock_to_boards_path, 'w', encoding='utf-8') as f:
        json.dump(stock_to_boards, f, ensure_ascii=False, indent=4)
    print(f"个股->板块映射已保存到 {stock_to_boards_path}")

    
if __name__ == "__main__":
    main()