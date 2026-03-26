import argparse
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


def get_limit_up_threshold(ts_code: str, stock_name: str) -> float:
    """Return a practical涨停判定阈值(%) by market segment and ST status."""
    # ST状态按当前股票简称近似判断，历史状态变化未逐日回溯。
    if isinstance(stock_name, str) and "ST" in stock_name.upper():
        return 4.8
    if isinstance(ts_code, str) and ts_code.endswith(".BJ"):
        return 29.8
    if isinstance(ts_code, str):
        symbol = ts_code.split(".")[0]
        if symbol.startswith("300") or symbol.startswith("688"):
            return 19.8
    return 9.8


def main():
    parser = argparse.ArgumentParser(description="Update market data.")
    parser.add_argument("--force", action="store_true", help="Force update even on non-trading days.")
    args = parser.parse_args()
    print(f"历史日线调度器于 {time.strftime('%Y-%m-%d %H:%M:%S')} 启动")

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
    if current_trade_day != today_str and not args.force:
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
    total_stock_count = len(df_stock_list)
    fetched_count = 0
    saved_count = 0
    skipped_count = 0
    failed_count = 0

    for _, row in tqdm(df_stock_list.iterrows(), total=len(df_stock_list), desc="更新股票日线历史数据"):
        ts_code = row["ts_code"]
        daily_path = os.path.join(data_dir, "stock_daily", f"{ts_code}.csv")
        # if is_fresh(daily_path):
        #     # print(f"{ts_code} 的日线历史数据文件较新，已跳过更新")
        #     skipped_count += 1
        #     continue
        df_daily = fetch_with_retry(
            lambda: pro.daily(
                ts_code=ts_code,
                start_date='20220101'
            ),
            f"{ts_code} 日线历史数据",
        )
        if df_daily is None:
            failed_count += 1
            print(f"警告: {ts_code} 日线历史数据获取失败，已跳过保存")
            continue

        fetched_count += 1
        try:
            df_daily.to_csv(daily_path, index=False, encoding="utf-8-sig")
            saved_count += 1
        except Exception as exc:
            failed_count += 1
            print(f"警告: {ts_code} 日线历史数据保存失败: {exc}")
        # print(f"{ts_code} 的日线历史数据已保存到 stock_daily/{ts_code}.csv")
        time.sleep(MIN_CALL_INTERVAL_SECONDS)

    print(
        f"股票日线历史更新统计: 总数 {total_stock_count} 只, 获取到 {fetched_count} 只, "
        f"更新保存 {saved_count} 只, 跳过 {skipped_count} 只, 失败 {failed_count} 只"
    )
    
    # 保存所有板块概念的成分股数据到CSV文件
    os.makedirs(os.path.join(data_dir, "board_stocks"), exist_ok=True)
    total_board_count = len(stock_board)
    board_fetched_count = 0
    board_saved_count = 0
    board_skipped_count = 0
    board_failed_count = 0

    for _, row in tqdm(stock_board.iterrows(), total=len(stock_board), desc="更新板块成分股数据"):
        code = row["ts_code"]
        name = row["name"]
        board_path = os.path.join(data_dir, "board_stocks", f"{name}.csv")
        # if is_fresh(board_path):
        #     # print(f"{name} 的板块成分股数据文件较新，已跳过更新")
        #     board_skipped_count += 1
        #     continue
        df_board_stocks = fetch_with_retry(
            lambda: pro.ths_member(ts_code=code),
            f"{name} 板块成分股",
        )
        if df_board_stocks is None:
            board_failed_count += 1
            print(f"警告: {name} 板块成分股数据获取失败，已跳过保存")
            continue

        board_fetched_count += 1
        try:
            df_board_stocks.to_csv(board_path, index=False, encoding="utf-8-sig")
            board_saved_count += 1
        except Exception as exc:
            board_failed_count += 1
            os.makedirs(os.path.dirname(board_path), exist_ok=True)
            try:
                df_board_stocks.to_csv(board_path, index=False, encoding="utf-8-sig")
                board_saved_count += 1
            except Exception as retry_exc:
                board_failed_count += 1
                print(f"警告: {name} 板块成分股数据保存失败: {retry_exc}")
        time.sleep(MIN_CALL_INTERVAL_SECONDS)

    print(
        f"板块成分股更新统计: 总数 {total_board_count} 个, 获取到 {board_fetched_count} 个, "
        f"更新保存 {board_saved_count} 个, 跳过 {board_skipped_count} 个, 失败 {board_failed_count} 个"
    )

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
            stock_code = stock_row["con_code"]
            if stock_code not in stock_to_boards:
                stock_to_boards[stock_code] = set()
            stock_to_boards[stock_code].add(name)
    # Convert sets to sorted lists for stable JSON output.
    stock_to_boards = {k: sorted(v) for k, v in stock_to_boards.items()}
    with open(stock_to_boards_path, 'w', encoding='utf-8') as f:
        json.dump(stock_to_boards, f, ensure_ascii=False, indent=4)
    print(f"个股->板块映射已保存到 {stock_to_boards_path}")

    # 计算股性数据（当前先计算近一年涨停次数）并保存到CSV。
    stock_personality_path = os.path.join(data_dir, "stock_personality.csv")
    calc_trade_day = str(current_trade_day)
    calc_date = pd.to_datetime(calc_trade_day, format="%Y%m%d")
    start_date_1y = calc_date - pd.DateOffset(years=1)
    stock_name_map = df_stock_list.set_index("ts_code")["name"].to_dict()
    personality_rows = []
    personality_failed_count = 0

    for _, row in tqdm(df_stock_list.iterrows(), total=len(df_stock_list), desc="计算个股股性数据"):
        ts_code = row["ts_code"]
        stock_name = row.get("name", "")
        limit_up_threshold = get_limit_up_threshold(ts_code, stock_name)
        daily_path = os.path.join(data_dir, "stock_daily", f"{ts_code}.csv")
        limit_up_count_1y = 0

        if os.path.exists(daily_path):
            try:
                df_daily = pd.read_csv(daily_path)
                if "trade_date" not in df_daily.columns:
                    personality_failed_count += 1
                    print(f"警告: {ts_code} 日线缺少 trade_date 字段，股性计算已跳过")
                    df_1y = None
                else:
                    trade_dates = pd.to_datetime(df_daily["trade_date"], format="%Y%m%d", errors="coerce")
                    valid_mask = trade_dates.notna()
                    if not valid_mask.any():
                        personality_failed_count += 1
                        print(f"警告: {ts_code} trade_date 全部无效，股性计算已跳过")
                        df_1y = None
                    else:
                        df_valid = df_daily[valid_mask].copy()
                        trade_dates_valid = trade_dates[valid_mask]
                        window_mask = (trade_dates_valid >= start_date_1y) & (trade_dates_valid <= calc_date)
                        df_1y = df_valid[window_mask]

                if df_1y is None:
                    pass
                elif "pct_chg" in df_1y.columns:
                    pct_series = pd.to_numeric(df_1y["pct_chg"], errors="coerce")
                    limit_up_count_1y = int((pct_series >= limit_up_threshold).sum())
                elif {"close", "pre_close"}.issubset(df_1y.columns):
                    close_series = pd.to_numeric(df_1y["close"], errors="coerce")
                    pre_close_series = pd.to_numeric(df_1y["pre_close"], errors="coerce")
                    pct_series = (close_series / pre_close_series - 1) * 100
                    limit_up_count_1y = int((pct_series >= limit_up_threshold).sum())
                else:
                    personality_failed_count += 1
                    print(f"警告: {ts_code} 日线缺少 pct_chg 或 close/pre_close 字段，股性计算已跳过")
            except Exception as exc:
                personality_failed_count += 1
                print(f"警告: {ts_code} 股性计算失败: {exc}")
        else:
            personality_failed_count += 1

        personality_rows.append(
            {
                "ts_code": ts_code,
                "name": stock_name_map.get(ts_code, ""),
                "limit_up_count_1y": limit_up_count_1y,
                "calc_date": calc_trade_day,
                "limit_up_threshold": limit_up_threshold,
            }
        )

    df_stock_personality = pd.DataFrame(personality_rows)
    df_stock_personality.to_csv(stock_personality_path, index=False, encoding="utf-8-sig")
    print(
        f"个股股性数据已保存到 {stock_personality_path}，"
        f"共 {len(df_stock_personality)} 只，计算异常或缺失日线 {personality_failed_count} 只"
    )

    
if __name__ == "__main__":
    main()