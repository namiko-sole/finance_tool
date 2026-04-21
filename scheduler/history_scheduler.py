import argparse
import datetime as _dt
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
        force=args.force,
    )

    # 保存股票列表数据到CSV文件
    stock_list_path = os.path.join(data_dir, "stock_list.csv")
    df_stock_list = update_csv(
        stock_list_path,
        lambda: pro.stock_basic(list_status='L', fields=["ts_code", "symbol", "name", "area", "industry", "cnspell", "market", "list_date", "act_name", "act_ent_type", "exchange", "fullname", "enname"]),
        "股票列表",
        read_existing=True,
        force=args.force,
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
        if not args.force and is_fresh(daily_path):
            # print(f"{ts_code} 的日线历史数据文件较新，已跳过更新")
            skipped_count += 1
            continue
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

    # ------------------------------------------------------------------
    # 复权因子: adj_factor
    # 每只股票单独保存到 data/raw/adj_factor/{ts_code}.csv
    # 字段: ts_code, trade_date, adj_factor
    # ------------------------------------------------------------------
    adj_factor_dir = os.path.join(data_dir, "adj_factor")
    os.makedirs(adj_factor_dir, exist_ok=True)
    adj_fetched = 0
    adj_saved = 0
    adj_failed = 0
    adj_skipped = 0

    for _, row in tqdm(df_stock_list.iterrows(), total=len(df_stock_list), desc="更新复权因子"):
        ts_code = row["ts_code"]
        adj_path = os.path.join(adj_factor_dir, f"{ts_code}.csv")
        if not args.force and is_fresh(adj_path):
            adj_skipped += 1
            continue
        df_adj = fetch_with_retry(
            lambda _tc=ts_code: pro.adj_factor(ts_code=_tc, start_date='20220101'),
            f"{ts_code} 复权因子",
        )
        if df_adj is None:
            adj_failed += 1
            continue
        adj_fetched += 1
        try:
            df_adj.to_csv(adj_path, index=False, encoding="utf-8-sig")
            adj_saved += 1
        except Exception as exc:
            adj_failed += 1
            print(f"警告: {ts_code} 复权因子保存失败: {exc}")
        time.sleep(MIN_CALL_INTERVAL_SECONDS)

    print(
        f"复权因子更新统计: 总数 {total_stock_count} 只, 获取到 {adj_fetched} 只, "
        f"保存 {adj_saved} 只, 跳过 {adj_skipped} 只, 失败 {adj_failed} 只"
    )

    # ------------------------------------------------------------------
    # 停复牌数据: suspend_d
    # 按交易日采集, 合并保存到 data/raw/suspend_d.csv
    # 字段: ts_code, trade_date, suspend_timing, suspend_type (S=停牌, R=复牌)
    # ------------------------------------------------------------------
    trade_cal_path = os.path.join(data_dir, "trade_calendar.csv")
    suspend_path = os.path.join(data_dir, "suspend_d.csv")

    # 确定已有的最大日期, 支持增量更新
    existing_suspend_end = None
    if not args.force and is_fresh(suspend_path):
        print(f"停复牌数据文件较新，已跳过更新")
    else:
        if os.path.exists(suspend_path):
            try:
                df_exist = pd.read_csv(suspend_path, dtype=str, encoding="utf-8-sig")
                if not df_exist.empty and "trade_date" in df_exist.columns:
                    existing_suspend_end = df_exist["trade_date"].max()
            except Exception:
                pass

        # 读取交易日历中 20220101 ~ 今天 的所有交易日
        df_cal = pd.read_csv(trade_cal_path, dtype=str, encoding="utf-8-sig")
        df_cal = df_cal[df_cal["is_open"] == "1"]
        cal_dates = sorted(df_cal["cal_date"].tolist())
        cal_dates = [d for d in cal_dates if "20220101" <= d <= today_str]
        # 增量: 只采集已有最大日期之后的
        if existing_suspend_end:
            cal_dates = [d for d in cal_dates if d > existing_suspend_end]

        suspend_all = []
        suspend_failed = 0
        if cal_dates:
            for td in tqdm(cal_dates, desc="更新停复牌数据"):
                df_sd = fetch_with_retry(
                    lambda _td=td: pro.suspend_d(trade_date=_td),
                    f"停复牌 {td}",
                )
                if df_sd is not None and not df_sd.empty:
                    suspend_all.append(df_sd)
                time.sleep(MIN_CALL_INTERVAL_SECONDS)

            if suspend_all:
                df_new = pd.concat(suspend_all, ignore_index=True)
                if os.path.exists(suspend_path):
                    df_exist = pd.read_csv(suspend_path, dtype=str, encoding="utf-8-sig")
                    df_new = pd.concat([df_exist, df_new], ignore_index=True).drop_duplicates(
                        subset=["ts_code", "trade_date"], keep="last"
                    )
                df_new.to_csv(suspend_path, index=False, encoding="utf-8-sig")
                print(f"停复牌数据已保存到 {suspend_path}, 共 {len(df_new)} 条记录")
            else:
                print("停复牌数据: 无新数据")
        else:
            print(f"停复牌数据: 已是最新 (到 {existing_suspend_end})")

    # ------------------------------------------------------------------
    # ST/名称变更数据: namechange
    # 每只股票采集, 合并保存到 data/raw/namechange.csv
    # 字段: ts_code, name, start_date, end_date, ann_date, change_reason
    # ST状态通过 change_reason 判断: ST, *ST, 撤销ST, 摘星 等
    # ------------------------------------------------------------------
    namechange_path = os.path.join(data_dir, "namechange.csv")

    # namechange 数据变化极少, 仅在文件不存在或 force 时采集
    if not args.force and os.path.exists(namechange_path):
        print(f"ST/名称变更数据已存在: {namechange_path}, 跳过采集")
    else:
        nc_all = []
        nc_failed = 0
        for _, row in tqdm(df_stock_list.iterrows(), total=len(df_stock_list), desc="采集ST/名称变更"):
            ts_code = row["ts_code"]
            df_nc = fetch_with_retry(
                lambda _tc=ts_code: pro.namechange(ts_code=_tc),
                f"{ts_code} 名称变更",
            )
            if df_nc is not None and not df_nc.empty:
                nc_all.append(df_nc)
            else:
                nc_failed += 1
            time.sleep(MIN_CALL_INTERVAL_SECONDS)

        if nc_all:
            df_nc_all = pd.concat(nc_all, ignore_index=True).drop_duplicates()
            df_nc_all.to_csv(namechange_path, index=False, encoding="utf-8-sig")
            print(f"ST/名称变更数据已保存到 {namechange_path}, 共 {len(df_nc_all)} 条记录, 失败 {nc_failed} 只")
        else:
            print("ST/名称变更数据: 采集失败或无数据")
    
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
        if not args.force and is_fresh(board_path):
            # print(f"{name} 的板块成分股数据文件较新，已跳过更新")
            board_skipped_count += 1
            continue
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

    # ------------------------------------------------------------------
    # 板块(同花顺行业/概念)日线数据更新
    # 从 stock_board.csv 读取板块列表, 增量更新到 board_daily/
    # 接口: pro.ths_daily(ts_code, start_date, end_date)
    # 字段: ts_code, trade_date, open, high, low, close, pre_close,
    #        avg_price, change, pct_change, vol, turnover_rate
    # ------------------------------------------------------------------
    board_daily_dir = os.path.join(data_dir, "board_daily")
    os.makedirs(board_daily_dir, exist_ok=True)
    bd_fetched = 0
    bd_saved = 0
    bd_skipped = 0
    bd_failed = 0

    for _, row in tqdm(stock_board.iterrows(), total=len(stock_board), desc="更新板块日线数据"):
        ts_code = row["ts_code"]
        board_name = row["name"]
        bd_path = os.path.join(board_daily_dir, f"{ts_code}.csv")

        # 增量: 确定起始日期
        start_dt = "20220101"
        if not args.force and os.path.exists(bd_path):
            try:
                df_exist = pd.read_csv(bd_path, dtype=str)
                if not df_exist.empty and "trade_date" in df_exist.columns:
                    last_date = df_exist["trade_date"].max()
                    last_d = _dt.datetime.strptime(last_date, "%Y%m%d")
                    start_dt = (last_d + _dt.timedelta(days=1)).strftime("%Y%m%d")
                    if start_dt > today_str:
                        bd_skipped += 1
                        continue  # 已是最新
            except Exception:
                start_dt = "20220101"

        df_bd = fetch_with_retry(
            lambda _tc=ts_code, _sd=start_dt: pro.ths_daily(
                ts_code=_tc, start_date=_sd, end_date=today_str
            ),
            f"{board_name}({ts_code}) 板块日线",
        )
        if df_bd is None or df_bd.empty:
            bd_failed += 1
            continue

        bd_fetched += 1
        try:
            # 增量合并
            if os.path.exists(bd_path) and start_dt != "20220101":
                df_old = pd.read_csv(bd_path, dtype=str)
                df_bd = pd.concat([df_old, df_bd], ignore_index=True).drop_duplicates(
                    subset=["trade_date"], keep="last"
                ).sort_values("trade_date")
            df_bd.to_csv(bd_path, index=False, encoding="utf-8-sig")
            bd_saved += 1
        except Exception as exc:
            bd_failed += 1
            print(f"警告: {board_name}({ts_code}) 板块日线保存失败: {exc}")
        time.sleep(MIN_CALL_INTERVAL_SECONDS)

    print(
        f"板块日线更新统计: 总数 {len(stock_board)} 个, 获取到 {bd_fetched} 个, "
        f"保存 {bd_saved} 个, 跳过 {bd_skipped} 个, 失败 {bd_failed} 个"
    )

    # ------------------------------------------------------------------
    # 指数日线数据更新
    # 从 index_list.csv 读取需要更新的指数列表, 增量更新到 stock_daily/
    # ------------------------------------------------------------------
    index_list_path = os.path.join(data_dir, "index_list.csv")
    if os.path.exists(index_list_path):
        df_index_list = pd.read_csv(index_list_path, dtype=str, encoding="utf-8-sig")
        idx_fetched = 0
        idx_saved = 0
        idx_failed = 0

        for _, row in tqdm(df_index_list.iterrows(), total=len(df_index_list), desc="更新指数日线数据"):
            ts_code = row["ts_code"]
            idx_daily_path = os.path.join(data_dir, "stock_daily", f"{ts_code}.csv")
            # 确定增量起始日期
            start_dt = "20200101"
            if not args.force and os.path.exists(idx_daily_path):
                try:
                    df_exist = pd.read_csv(idx_daily_path, dtype=str)
                    if not df_exist.empty and "trade_date" in df_exist.columns:
                        last_date = df_exist["trade_date"].max()
                        # 从最后日期的下一天开始拉
                        last_d = _dt.datetime.strptime(last_date, "%Y%m%d")
                        start_dt = (last_d + _dt.timedelta(days=1)).strftime("%Y%m%d")
                        if start_dt > today_str:
                            continue  # 已是最新
                except Exception:
                    start_dt = "20200101"

            df_idx = fetch_with_retry(
                lambda _tc=ts_code, _sd=start_dt: pro.index_daily(
                    ts_code=_tc, start_date=_sd, end_date=today_str
                ),
                f"{ts_code} 指数日线",
            )
            if df_idx is None or df_idx.empty:
                idx_failed += 1
                continue

            idx_fetched += 1
            try:
                # 增量合并
                if os.path.exists(idx_daily_path) and start_dt != "20200101":
                    df_old = pd.read_csv(idx_daily_path, dtype=str)
                    df_idx = pd.concat([df_old, df_idx], ignore_index=True).drop_duplicates(
                        subset=["trade_date"], keep="last"
                    ).sort_values("trade_date")
                df_idx.to_csv(idx_daily_path, index=False, encoding="utf-8-sig")
                idx_saved += 1
            except Exception as exc:
                idx_failed += 1
                print(f"警告: {ts_code} 指数日线保存失败: {exc}")
            time.sleep(MIN_CALL_INTERVAL_SECONDS)

        print(
            f"指数日线更新统计: 总数 {len(df_index_list)} 个, 获取到 {idx_fetched} 个, "
            f"保存 {idx_saved} 个, 失败 {idx_failed} 个"
        )
    else:
        print("未找到 index_list.csv, 跳过指数日线更新")

    # ------------------------------------------------------------------
    # 每日基本面指标: daily_basic
    # 按交易日增量采集, 按个股保存到 data/raw/daily_basic/{ts_code}.csv
    # 字段: ts_code, trade_date, close, turnover_rate, turnover_rate_f,
    #        volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm,
    #        total_share, float_share, free_share, total_mv, circ_mv
    # 市值单位: 万元
    # ------------------------------------------------------------------
    daily_basic_dir = os.path.join(data_dir, "daily_basic")
    os.makedirs(daily_basic_dir, exist_ok=True)
    daily_basic_meta_path = os.path.join(daily_basic_dir, "_meta.json")
    db_existing_end = None
    if not args.force and os.path.exists(daily_basic_meta_path) and is_fresh(daily_basic_meta_path):
        print("每日基本面指标文件较新，已跳过更新")
    else:
        if os.path.exists(daily_basic_meta_path):
            try:
                with open(daily_basic_meta_path, "r", encoding="utf-8") as f:
                    db_meta = json.load(f)
                db_existing_end = str(db_meta.get("last_trade_date", "")) or None
            except Exception:
                pass

        if not db_existing_end:
            for fn in os.listdir(daily_basic_dir):
                if not fn.endswith(".csv"):
                    continue
                fp = os.path.join(daily_basic_dir, fn)
                try:
                    df_db_exist = pd.read_csv(fp, dtype=str, encoding="utf-8-sig", usecols=["trade_date"])
                    if not df_db_exist.empty and "trade_date" in df_db_exist.columns:
                        local_end = df_db_exist["trade_date"].max()
                        if local_end and (db_existing_end is None or local_end > db_existing_end):
                            db_existing_end = local_end
                except Exception:
                    continue

        # 读取交易日历，确定需要采集的日期范围
        df_cal = pd.read_csv(trade_cal_path, dtype=str, encoding="utf-8-sig")
        df_cal = df_cal[df_cal["is_open"] == "1"]
        cal_dates = sorted(df_cal["cal_date"].tolist())
        cal_dates = [d for d in cal_dates if "20220101" <= d <= today_str]
        if db_existing_end:
            cal_dates = [d for d in cal_dates if d > db_existing_end]

        db_all = []
        db_failed = 0
        if cal_dates:
            for td in tqdm(cal_dates, desc="更新每日基本面指标"):
                df_db = fetch_with_retry(
                    lambda _td=td: pro.daily_basic(trade_date=_td),
                    f"每日基本面 {td}",
                )
                if df_db is not None and not df_db.empty:
                    db_all.append(df_db)
                else:
                    db_failed += 1
                time.sleep(MIN_CALL_INTERVAL_SECONDS)

            if db_all:
                df_db_new = pd.concat(db_all, ignore_index=True)
                if "ts_code" not in df_db_new.columns or "trade_date" not in df_db_new.columns:
                    print("每日基本面指标: 返回数据缺少 ts_code 或 trade_date 字段，未保存")
                else:
                    df_db_new = df_db_new[df_db_new["ts_code"].notna()]
                    db_saved_stocks = 0
                    for ts_code, df_group in tqdm(
                        df_db_new.groupby("ts_code"),
                        total=df_db_new["ts_code"].nunique(),
                        desc="写入每日基本面分文件",
                    ):
                        stock_db_path = os.path.join(daily_basic_dir, f"{ts_code}.csv")
                        df_merged = df_group.copy()
                        if os.path.exists(stock_db_path):
                            try:
                                df_db_exist = pd.read_csv(stock_db_path, dtype=str, encoding="utf-8-sig")
                                df_merged = pd.concat([df_db_exist, df_merged], ignore_index=True).drop_duplicates(
                                    subset=["trade_date"], keep="last"
                                )
                            except Exception:
                                pass
                        if "trade_date" in df_merged.columns:
                            df_merged = df_merged.sort_values("trade_date")
                        df_merged.to_csv(stock_db_path, index=False, encoding="utf-8-sig")
                        db_saved_stocks += 1

                    latest_trade_date = df_db_new["trade_date"].max()
                    try:
                        with open(daily_basic_meta_path, "w", encoding="utf-8") as f:
                            json.dump(
                                {
                                    "last_trade_date": latest_trade_date,
                                    "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                                },
                                f,
                                ensure_ascii=False,
                                indent=2,
                            )
                    except Exception:
                        pass

                    print(
                        f"每日基本面指标已保存到 {daily_basic_dir}, "
                        f"更新 {db_saved_stocks} 只股票, 失败 {db_failed} 天"
                    )
            else:
                print("每日基本面指标: 无新数据")
        else:
            print(f"每日基本面指标: 已是最新 (到 {db_existing_end})")

    # 创建一个映射, 用于个股->板块的快速查询, 并保存到json中, 以便后续使用
    stock_to_boards_path = os.path.join(data_dir, "stock_to_boards.json")
    if not args.force and is_fresh(stock_to_boards_path):
        print("个股->板块映射文件较新，已跳过更新")
    else:
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
    if not args.force and is_fresh(stock_personality_path):
        print("个股股性数据文件较新，已跳过更新")
    else:
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

    # ------------------------------------------------------------------
    # 小卓策略收盘后扫描
    # ------------------------------------------------------------------
    try:
        import sys
        scheduler_dir = os.path.dirname(os.path.abspath(__file__))
        if scheduler_dir not in sys.path:
            sys.path.insert(0, scheduler_dir)
        from xiaozhuo_scanner import run_xiaozhuo_scan
        run_xiaozhuo_scan(current_trade_day)
    except Exception as e:
        print(f"小卓策略扫描异常(不影响主流程): {e}")

    
if __name__ == "__main__":
    main()