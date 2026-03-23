#!/usr/bin/env python3
"""
本地数据获取器 - 从 /root/.openclaw/workspace/data/raw 统一读取各类数据
"""
import os
import sys
import json
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

DATA_DIR = Path("/root/.openclaw/workspace/data/raw")
STOCK_DAILY_DIR = DATA_DIR / "stock_daily"
BOARD_DIR = DATA_DIR / "board_stocks"


def get_stock_daily(ts_code: str, days: int = 252) -> pd.DataFrame:
    """获取股票日线，默认最近一年"""
    path = STOCK_DAILY_DIR / f"{ts_code}.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path).tail(days)
    return df.sort_values("trade_date", ascending=False).reset_index(drop=True)


def get_stock_daily_all(days: int = 252) -> dict:
    """获取所有股票日线（返回字典，key为ts_code）"""
    result = {}
    for f in STOCK_DAILY_DIR.glob("*.csv"):
        ts_code = f.stem
        df = pd.read_csv(f).tail(days)
        result[ts_code] = df
    return result


def get_board_stocks(board_name: str = None) -> pd.DataFrame | dict:
    """获取板块成分股，不指定则返回所有板块列表"""
    if board_name is None:
        boards = {}
        for f in BOARD_DIR.glob("*.csv"):
            boards[f.stem] = pd.read_csv(f)
        return boards
    path = BOARD_DIR / f"{board_name}.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def get_stock_list() -> pd.DataFrame:
    """获取股票列表"""
    return pd.read_csv(DATA_DIR / "stock_list.csv")


def get_stock_boards(ts_code: str = None) -> dict:
    """获取股票-板块映射"""
    with open(DATA_DIR / "stock_to_boards.json") as f:
        data = json.load(f)
    if ts_code:
        return data.get(ts_code, [])
    return data


def get_trade_calendar(start: str = None, end: str = None) -> pd.DataFrame:
    """获取交易日历，start/end 格式: YYYYMMDD"""
    df = pd.read_csv(DATA_DIR / "trade_calendar.csv")
    if start:
        df = df[df["cal_date"] >= int(start)]
    if end:
        df = df[df["cal_date"] <= int(end)]
    return df


def get_latest_trade_date() -> str:
    """获取最新交易日"""
    df = pd.read_csv(DATA_DIR / "trade_calendar.csv")
    df = df[df["is_open"] == 1]
    return str(df["cal_date"].max())


def get_news(date: str = None) -> dict:
    """获取新闻，date 格式: YYYY-MM-DD，不指定则返回最新"""
    news_dir = DATA_DIR / "news"
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")
    path = news_dir / f"news_{date}.json"
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(description="本地数据获取器")
    parser.add_argument("--type", "-t", required=True, 
                        choices=["daily", "board", "list", "boards", "calendar", "news"],
                        help="数据类型: daily=股票日线, board=板块成分股, list=股票列表, boards=股票-板块映射, calendar=交易日历, news=新闻")
    parser.add_argument("--code", "-c", help="股票代码，如 600000.SH")
    parser.add_argument("--board", "-b", help="板块名称，如 半导体设备")
    parser.add_argument("--days", "-d", type=int, default=252, help="获取天数，默认252天")
    parser.add_argument("--start", "-s", help="开始日期，格式 YYYYMMDD")
    parser.add_argument("--end", "-e", help="结束日期，格式 YYYYMMDD")
    parser.add_argument("--date", help="新闻日期，格式 YYYY-MM-DD")
    
    args = parser.parse_args()
    
    if args.type == "daily":
        if args.code:
            df = get_stock_daily(args.code, args.days)
        else:
            print("请指定 --code")
            sys.exit(1)
    elif args.type == "board":
        df = get_board_stocks(args.board)
    elif args.type == "list":
        df = get_stock_list()
    elif args.type == "boards":
        data = get_stock_boards(args.code)
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return
    elif args.type == "calendar":
        df = get_trade_calendar(args.start, args.end)
    elif args.type == "news":
        data = get_news(args.date)
        print(json.dumps(data, ensure_ascii=False, indent=2)[:5000])
        return
    
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
