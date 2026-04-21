#!/usr/bin/env python3
"""
财报扫描器 - 每日扫描新披露的财报/业绩预告/快报
用法:
  python earnings_scanner.py                    # 扫描最新数据
  python earnings_scanner.py --date 20260409    # 指定日期
  python earnings_scanner.py --type forecast    # 只看业绩预告
  python earnings_scanner.py --type express     # 只看业绩快报
  python earnings_scanner.py --type dividend    # 只看分红送转
"""

import argparse
import tushare as ts
import pandas as pd
import os
from datetime import datetime, timedelta

TUSHARE_TOKEN = "da8b0417e689bd72a3e5b3312fb6094f9405b06b667f7033eebecf83"
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

DATA_DIR = "/root/.openclaw/workspace/data/raw/earnings"
os.makedirs(DATA_DIR, exist_ok=True)


def scan_forecast(ann_date):
    """扫描业绩预告"""
    try:
        df = pro.forecast(ann_date=ann_date)
        if df is not None and not df.empty:
            df = df.sort_values('end_date', ascending=False)
            return df
    except Exception as e:
        print(f"[WARN] forecast 查询失败: {e}")
    return pd.DataFrame()


def scan_express(ann_date):
    """扫描业绩快报"""
    try:
        df = pro.express(ann_date=ann_date)
        if df is not None and not df.empty:
            df = df.sort_values('end_date', ascending=False)
            return df
    except Exception as e:
        print(f"[WARN] express 查询失败: {e}")
    return pd.DataFrame()


def scan_dividend(ann_date):
    """扫描分红送转"""
    try:
        df = pro.dividend(ann_date=ann_date)
        if df is not None and not df.empty:
            return df
    except Exception as e:
        print(f"[WARN] dividend 查询失败: {e}")
    return pd.DataFrame()


def format_change(val):
    """格式化涨跌幅"""
    if pd.isna(val):
        return "N/A"
    return f"{val:+.2f}%"


def format_amount(val):
    """格式化金额（亿元）"""
    if pd.isna(val):
        return "N/A"
    return f"{val/1e8:.2f}亿"


def print_forecast_summary(df):
    """打印业绩预告摘要"""
    print(f"\n{'='*60}")
    print(f"📊 业绩预告 ({len(df)} 条)")
    print(f"{'='*60}")
    for _, row in df.head(30).iterrows():
        ts_code = row.get('ts_code', '')
        name = row.get('name', ts_code)
        end_date = row.get('end_date', '')
        type_map = {
            '1': '预增', '2': '预减', '3': '扭亏', '4': '首亏',
            '5': '续盈', '6': '续亏', '7': '略增', '8': '略减'
        }
        ptype = type_map.get(str(row.get('type', '')), str(row.get('type', '')))
        pct_min = row.get('pct_min', float('nan'))
        pct_max = row.get('pct_max', float('nan'))

        pct_str = ""
        if not pd.isna(pct_min) and not pd.isna(pct_max):
            pct_str = f"[{pct_min:+.0f}% ~ {pct_max:+.0f}%]"
        elif not pd.isna(pct_max):
            pct_str = f"[{pct_max:+.0f}%]"

        summary = row.get('summary', '')
        print(f"  🔹 {ts_code} {name} | {end_date} | {ptype} {pct_str}")
        if summary:
            print(f"     {summary}")


def print_express_summary(df):
    """打印业绩快报摘要"""
    print(f"\n{'='*60}")
    print(f"📋 业绩快报 ({len(df)} 条)")
    print(f"{'='*60}")
    for _, row in df.head(30).iterrows():
        ts_code = row.get('ts_code', '')
        name = row.get('name', ts_code)
        end_date = row.get('end_date', '')
        revenue = row.get('total_revenue', float('nan'))
        profit = row.get('net_profit', float('nan'))
        yoy_profit = row.get('yoy_net_profit', float('nan'))

        print(f"  🔹 {ts_code} {name} | {end_date}")
        if not pd.isna(revenue):
            print(f"     营收: {format_amount(revenue)}")
        if not pd.isna(profit):
            print(f"     净利: {format_amount(profit)} YoY: {format_change(yoy_profit)}")


def print_dividend_summary(df):
    """打印分红摘要"""
    print(f"\n{'='*60}")
    print(f"💰 分红送转 ({len(df)} 条)")
    print(f"{'='*60}")
    for _, row in df.head(30).iterrows():
        ts_code = row.get('ts_code', '')
        end_date = row.get('end_date', '')
        bonus = row.get('bonus_list_str', row.get('cash_div', ''))
        print(f"  🔹 {ts_code} | {end_date} | {bonus}")


def main():
    parser = argparse.ArgumentParser(description='财报扫描器')
    parser.add_argument('--date', type=str, help='公告日期 YYYYMMDD，默认今天')
    parser.add_argument('--type', type=str, choices=['forecast', 'express', 'dividend', 'all'],
                        default='all', help='扫描类型，默认all')
    args = parser.parse_args()

    ann_date = args.date or datetime.now().strftime('%Y%m%d')
    print(f"📅 扫描公告日期: {ann_date}")

    scan_type = args.type

    results = {}

    if scan_type in ('all', 'forecast'):
        df = scan_forecast(ann_date)
        if not df.empty:
            print_forecast_summary(df)
            df.to_csv(f"{DATA_DIR}/forecast_{ann_date}.csv", index=False)
            results['forecast'] = len(df)
        else:
            print("\n📊 业绩预告: 无新数据")

    if scan_type in ('all', 'express'):
        df = scan_express(ann_date)
        if not df.empty:
            print_express_summary(df)
            df.to_csv(f"{DATA_DIR}/express_{ann_date}.csv", index=False)
            results['express'] = len(df)
        else:
            print("\n📋 业绩快报: 无新数据")

    if scan_type in ('all', 'dividend'):
        df = scan_dividend(ann_date)
        if not df.empty:
            print_dividend_summary(df)
            df.to_csv(f"{DATA_DIR}/dividend_{ann_date}.csv", index=False)
            results['dividend'] = len(df)
        else:
            print("\n💰 分红送转: 无新数据")

    # 汇总
    print(f"\n{'='*60}")
    print(f"📈 扫描汇总: {ann_date}")
    for k, v in results.items():
        print(f"  {k}: {v} 条")
    print(f"数据保存至: {DATA_DIR}/")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
