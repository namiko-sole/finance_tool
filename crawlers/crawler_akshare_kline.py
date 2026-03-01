#!/usr/bin/env python3
"""
使用AKShare获取股票历史数据
AKShare是一个开源的财经数据接口库
支持A股、港股、美股、期货、基金等

安装: pip install akshare
"""

import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

# 添加父目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DATA_RAW, LOGS_DIR
import json

# 输出目录
AKSHARE_DIR = DATA_RAW / 'akshare_kline'
AKSHARE_FILE = AKSHARE_DIR / 'daily_kline.json'

# 日志文件
CRAWLER_LOG = LOGS_DIR / 'akshare_kline_crawler.log'


def log(msg: str):
    """日志记录"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}\n"
    print(line.strip())
    with open(CRAWLER_LOG, 'a', encoding='utf-8') as f:
        f.write(line)


def fetch_stock_kline(symbol: str, period: str = 'daily', start_date: str = None, end_date: str = None):
    """
    使用AKShare获取股票K线数据

    Args:
        symbol: 股票代码（带市场前缀，如 'sh600519', 'sz000001', 'bj920000'）
        period: 周期（daily=日线, weekly=周线, monthly=月线）
        start_date: 开始日期（格式：YYYYMMDD）
        end_date: 结束日期（格式：YYYYMMDD）

    Returns:
        DataFrame with K线数据
    """
    try:
        import akshare as ak
    except ImportError:
        log("❌ 未安装akshare库，请运行: pip install akshare")
        return None

    log(f"📡 获取 {symbol} {period}数据...")

    # 默认日期：最近120天
    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=120)).strftime('%Y%m%d')

    # 转换股票代码格式（AKShare使用特定格式）
    # 例如：sh600519 -> 600519, bj920000 -> 920000
    code = symbol[2:] if len(symbol) > 6 else symbol

    try:
        if period == 'daily':
            # 日线数据
            df = ak.stock_zh_a_hist(
                symbol=code,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust=""
            )
        else:
            # 其他周期
            df = ak.stock_zh_a_hist(
                symbol=code,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust=""
            )

        if df is not None and len(df) > 0:
            # 查看实际列数
            log(f"  🔍 实际列数: {len(df.columns)}, 列名: {list(df.columns)}")

            # 根据实际列名映射到标准列名
            # AKShare返回的列名通常是中文
            column_mapping = {
                '日期': 'date',
                '股票代码': 'code',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'turnover',
                '振幅': 'amplitude',
                '涨跌幅': 'pct_chg',
                '涨跌额': 'chg',
                '换手率': 'turnover_rate'
            }

            # 重命名列
            df = df.rename(columns=column_mapping)

            # 选择需要的列（只选择存在的列）
            required_cols = [col for col in ['date', 'open', 'high', 'low', 'close', 'volume'] if col in df.columns]
            df = df[required_cols]

            log(f"  ✅ {symbol}: 获取 {len(df)} 条数据")
            return df
        else:
            log(f"  ⚠️ {symbol}: 无数据")
            return None

    except Exception as e:
        log(f"  ❌ {symbol}: {str(e)}")
        return None


def fetch_bj_stock(symbol: str, period: str = 'daily', days: int = 120):
    """
    获取北交所股票数据（使用通用接口）

    Args:
        symbol: 股票代码（如 'bj920000'）
        period: 周期
        days: 获取天数

    Returns:
        DataFrame with K线数据
    """
    log(f"📡 获取北交所股票 {symbol} 数据...")

    # 计算日期
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')

    # 直接使用通用接口
    return fetch_stock_kline(symbol, period, start_date=start_date, end_date=end_date)


def save_kline_data(symbol: str, df):
    """保存K线数据"""
    # 创建输出目录
    AKSHARE_DIR.mkdir(parents=True, exist_ok=True)

    # 转换日期列为字符串
    if 'date' in df.columns:
        df['date'] = df['date'].astype(str)

    # 转换为字典格式
    data = {
        'symbol': symbol,
        'crawl_time': datetime.now().isoformat(),
        'total_count': len(df),
        'date_range': {
            'start': df['date'].iloc[0] if len(df) > 0 else None,
            'end': df['date'].iloc[-1] if len(df) > 0 else None
        },
        'kline_data': df.to_dict('records')
    }

    # 保存到文件
    output_file = AKSHARE_DIR / f'{symbol}_daily.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    log(f"💾 数据已保存: {output_file}")


def main():
    """主函数"""
    print("=" * 70)
    print("📊 AKShare股票K线数据爬虫")
    print(f"⏰ 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    log("=" * 50)
    log("开始爬取股票K线数据（使用AKShare）")
    log("=" * 50)

    # 测试股票代码
    test_symbols = [
        ('bj920000', '北交所-贝特瑞'),  # 北交所
        ('sh600519', '上交所-茅台'),    # 上交所
        ('sz000001', '深交所-平安银行'),  # 深交所
    ]

    all_results = {}

    for symbol, name in test_symbols:
        log(f"\n📊 {name} ({symbol})")

        if symbol.startswith('bj'):
            df = fetch_bj_stock(symbol, days=120)
        else:
            # 修正：使用start_date和end_date参数
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=120)).strftime('%Y%m%d')
            df = fetch_stock_kline(symbol, start_date=start_date, end_date=end_date)

        if df is not None and len(df) > 0:
            save_kline_data(symbol, df)
            all_results[symbol] = {
                'name': name,
                'count': len(df),
                'start': df['date'].iloc[0],
                'end': df['date'].iloc[-1]
            }

    # 统计
    print("\n" + "=" * 70)
    print("✅ 爬取完成")
    print(f"\n📊 数据统计:")
    for symbol, result in all_results.items():
        print(f"   {result['name']} ({symbol}): {result['count']}条")
        print(f"      日期范围: {result['start']} ~ {result['end']}")
    print("=" * 70)

    log("\n" + "=" * 50)
    log("股票K线数据爬取完成")
    log("=" * 50)

    return 0


if __name__ == "__main__":
    exit(main())
