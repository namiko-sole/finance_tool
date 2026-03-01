#!/usr/bin/env python3
"""
新浪财经日线数据爬虫
支持A股（包括北交所）的日线K线数据获取

数据来源：新浪财经
API接口：http://money.finance.sina.com.cn/quotes_service/api/json_v2.php
"""

import requests
import json
import time
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pathlib import Path

# 添加父目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DATA_RAW, LOGS_DIR

# 输出目录
SINA_DIR = DATA_RAW / 'sina_kline'
SINA_FILE = SINA_DIR / 'daily_kline.json'

# 日志文件
CRAWLER_LOG = LOGS_DIR / 'sina_kline_crawler.log'


def log(msg: str):
    """日志记录"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}\n"
    print(line.strip())
    with open(CRAWLER_LOG, 'a', encoding='utf-8') as f:
        f.write(line)


# 新浪财经API接口
SINA_KLINE_API = "http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"

# 请求头
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Referer': 'http://finance.sina.com.cn',
}


def fetch_daily_kline(symbol: str, days: int = 120) -> Optional[pd.DataFrame]:
    """
    获取日线K线数据

    Args:
        symbol: 股票代码（如 'sh600000' 或 'sz000001' 或 'bj920000'）
        days: 获取天数（默认120天）

    Returns:
        DataFrame with columns: date, open, high, low, close, volume
    """
    log(f"📡 获取 {symbol} 日线数据（最近{days}天）...")

    # 计算日期范围
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    # API参数
    params = {
        'symbol': symbol,
        'scale': '240',  # 日线（240分钟）
        'ma': 'no',      # 不返回均线
        'datalen': days,
    }

    try:
        response = requests.get(
            SINA_KLINE_API,
            params=params,
            headers=HEADERS,
            timeout=30
        )

        if response.status_code == 200:
            # 新浪返回的是JavaScript格式的数据
            content = response.text

            # 尝试解析JSON（新浪API可能返回不同的格式）
            try:
                # 格式1: 纯JSON
                data = json.loads(content)
            except:
                # 格式2: 可能包含JavaScript回调
                # 提取JSON部分
                if '(' in content and ')' in content:
                    json_str = content[content.find('(')+1:content.rfind(')')]
                    data = json.loads(json_str)
                else:
                    log(f"  ⚠️ {symbol}: 无法解析数据格式")
                    return None

            # 解析K线数据
            if isinstance(data, list) and len(data) > 0:
                kline_data = []
                for item in data:
                    if isinstance(item, dict):
                        kline_data.append({
                            'date': item.get('d', ''),      # 日期
                            'open': float(item.get('o', 0)),  # 开盘
                            'high': float(item.get('h', 0)),  # 最高
                            'low': float(item.get('l', 0)),   # 最低
                            'close': float(item.get('c', 0)), # 收盘
                            'volume': int(item.get('v', 0))   # 成交量
                        })

                df = pd.DataFrame(kline_data)
                log(f"  ✅ {symbol}: 获取 {len(df)} 条K线数据")
                return df
            else:
                log(f"  ⚠️ {symbol}: 数据格式异常")
                return None

        else:
            log(f"  ❌ {symbol}: HTTP {response.status_code}")
            return None

    except requests.exceptions.Timeout:
        log(f"  ⏱️ {symbol}: 请求超时")
        return None
    except Exception as e:
        log(f"  ❌ {symbol}: {str(e)}")
        return None

    # 延迟避免过快请求
    time.sleep(1)


def fetch_historical_kline_html(symbol: str, page: int = 1) -> Optional[pd.DataFrame]:
    """
    通过HTML页面获取历史K线数据（备用方案）

    Args:
        symbol: 股票代码
        page: 页码

    Returns:
        DataFrame with K线数据
    """
    log(f"📡 通过HTML获取 {symbol} 历史数据（第{page}页）...")

    # 构造URL
    if symbol.startswith('sh'):
        code = symbol[2:]
        url = f"http://finance.sina.com.cn/realstock/company/{code}/historicaldata.html"
    elif symbol.startswith('sz'):
        code = symbol[2:]
        url = f"http://finance.sina.com.cn/realstock/company/{code}/historicaldata.html"
    elif symbol.startswith('bj'):
        code = symbol[2:]
        url = f"http://finance.sina.com.cn/realstock/company/bj{code}/historicaldata.html"
    else:
        log(f"  ⚠️ 不支持的股票代码格式: {symbol}")
        return None

    params = {
        'page': page
    }

    try:
        response = requests.get(
            url,
            params=params,
            headers=HEADERS,
            timeout=30
        )

        if response.status_code == 200:
            # 使用pandas直接读取HTML表格
            tables = pd.read_html(response.text)

            if len(tables) > 0:
                df = tables[0]

                # 重命名列
                df.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'chg', 'pct_chg', 'turnover']

                # 选择需要的列
                df = df[['date', 'open', 'high', 'low', 'close', 'volume']]

                # 转换数据类型
                df['open'] = df['open'].astype(float)
                df['high'] = df['high'].astype(float)
                df['low'] = df['low'].astype(float)
                df['close'] = df['close'].astype(float)
                df['volume'] = df['volume'].astype(str).str.replace(',', '').astype(int)

                log(f"  ✅ {symbol}: 获取 {len(df)} 条数据（HTML）")
                return df
            else:
                log(f"  ⚠️ {symbol}: 未找到数据表格")
                return None
        else:
            log(f"  ❌ {symbol}: HTTP {response.status_code}")
            return None

    except Exception as e:
        log(f"  ❌ {symbol}: HTML解析失败 - {str(e)}")
        return None

    time.sleep(2)


def save_kline_data(symbol: str, df: pd.DataFrame):
    """保存K线数据"""
    # 创建输出目录
    SINA_DIR.mkdir(parents=True, exist_ok=True)

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
    output_file = SINA_DIR / f'{symbol}_daily.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    log(f"💾 数据已保存: {output_file}")


def main():
    """主函数"""
    print("=" * 70)
    print("📊 新浪财经日线数据爬虫")
    print(f"⏰ 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    log("=" * 50)
    log("开始爬取新浪财经日线数据")
    log("=" * 50)

    # 测试股票代码（北交所）
    test_symbols = [
        'bj920000',  # 贝特瑞（北交所）
        'sh600000',  # 浦发银行（上交所）
        'sz000001',  # 平安银行（深交所）
    ]

    all_results = {}

    for symbol in test_symbols:
        # 方法1：尝试API接口
        df = fetch_daily_kline(symbol, days=120)

        # 方法2：如果API失败，尝试HTML页面
        if df is None or len(df) == 0:
            log(f"  🔄 尝试备用方案（HTML页面）...")
            df = fetch_historical_kline_html(symbol, page=1)

        # 保存数据
        if df is not None and len(df) > 0:
            save_kline_data(symbol, df)
            all_results[symbol] = {
                'count': len(df),
                'start': df['date'].iloc[0],
                'end': df['date'].iloc[-1]
            }

    # 统计
    print("\n" + "=" * 70)
    print("✅ 爬取完成")
    print(f"\n📊 数据统计:")
    for symbol, result in all_results.items():
        print(f"   {symbol}: {result['count']}条 ({result['start']} ~ {result['end']})")
    print("=" * 70)

    log("\n" + "=" * 50)
    log("新浪财经日线数据爬取完成")
    log("=" * 50)

    return 0


if __name__ == "__main__":
    import os
    exit(main())
