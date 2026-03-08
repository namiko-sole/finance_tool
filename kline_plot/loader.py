#!/usr/bin/env python3
"""
数据加载器
"""

import os
import pandas as pd
from datetime import datetime, timedelta

# 数据目录
DATA_DIR = "/root/.openclaw/workspace/data/raw"
STOCK_DAILY_DIR = os.path.join(DATA_DIR, "stock_daily")


def get_stock_list():
    """
    获取所有股票代码列表
    """
    if not os.path.exists(STOCK_DAILY_DIR):
        return []
    
    files = os.listdir(STOCK_DAILY_DIR)
    stocks = [f.replace('.csv', '') for f in files if f.endswith('.csv')]
    return sorted(stocks)


def find_stock_code(keyword):
    """
    根据关键词查找股票代码
    
    Args:
        keyword: 股票代码、名称、拼音缩写
    
    Returns:
        str or None: 股票代码
    """
    from . import STOCK_LIST_CACHE
    
    keyword = keyword.strip().upper()
    
    # 直接匹配
    if keyword in STOCK_LIST_CACHE:
        return keyword
    
    # 模糊匹配
    load_stock_list_cache()  # 确保缓存已加载
    matches = [code for code in STOCK_LIST_CACHE if keyword in code]
    
    if len(matches) == 1:
        return matches[0]
    elif len(matches) > 1:
        return matches[0]  # 返回第一个
    
    return None


# 简单的股票列表缓存（实际使用时从stock_list.csv加载）
STOCK_LIST_CACHE = None


def load_stock_list_cache():
    """加载股票列表到缓存"""
    global STOCK_LIST_CACHE
    
    if STOCK_LIST_CACHE is not None:
        return STOCK_LIST_CACHE
    
    stock_list_path = os.path.join(DATA_DIR, "stock_list.csv")
    
    if not os.path.exists(stock_list_path):
        # 使用目录中的文件
        STOCK_LIST_CACHE = set(get_stock_list())
        return STOCK_LIST_CACHE
    
    try:
        df = pd.read_csv(stock_list_path, encoding='utf-8-sig')
        codes = df['ts_code'].tolist()
        STOCK_LIST_CACHE = set(codes)
    except:
        STOCK_LIST_CACHE = set(get_stock_list())
    
    return STOCK_LIST_CACHE


def stock_exists(stock_code):
    """
    检查股票代码是否存在
    """
    load_stock_list_cache()
    return stock_code in STOCK_LIST_CACHE


def get_latest_trade_date():
    """
    获取最近一个交易日
    """
    import json
    
    calendar_path = os.path.join(DATA_DIR, "trade_calendar_info.json")
    
    if os.path.exists(calendar_path):
        with open(calendar_path, 'r', encoding='utf-8') as f:
            info = json.load(f)
            return info.get('current_trade_day')
    
    # 如果没有日历，返回今天
    return datetime.now().strftime('%Y%m%d')


def get_date_range(days=30):
    """
    获取日期范围
    
    Args:
        days: 天数
    
    Returns:
        tuple: (start_date, end_date) YYYYMMDD
    """
    end_date = get_latest_trade_date()
    
    end_dt = datetime.strptime(end_date, '%Y%m%d')
    start_dt = end_dt - timedelta(days=days)
    start_date = start_dt.strftime('%Y%m%d')
    
    return start_date, end_date
