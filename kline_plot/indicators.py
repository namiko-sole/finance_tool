#!/usr/bin/env python3
"""
技术指标计算
"""

import pandas as pd
import numpy as np


def ema(series, period):
    """指数移动平均"""
    return series.ewm(span=period, adjust=False).mean()


def macd(df, fast=12, slow=26, signal=9):
    """
    MACD指标
    
    Returns:
        DataFrame with columns: macd, signal, hist
    """
    close = df['close']
    
    ema_fast = ema(close, fast)
    ema_slow = ema(close, slow)
    
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, signal)
    histogram = macd_line - signal_line
    
    return pd.DataFrame({
        'macd': macd_line,
        'signal': signal_line,
        'hist': histogram
    })


def rsi(df, period=14):
    """
    RSI指标
    
    Args:
        df: DataFrame with 'close' column
        period: RSI周期
    
    Returns:
        Series
    """
    close = df['close']
    delta = close.diff()
    
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi


def kdj(df, n=9, m1=3, m2=3):
    """
    KDJ指标
    
    Args:
        df: DataFrame with 'high', 'low', 'close' columns
        n: RSV周期
        m1: K线平滑因子
        m2: D线平滑因子
    
    Returns:
        DataFrame with columns: k, d, j
    """
    high = df['high']
    low = df['low']
    close = df['close']
    
    lowest_low = low.rolling(window=n).min()
    highest_high = high.rolling(window=n).max()
    
    rsv = (close - lowest_low) / (highest_high - lowest_low) * 100
    rsv = rsv.fillna(50)
    
    k = rsv.ewm(adjust=False, alpha=1/m1).mean()
    d = k.ewm(adjust=False, alpha=1/m2).mean()
    j = 3 * k - 2 * d
    
    return pd.DataFrame({
        'k': k,
        'd': d,
        'j': j
    })


def bollinger_bands(df, period=20, std_dev=2):
    """
    布林带指标
    
    Args:
        df: DataFrame with 'close' column
        period: 均线周期
        std_dev: 标准差倍数
    
    Returns:
        DataFrame with columns: upper, middle, lower
    """
    close = df['close']
    
    middle = close.rolling(window=period).mean()
    std = close.rolling(window=period).std()
    
    upper = middle + (std * std_dev)
    lower = middle - (std * std_dev)
    
    return pd.DataFrame({
        'upper': upper,
        'middle': middle,
        'lower': lower
    })


def moving_average(df, periods=[5, 10, 20, 30, 60]):
    """
    移动平均线
    
    Args:
        df: DataFrame with 'close' column
        periods: 均线周期列表
    
    Returns:
        DataFrame with columns: MA5, MA10, ...
    """
    close = df['close']
    result = {}
    
    for period in periods:
        result[f'MA{period}'] = close.rolling(window=period).mean()
    
    return pd.DataFrame(result)


def volume_ma(df, periods=[5, 10]):
    """
    成交量均线

    Args:
        df: DataFrame with 'vol' column
        periods: 周期列表

    Returns:
        DataFrame
    """
    vol = df['vol']
    result = {}

    for period in periods:
        result[f'VOL_MA{period}'] = vol.rolling(window=period).mean()

    return pd.DataFrame(result)


def bulao(df):
    """
    不落浪指标 - 通达信指标移植

    Args:
        df: DataFrame with 'close', 'high', 'low' columns

    Returns:
        DataFrame with columns: xys0, x1, x2
    """
    close = df['close']
    high = df['high']
    low = df['low']

    # WY1001:=(2*CLOSE+HIGH+LOW)/4
    wy1001 = (2 * close + high + low) / 4

    # 三层EMA平滑
    wy1002 = ema(wy1001, 3)
    wy1003 = ema(wy1002, 3)
    wy1004 = ema(wy1003, 3)

    # XYS0: 变化率百分比
    xys0 = (wy1004 - wy1004.shift(1)) / wy1004.shift(1) * 100

    # X1, X2: 移动平均
    x1 = xys0.rolling(window=1).mean()
    x2 = xys0.rolling(window=2).mean()

    return pd.DataFrame({
        'xys0': xys0,
        'x1': x1,
        'x2': x2
    })
