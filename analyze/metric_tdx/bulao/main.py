import sys
import os
import json
import numpy as np
import pandas as pd

# 添加路径
sys.path.insert(0, '/root/.openclaw/workspace/finance_tool/analyze/metric_tdx')
sys.path.insert(0, '/root/.openclaw/workspace/finance_tool/fetchers')

# 导入数据获取器
from data_fetcher import DataFetcher

from backset import *
from draw import *
from MyTT import *


def main():
    fetcher = DataFetcher()
    df = fetcher.get_stock_daily('601888', start_date='20250101', end_date=None)
    if df is None or df.empty:
        print("无法获取数据")
        return
    
    C = df.close.values
    O = df.open.values
    H = df.high.values
    L = df.low.values
    
    # 创建注解构建器
    builder = AnnotationBuilder(df)
    
    # BULAO 公式计算
    WY1001 = (2 * C + H + L) / 4
    WY1002 = EMA(WY1001, 3)
    WY1003 = EMA(WY1002, 3)
    WY1004 = EMA(WY1003, 3)
    XYS0 = (WY1004 - REF(WY1004, 1)) / REF(WY1004, 1) * 100
    X1 = MA(XYS0, 1)
    X2 = MA(XYS0, 2)
    
    # 金叉死叉信号检测
    # 金叉: X1从下方穿过X2 (买入信号)
    # 死叉: X1从上方穿过X2 (卖出信号)
    golden_cross = (X1 > X2) & (REF(X1, 1) <= REF(X2, 1))  # X1上穿X2
    dead_cross = (X1 < X2) & (REF(X1, 1) >= REF(X2, 1))    # X1下穿X2
    
    # 添加买入信号标注
    buy_signal = DRAWTEXT(golden_cross == 1, L * 0.98, '买', builder, 'COLORRED')
    
    # 添加卖出信号标注
    sell_signal = DRAWTEXT(dead_cross == 1, H * 1.02, '卖', builder, 'COLORGREEN')
    
    # 输出JSON到文件
    output_dir = '/root/.openclaw/workspace/output/kline'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'bulao_annotations.json')
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(builder.to_json())
    
    print(f"Annotations saved to {output_path}")
    print(builder.to_json())


def get_buy_sell_signals(stock_code, start_date=None, end_date=None, data_fetcher=None):
    """
    获取股票的BULAO买卖信号
    
    参数:
        stock_code: 股票代码，如 '601888'
        start_date: 开始日期，格式 'YYYYMMDD'，如 None 则从一年前开始
        end_date: 结束日期，格式 'YYYYMMDD'，如 None 则取到最新
        data_fetcher: DataFetcher实例，如为 None 则创建新实例
    
    返回:
        dict: 包含以下键的结构化数据:
            - stock_code: 股票代码
            - signals: list of dict，每个信号包含:
                - date: 日期 (YYYY-MM-DD)
                - signal: '买入' 或 '卖出'
                - price: 信号触发时的价格
                - xys0: XYS0指标值
                - x1: X1指标值
                - x2: X2指标值
            - df: pandas DataFrame，原始K线数据
            - annotations: dict，注解数据
    """
    # 使用传入的 data_fetcher 或创建新的
    fetcher = data_fetcher if data_fetcher else DataFetcher()
    
    # 获取数据
    df = fetcher.get_stock_daily(stock_code, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        return {
            'stock_code': stock_code,
            'error': '无法获取数据',
            'signals': [],
            'df': None,
            'annotations': None
        }
    
    C = df.close.values
    H = df.high.values
    L = df.low.values
    
    # 创建注解构建器
    builder = AnnotationBuilder(df)
    
    # BULAO 公式计算
    WY1001 = (2 * C + H + L) / 4
    WY1002 = EMA(WY1001, 3)
    WY1003 = EMA(WY1002, 3)
    WY1004 = EMA(WY1003, 3)
    XYS0 = (WY1004 - REF(WY1004, 1)) / REF(WY1004, 1) * 100
    X1 = MA(XYS0, 1)
    X2 = MA(XYS0, 2)
    
    # 金叉死叉信号检测
    # 金叉: X1从下方穿过X2 (买入信号)
    # 死叉: X1从上方穿过X2 (卖出信号)
    golden_cross = (X1 > X2) & (REF(X1, 1) <= REF(X2, 1))  # X1上穿X2
    dead_cross = (X1 < X2) & (REF(X1, 1) >= REF(X2, 1))    # X1下穿X2
    
    # 添加买入信号标注
    buy_signal = DRAWTEXT(golden_cross == 1, L * 0.98, '买', builder, 'COLORRED')
    
    # 添加卖出信号标注
    sell_signal = DRAWTEXT(dead_cross == 1, H * 1.02, '卖', builder, 'COLORGREEN')
    
    # 提取买卖信号
    signals = []
    for i in range(len(df)):
        date_str = df.iloc[i]['date'] if 'date' in df.columns else df.index[i]
        if hasattr(date_str, 'strftime'):
            date_str = date_str.strftime('%Y-%m-%d')
        
        # 使用收盘价作为信号价格
        price = float(C[i])
        xys0_val = float(XYS0[i]) if not np.isnan(XYS0[i]) else 0.0
        x1_val = float(X1[i]) if not np.isnan(X1[i]) else 0.0
        x2_val = float(X2[i]) if not np.isnan(X2[i]) else 0.0
        
        if golden_cross[i]:
            signals.append({
                'date': date_str,
                'signal': '买入',
                'price': price,
                'xys0': xys0_val,
                'x1': x1_val,
                'x2': x2_val
            })
        elif dead_cross[i]:
            signals.append({
                'date': date_str,
                'signal': '卖出',
                'price': price,
                'xys0': xys0_val,
                'x1': x1_val,
                'x2': x2_val
            })
    
    return {
        'stock_code': stock_code,
        'signals': signals,
        'df': df,
        'annotations': json.loads(builder.to_json()) if builder.to_json() else None
    }


if __name__ == "__main__":
    main()
