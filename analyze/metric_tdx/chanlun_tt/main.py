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
    
    C=df.close.values; O=df.open.values
    H=df.high.values; L=df.low.values
    
    # 创建注解构建器
    builder = AnnotationBuilder(df)

    N = (0, 1, 1)
    GSKZA = BACKSET(LLV(L, 5) < REF(LLV(L, 4), 1), 4)
    GSKZB = BACKSET((GSKZA == 0) & (REF(GSKZA, 1) == 1), 2)
    GSKZC = IF((GSKZB == 1) & (REF(GSKZB, 1) == 0), -1, 0)
    缠A = BACKSET(HHV(H, 5) > REF(HHV(H, 4), 1), 4)
    缠B = BACKSET((缠A == 0) & (REF(缠A, 1) == 1), 2)
    缠C = IF((缠B == 1) & (REF(缠B, 1) == 0), 1, 0)
    QKPD = IF(L > REF(H, 1), 1, IF(H < REF(L, 1), -1, 0))
    JQTG = BARSLAST(缠C == 1)
    JQDD = BARSLAST(GSKZC == -1)
    XZZQ = LOWRANGE(L)
    DZZQ = TOPRANGE(H)
    DBLAA = IF((GSKZC == -1) & (REF(JQTG, 1) > REF(JQDD, 1)) & (LLV(L, JQTG + 1) < REF(LLV(L, JQTG + 1), 1)), -1, 0)
    DBLAB = IF((GSKZC == -1) & (REF(JQTG, 1) <= REF(JQDD, 1)) & ((JQTG >= 4) | (LLV(QKPD, JQTG) == -1) | (LLV(L, JQDD + 2) < REF(LLV(L, JQDD + 1), 1))), -1, 0)
    DBLS = IF(((DBLAA == -1) | (DBLAB == -1)) & (L < REF(H, JQTG + 1)), -1, 0)
    DTIME = 11
    A = (H == HHV(H, DTIME * 5)) & (HHV(H, DTIME * 5) > REF(HHV(H, DTIME * 5), 1))
    B = (L == LLV(L, DTIME * 5)) & (LLV(L, DTIME * 5) < REF(LLV(L, DTIME * 5), 1))

    # # 通达信绘图样式参数(LINETHICK/COLOR等)在Python中不直接可用，这里仅保留线数据
    # # DRAWLINE(cond1, price1, cond2, price2, builder, color, style, width)
    # 画线 = DRAWLINE(A, H, B, L, builder, 'COLOR3300FF', 'solid', 2)
    # 中枢 = DRAWLINE(B, L, A, H, builder, 'COLOR3300FF', 'solid', 2)

    YP = IF(((JQDD < 4) & (HHV(QKPD, JQDD) == 1)) | (REF(DBLS, JQDD) == 0), 1, 0)
    PD = IF((缠C == 1) & (REF(JQDD, 1) <= REF(JQTG, 1)) & (YP == 1) & (DZZQ > REF(XZZQ, JQDD + 1)) & (DZZQ > REF(XZZQ, JQDD)) & (DZZQ > REF(DZZQ, JQTG)), 1, 0)
    GBLA = IF((缠C == 1) & (REF(JQDD, 1) > REF(JQTG, 1)) & (HHV(H, JQDD + 1) > REF(HHV(H, JQDD + 1), 1)), 1, 0)
    GBLB = IF((缠C == 1) & (REF(JQDD, 1) <= REF(JQTG, 1)) & (REF(DBLS, JQDD) == -1) & ((JQDD >= 4) | (HHV(QKPD, JQDD) == 1)), 1, 0)
    GBL = IF(((GBLA == 1) | (GBLB == 1) | (PD == 1)) & (H > REF(L, JQDD + 1)), 1, 0)
    YPA = IF(((JQTG < 4) & (HHV(QKPD, JQTG) != 1)) | (REF(GBL, JQTG) == 0), 1, 0)
    PDA = IF((GSKZC == -1) & (REF(JQTG, 1) <= REF(JQDD, 1)) & (YPA == 1) & (XZZQ > REF(DZZQ, JQTG + 1)) & (XZZQ > REF(DZZQ, JQTG)) & (XZZQ > REF(XZZQ, JQDD)), -1, 0)
    DBLA = IF((GSKZC == -1) & (REF(JQTG, 1) > REF(JQDD, 1)) & (LLV(L, JQTG + 1) < REF(LLV(L, JQTG + 1), 1)), -1, 0)
    DBLB = IF((GSKZC == -1) & (REF(JQTG, 1) <= REF(JQDD, 1)) & ((JQTG >= 4) | (LLV(QKPD, JQTG) == -1) | (PDA == -1)), -1, 0)
    DBL = IF(((DBLA == -1) | (DBLB == -1)) & (L < REF(H, JQTG + 1)), -1, 0)
    JQTGA = BARSLAST(GBL == 1)
    JQDDA = BARSLAST(DBL == -1)
    YPX = IF(((JQDDA < 4) & (HHV(QKPD, JQDDA) == 1)) | (REF(DBL, JQDDA) == 0), 1, 0)
    PDX = IF((缠C == 1) & (REF(JQDDA, 1) <= REF(JQTGA, 1)) & (YPX == 1) & (DZZQ > REF(XZZQ, JQDDA + 1)) & (DZZQ > REF(XZZQ, JQDDA)) & (DZZQ > REF(DZZQ, JQTGA)), 1, 0)
    GBLXA = IF((缠C == 1) & (REF(JQDDA, 1) > REF(JQTGA, 1)) & (HHV(H, JQDDA + 1) > REF(HHV(H, JQDDA + 1), 1)), 1, 0)
    GBLXB = IF((缠C == 1) & (REF(JQDDA, 1) <= REF(JQTGA, 1)) & (REF(DBL, JQDDA) == -1) & ((JQDDA >= 4) | (HHV(QKPD, JQDDA) == 1)), 1, 0)
    GBLX = IF(((GBLXA == 1) | (GBLXB == 1) | (PDX == 1)) & (H > REF(L, JQDDA + 1)), 1, 0)
    YPXA = IF(((JQTGA < 4) & (HHV(QKPD, JQTGA) != 1)) | (REF(GBLXA, JQTGA) == 0), 1, 0)
    PDXA = IF((GSKZC == -1) & (REF(JQTGA, 1) <= REF(JQDDA, 1)) & (YPXA == 1) & (XZZQ > REF(DZZQ, JQTGA + 1)) & (XZZQ > REF(DZZQ, JQTGA)) & (XZZQ > REF(XZZQ, JQDDA)), -1, 0)
    DBLXA = IF((GSKZC == -1) & (REF(JQTGA, 1) > REF(JQDDA, 1)) & (LLV(L, JQTGA + 1) < REF(LLV(L, JQTGA + 1), 1)), -1, 0)
    DBLXB = IF((GSKZC == -1) & (REF(JQTGA, 1) <= REF(JQDDA, 1)) & ((JQTGA >= 4) | (LLV(QKPD, JQTGA) == -1) | (PDXA == -1)), -1, 0)
    DBLX = IF(((DBLXA == -1) | (DBLXB == -1)) & (L < REF(H, JQTGA + 1)), -1, 0)
    JQTGYA = BARSLAST(GBLX == 1)
    JQDDYA = BARSLAST(DBLX == -1)
    YPYX = IF(((JQDDYA < 4) & (HHV(QKPD, JQDDYA) == 1)) | (REF(DBLX, JQDDYA) == 0), 1, 0)
    PDYX = IF((缠C == 1) & (REF(JQDDYA, 1) <= REF(JQTGYA, 1)) & (YPYX == 1) & (DZZQ > REF(XZZQ, JQDDYA + 1)) & (DZZQ > REF(XZZQ, JQDDYA)) & (DZZQ > REF(DZZQ, JQTGYA)), 1, 0)
    GBLYXA = IF((缠C == 1) & (REF(JQDDYA, 1) > REF(JQTGYA, 1)) & (HHV(H, JQDDYA + 1) > REF(HHV(H, JQDDYA + 1), 1)), 1, 0)
    GBLYXB = IF((缠C == 1) & (REF(JQDDYA, 1) <= REF(JQTGYA, 1)) & (REF(DBLX, JQDDYA) == -1) & ((JQDDYA >= 4) | (HHV(QKPD, JQDDYA) == 1)), 1, 0)
    GBLYX = IF(((GBLYXA == 1) | (GBLYXB == 1) | (PDYX == 1)) & (H > REF(L, JQDDYA + 1)), 1, 0)
    YPYXA = IF(((JQTGYA < 4) & (HHV(QKPD, JQTGYA) == 1)) | (REF(GBLYXA, JQTGYA) == 0), 1, 0)
    PDYXA = IF((GSKZC == -1) & (REF(JQTGYA, 1) <= REF(JQDDYA, 1)) & (YPYXA == 1) & (XZZQ > REF(DZZQ, JQTGYA + 1)) & (XZZQ > REF(DZZQ, JQTGYA)) & (XZZQ > REF(XZZQ, JQDDYA)), -1, 0)
    DBLYXA = IF((GSKZC == -1) & (REF(JQTGYA, 1) > REF(JQDDYA, 1)) & (LLV(L, JQTGYA + 1) < REF(LLV(L, JQTGYA + 1), 1)), -1, 0)
    DBLYXB = IF((GSKZC == -1) & (REF(JQTGYA, 1) <= REF(JQDDYA, 1)) & ((JQTGYA >= 4) | (LLV(QKPD, JQTGYA) == -1) | (PDYXA == -1)), -1, 0)
    DBLYX = IF(((DBLYXA == -1) | (DBLYXB == -1)) & (L < REF(H, JQTGYA + 1)), -1, 0)
    AAAD = IF((GBLYX == 1) & (DBLYX == -1) & (H > REF(H, REF(JQTGYA, 1) + 2)), 1, IF((GBLYX == 1) & (DBLYX == -1) & (L < REF(L, REF(JQDDYA, 1) + 2)), -1, 0))
    JDBL = IF(AAAD == 0, GBLYX + DBLYX, AAAD)

    # 原公式中的 DRAWNULL/CIRCLEDOT/COLOR/DRAWTEXT 是绘图语法，这里保留信号序列
    # 缠: IF(JDBL=-1, L*0.99, IF(JDBL=1, H*1.01, DRAWNULL)), CIRCLEDOT, COLORLIMAGENTA
    缠 = IF(JDBL == -1, L * 0.99, IF(JDBL == 1, H * 1.01, np.nan))
    # # 添加圆点标记
    # 缠_dot = CIRCLEDOT(缠, builder, 'COLORLIMAGENTA')

    # DRAWTEXT(JDBL=1, H*1.03, '卖'), COLORLIGREEN
    卖信号 = DRAWTEXT(JDBL == 1, H * 1.03, '卖', builder, 'COLORLIGREEN')

    # DRAWTEXT(JDBL=-1, L*0.97, '买'), COLORRED
    买信号 = DRAWTEXT(JDBL == -1, L * 0.97, '买', builder, 'COLORRED')

    # 缠论1: DRAWLINE(JDBL=-1, 缠, JDBL=1, 缠, 0), DOTLINE, COLORFF6600
    缠论1 = DRAWLINE(JDBL == -1, 缠, JDBL == 1, 缠, builder, 'COLORFF6600', 'dotted', 1)

    # 缠论2: DRAWLINE(JDBL=1, 缠, JDBL=-1, 缠, 0), DOTLINE, COLORFF6600
    缠论2 = DRAWLINE(JDBL == 1, 缠, JDBL == -1, 缠, builder, 'COLOR3300FF', 'solid', 1, merge=True)

    # 输出JSON到文件
    output_dir = '/root/.openclaw/workspace/output/kline'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'chanlun_tt_annotations.json')

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(builder.to_json())

    print(f"Annotations saved to {output_path}")
    print(builder.to_json())


def get_buy_sell_signals(stock_code, start_date=None, end_date=None, data_fetcher=None):
    """
    获取股票的缠论买卖信号
    
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
                - jdbl: 信号值 (1=卖出, -1=买入)
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
    O = df.open.values
    H = df.high.values
    L = df.low.values
    
    # 创建注解构建器
    builder = AnnotationBuilder(df)
    
    N = (0, 1, 1)
    GSKZA = BACKSET(LLV(L, 5) < REF(LLV(L, 4), 1), 4)
    GSKZB = BACKSET((GSKZA == 0) & (REF(GSKZA, 1) == 1), 2)
    GSKZC = IF((GSKZB == 1) & (REF(GSKZB, 1) == 0), -1, 0)
    缠A = BACKSET(HHV(H, 5) > REF(HHV(H, 4), 1), 4)
    缠B = BACKSET((缠A == 0) & (REF(缠A, 1) == 1), 2)
    缠C = IF((缠B == 1) & (REF(缠B, 1) == 0), 1, 0)
    QKPD = IF(L > REF(H, 1), 1, IF(H < REF(L, 1), -1, 0))
    JQTG = BARSLAST(缠C == 1)
    JQDD = BARSLAST(GSKZC == -1)
    XZZQ = LOWRANGE(L)
    DZZQ = TOPRANGE(H)
    DBLAA = IF((GSKZC == -1) & (REF(JQTG, 1) > REF(JQDD, 1)) & (LLV(L, JQTG + 1) < REF(LLV(L, JQTG + 1), 1)), -1, 0)
    DBLAB = IF((GSKZC == -1) & (REF(JQTG, 1) <= REF(JQDD, 1)) & ((JQTG >= 4) | (LLV(QKPD, JQTG) == -1) | (LLV(L, JQDD + 2) < REF(LLV(L, JQDD + 1), 1))), -1, 0)
    DBLS = IF(((DBLAA == -1) | (DBLAB == -1)) & (L < REF(H, JQTG + 1)), -1, 0)
    DTIME = 11
    A = (H == HHV(H, DTIME * 5)) & (HHV(H, DTIME * 5) > REF(HHV(H, DTIME * 5), 1))
    B = (L == LLV(L, DTIME * 5)) & (LLV(L, DTIME * 5) < REF(LLV(L, DTIME * 5), 1))
    
    YP = IF(((JQDD < 4) & (HHV(QKPD, JQDD) == 1)) | (REF(DBLS, JQDD) == 0), 1, 0)
    PD = IF((缠C == 1) & (REF(JQDD, 1) <= REF(JQTG, 1)) & (YP == 1) & (DZZQ > REF(XZZQ, JQDD + 1)) & (DZZQ > REF(XZZQ, JQDD)) & (DZZQ > REF(DZZQ, JQTG)), 1, 0)
    GBLA = IF((缠C == 1) & (REF(JQDD, 1) > REF(JQTG, 1)) & (HHV(H, JQDD + 1) > REF(HHV(H, JQDD + 1), 1)), 1, 0)
    GBLB = IF((缠C == 1) & (REF(JQDD, 1) <= REF(JQTG, 1)) & (REF(DBLS, JQDD) == -1) & ((JQDD >= 4) | (HHV(QKPD, JQDD) == 1)), 1, 0)
    GBL = IF(((GBLA == 1) | (GBLB == 1) | (PD == 1)) & (H > REF(L, JQDD + 1)), 1, 0)
    YPA = IF(((JQTG < 4) & (HHV(QKPD, JQTG) != 1)) | (REF(GBL, JQTG) == 0), 1, 0)
    PDA = IF((GSKZC == -1) & (REF(JQTG, 1) <= REF(JQDD, 1)) & (YPA == 1) & (XZZQ > REF(DZZQ, JQTG + 1)) & (XZZQ > REF(DZZQ, JQTG)) & (XZZQ > REF(XZZQ, JQDD)), -1, 0)
    DBLA = IF((GSKZC == -1) & (REF(JQTG, 1) > REF(JQDD, 1)) & (LLV(L, JQTG + 1) < REF(LLV(L, JQTG + 1), 1)), -1, 0)
    DBLB = IF((GSKZC == -1) & (REF(JQTG, 1) <= REF(JQDD, 1)) & ((JQTG >= 4) | (LLV(QKPD, JQTG) == -1) | (PDA == -1)), -1, 0)
    DBL = IF(((DBLA == -1) | (DBLB == -1)) & (L < REF(H, JQTG + 1)), -1, 0)
    JQTGA = BARSLAST(GBL == 1)
    JQDDA = BARSLAST(DBL == -1)
    YPX = IF(((JQDDA < 4) & (HHV(QKPD, JQDDA) == 1)) | (REF(DBL, JQDDA) == 0), 1, 0)
    PDX = IF((缠C == 1) & (REF(JQDDA, 1) <= REF(JQTGA, 1)) & (YPX == 1) & (DZZQ > REF(XZZQ, JQDDA + 1)) & (DZZQ > REF(XZZQ, JQDDA)) & (DZZQ > REF(DZZQ, JQTGA)), 1, 0)
    GBLXA = IF((缠C == 1) & (REF(JQDDA, 1) > REF(JQTGA, 1)) & (HHV(H, JQDDA + 1) > REF(HHV(H, JQDDA + 1), 1)), 1, 0)
    GBLXB = IF((缠C == 1) & (REF(JQDDA, 1) <= REF(JQTGA, 1)) & (REF(DBL, JQDDA) == -1) & ((JQDDA >= 4) | (HHV(QKPD, JQDDA) == 1)), 1, 0)
    GBLX = IF(((GBLXA == 1) | (GBLXB == 1) | (PDX == 1)) & (H > REF(L, JQDDA + 1)), 1, 0)
    YPXA = IF(((JQTGA < 4) & (HHV(QKPD, JQTGA) != 1)) | (REF(GBLXA, JQTGA) == 0), 1, 0)
    PDXA = IF((GSKZC == -1) & (REF(JQTGA, 1) <= REF(JQDDA, 1)) & (YPXA == 1) & (XZZQ > REF(DZZQ, JQTGA + 1)) & (XZZQ > REF(DZZQ, JQTGA)) & (XZZQ > REF(XZZQ, JQDDA)), -1, 0)
    DBLXA = IF((GSKZC == -1) & (REF(JQTGA, 1) > REF(JQDDA, 1)) & (LLV(L, JQTGA + 1) < REF(LLV(L, JQTGA + 1), 1)), -1, 0)
    DBLXB = IF((GSKZC == -1) & (REF(JQTGA, 1) <= REF(JQDDA, 1)) & ((JQTGA >= 4) | (LLV(QKPD, JQTGA) == -1) | (PDXA == -1)), -1, 0)
    DBLX = IF(((DBLXA == -1) | (DBLXB == -1)) & (L < REF(H, JQTGA + 1)), -1, 0)
    JQTGYA = BARSLAST(GBLX == 1)
    JQDDYA = BARSLAST(DBLX == -1)
    YPYX = IF(((JQDDYA < 4) & (HHV(QKPD, JQDDYA) == 1)) | (REF(DBLX, JQDDYA) == 0), 1, 0)
    PDYX = IF((缠C == 1) & (REF(JQDDYA, 1) <= REF(JQTGYA, 1)) & (YPYX == 1) & (DZZQ > REF(XZZQ, JQDDYA + 1)) & (DZZQ > REF(XZZQ, JQDDYA)) & (DZZQ > REF(DZZQ, JQTGYA)), 1, 0)
    GBLYXA = IF((缠C == 1) & (REF(JQDDYA, 1) > REF(JQTGYA, 1)) & (HHV(H, JQDDYA + 1) > REF(HHV(H, JQDDYA + 1), 1)), 1, 0)
    GBLYXB = IF((缠C == 1) & (REF(JQDDYA, 1) <= REF(JQTGYA, 1)) & (REF(DBLX, JQDDYA) == -1) & ((JQDDYA >= 4) | (HHV(QKPD, JQDDYA) == 1)), 1, 0)
    GBLYX = IF(((GBLYXA == 1) | (GBLYXB == 1) | (PDYX == 1)) & (H > REF(L, JQDDYA + 1)), 1, 0)
    YPYXA = IF(((JQTGYA < 4) & (HHV(QKPD, JQTGYA) == 1)) | (REF(GBLYXA, JQTGYA) == 0), 1, 0)
    PDYXA = IF((GSKZC == -1) & (REF(JQTGYA, 1) <= REF(JQDDYA, 1)) & (YPYXA == 1) & (XZZQ > REF(DZZQ, JQTGYA + 1)) & (XZZQ > REF(DZZQ, JQTGYA)) & (XZZQ > REF(XZZQ, JQDDYA)), -1, 0)
    DBLYXA = IF((GSKZC == -1) & (REF(JQTGYA, 1) > REF(JQDDA, 1)) & (LLV(L, JQTGYA + 1) < REF(LLV(L, JQTGYA + 1), 1)), -1, 0)
    DBLYXB = IF((GSKZC == -1) & (REF(JQTGYA, 1) <= REF(JQDDYA, 1)) & ((JQTGYA >= 4) | (LLV(QKPD, JQTGYA) == -1) | (PDYXA == -1)), -1, 0)
    DBLYX = IF(((DBLYXA == -1) | (DBLYXB == -1)) & (L < REF(H, JQTGYA + 1)), -1, 0)
    AAAD = IF((GBLYX == 1) & (DBLYX == -1) & (H > REF(H, REF(JQTGYA, 1) + 2)), 1, IF((GBLYX == 1) & (DBLYX == -1) & (L < REF(L, REF(JQDDYA, 1) + 2)), -1, 0))
    JDBL = IF(AAAD == 0, GBLYX + DBLYX, AAAD)
    
    # 缠 = IF(JDBL == -1, L * 0.99, IF(JDBL == 1, H * 1.01, np.nan))
    # 卖信号 = DRAWTEXT(JDBL == 1, H * 1.03, '卖', builder, 'COLORLIGREEN')
    # 买信号 = DRAWTEXT(JDBL == -1, L * 0.97, '买', builder, 'COLORRED')
    
    # 提取买卖信号
    signals = []
    for i in range(len(df)):
        date_str = df.iloc[i]['date'] if 'date' in df.columns else df.index[i]
        if hasattr(date_str, 'strftime'):
            date_str = date_str.strftime('%Y-%m-%d')
        
        jdbl_val = JDBL[i]
        if jdbl_val == 1:
            signals.append({
                'date': date_str,
                'signal': '卖出',
                'price': float(H[i] * 1.03),
                'jdbl': int(jdbl_val)
            })
        elif jdbl_val == -1:
            signals.append({
                'date': date_str,
                'signal': '买入',
                'price': float(L[i] * 0.97),
                'jdbl': int(jdbl_val)
            })
    
    return {
        'stock_code': stock_code,
        'signals': signals,
        'df': df,
        'annotations': json.loads(builder.to_json()) if builder.to_json() else None
    }


if __name__ == "__main__":
    main()