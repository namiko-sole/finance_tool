#!/usr/bin/env python3
"""
K线图绘制 - 核心逻辑
"""

import os
import json
import pandas as pd
import numpy as np
import mplfinance as mpf
from datetime import datetime
from . import indicators

# 配置中文字体
import matplotlib
import matplotlib.pyplot as plt

# 尝试设置中文字体
try:
    matplotlib.rcParams['font.sans-serif'] = ['SimHei', 'Noto Sans CJK SC', 'DejaVu Sans']
    matplotlib.rcParams['axes.unicode_minus'] = False
except:
    pass


def get_custom_style(base_style='yahoo'):
    """获取支持中文的自定义样式"""
    return mpf.make_mpf_style(
        base_mpf_style=base_style,
        rc={
            'font.family': 'sans-serif',
            'font.sans-serif': ['SimHei', 'Noto Sans CJK SC', 'DejaVu Sans'],
            'axes.unicode_minus': False,
        }
    )

# 数据目录
DATA_DIR = "/root/.openclaw/workspace/data/raw"
STOCK_DAILY_DIR = os.path.join(DATA_DIR, "stock_daily")


def load_stock_data(stock_code, start_date=None, end_date=None):
    """
    加载股票日线数据
    
    Args:
        stock_code: 股票代码，如 000001.SZ
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD
    
    Returns:
        DataFrame with date as index
    """
    csv_path = os.path.join(STOCK_DAILY_DIR, f"{stock_code}.csv")
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"股票数据文件不存在: {csv_path}")
    
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    df = df.set_index('trade_date')
    df = df.sort_index()
    
    # 日期过滤
    if start_date:
        start_dt = pd.to_datetime(start_date, format='%Y%m%d')
        df = df[df.index >= start_dt]
    if end_date:
        end_dt = pd.to_datetime(end_date, format='%Y%m%d')
        df = df[df.index <= end_dt]
    
    # 重命名列名（mplfinance需要volume而不是vol）
    if 'vol' in df.columns:
        df = df.rename(columns={'vol': 'volume'})
    
    return df
    
    return df


def parse_annotations(config):
    """
    解析标注配置，返回mplfinance格式的标注列表
    """
    annotations = []
    
    if not config or 'annotations' not in config:
        return annotations
    
    for ann in config['annotations']:
        ann_type = ann.get('type')
        color = ann.get('color', 'red')
        alpha = ann.get('alpha', 0.2)
        
        if ann_type == 'hline':
            # 水平线 - 价格位
            price = ann.get('price')
            if isinstance(price, str) and price.startswith('M'):
                # 移动平均线，如 M5 代表MA5
                continue  # 暂时不支持动态计算
            annotations.append(
                mpf.make_addplot(
                    [float(price)] * 1000,  # 足够长的数据
                    color=color,
                    linestyle='--',
                    label=ann.get('label', '')
                )
            )
        
        elif ann_type == 'vline':
            # 垂直线 - 日期
            date_str = ann.get('date')
            if not date_str:
                continue
            # mplfinance 使用 panel_ratios 控制，需要特殊处理
            # 这里用另一种方式：在数据上添加标记
        
        elif ann_type == 'text':
            # 文字标注 - 在副图上添加
            pass  # 需要特殊处理
        
        elif ann_type == 'rect':
            # 矩形区域 - 需要特殊处理
            pass
    
    return annotations


def calculate_ma(df, periods):
    """计算移动平均线"""
    result = {}
    for period in periods:
        if f'MA{period}' in df.columns:
            result[f'MA{period}'] = df[f'MA{period}']
        elif f'ma{period}' in df.columns:
            result[f'MA{period}'] = df[f'ma{period}']
        else:
            result[f'MA{period}'] = df['close'].rolling(window=period).mean()
    return result


def find_nearest_trade_date(df, target_date):
    """
    找到最近的交易日
    """
    if target_date in df.index:
        return target_date
    
    # 向前找（找不超过目标日期的最近交易日）
    nearby_before = df.index[df.index <= target_date]
    if len(nearby_before) > 0:
        return nearby_before[-1]
    
    # 向后找
    nearby_after = df.index[df.index >= target_date]
    if len(nearby_after) > 0:
        return nearby_after[0]
    
    # 如果都找不到，返回第一个
    return df.index[0]


def plot_kline(stock_code, start_date=None, end_date=None, 
               output_path=None, config=None, title=None,
               indicators=None, style='yahoo'):
    """
    绘制K线图
    
    Args:
        stock_code: 股票代码
        start_date: 开始日期
        end_date: 结束日期
        output_path: 输出路径
        config: 标注配置dict或json文件路径
        title: 图表标题
        indicators: 指标列表，如 ['MA5', 'MA10', 'MACD']
        style: 样式
    """
    # 转换为支持中文的样式
    custom_style = get_custom_style(style)
    
    # 加载数据
    df = load_stock_data(stock_code, start_date, end_date)
    
    # 如果数据太少，返回错误
    if len(df) < 5:
        raise ValueError(f"数据不足，需要至少5条数据，当前只有{len(df)}条")
    
    # 解析配置
    if isinstance(config, str):
        # 是文件路径
        with open(config, 'r', encoding='utf-8') as f:
            config = json.load(f)
    
    # 导入指标模块
    import kline_plot.indicators as ind_module
    
    # 默认指标列表
    indicator_list = ['MA5', 'MA10', 'MA20']
    if indicators is None:
        indicators = indicator_list
    if isinstance(indicators, str):
        indicator_list = [i.strip() for i in indicators.split(',')]
    else:
        indicator_list = indicators
    
    # 如果配置中有indicators，合并
    if config and 'indicators' in config:
        for ind in config['indicators']:
            if ind not in indicator_list:
                indicator_list.append(ind)
    
    # 准备addplots列表
    apds = []
    panel_ratios = (4,)  # 主图比例
    
    # 添加指标到副图
    panel_count = 0
    for ind in indicator_list:
        ind_upper = ind.upper()
        
        if ind_upper in ['MA5', 'MA10', 'MA20', 'MA30', 'MA60', 'MA120', 'MA250']:
            # 均线 - 主图
            period = int(ind_upper[2:])
            ma = df['close'].rolling(window=period).mean()
            apds.append(mpf.make_addplot(ma, color='blue' if period==5 else 'orange' if period==10 else 'green', label=ind))
        
        elif ind_upper == 'BOLL' or ind_upper == 'BBANDS':
            # 布林带
            boll = ind_module.bollinger_bands(df)
            apds.append(mpf.make_addplot(boll['upper'], color='purple', linestyle='--'))
            apds.append(mpf.make_addplot(boll['lower'], color='purple', linestyle='--'))
            apds.append(mpf.make_addplot(boll['middle'], color='purple', label='BOLL'))
        
        elif ind_upper == 'MACD':
            # MACD - 副图 (panel 2，因为panel 1是成交量)
            macd = ind_module.macd(df)
            apds.append(mpf.make_addplot(macd['macd'], panel=2, color='fuchsia', secondary_y=False, label='MACD'))
            apds.append(mpf.make_addplot(macd['signal'], panel=2, color='orange', secondary_y=False, label='Signal'))
            apds.append(mpf.make_addplot(macd['hist'], panel=2, type='bar', color='gray', secondary_y=False))
            panel_count += 1
        
        elif ind_upper == 'RSI':
            # RSI - 副图
            rsi = ind_module.rsi(df)
            apds.append(mpf.make_addplot(rsi, panel=2+panel_count, color='purple', label='RSI'))
            panel_count += 1
        
        elif ind_upper == 'KDJ':
            # KDJ - 副图
            kdj = ind_module.kdj(df)
            apds.append(mpf.make_addplot(kdj['k'], panel=2+panel_count, color='r', label='K'))
            apds.append(mpf.make_addplot(kdj['d'], panel=2+panel_count, color='g', label='D'))
            apds.append(mpf.make_addplot(kdj['j'], panel=2+panel_count, color='b', label='J'))
            panel_count += 1
    
    # 调整面板比例 (主图 + 成交量 + 指标副图)
    if panel_count > 0:
        # panel 0: K线, panel 1: 成交量, panel 2+: 指标
        panel_ratios = (3, 1) + (1,) * panel_count
    else:
        panel_ratios = (4, 1)  # 主图 + 成交量
    
    # 处理标注
    # 保存需要后处理的标注（vline, text, rect, line需要在fig生成后处理）
    hline_annotations = []
    post_annotations = []  # vline, text, rect, line
    
    if config and 'annotations' in config:
        annotations = config['annotations']
        
        # 水平线 (hline) - 可以直接用addplot
        for ann in annotations:
            ann_type = ann.get('type')
            
            if ann_type == 'hline':
                price = ann.get('price')
                if isinstance(price, str):
                    continue
                # 扩展到足够长度
                hline_data = pd.Series([float(price)] * len(df), index=df.index)
                color = ann.get('color', 'red')
                apds.append(mpf.make_addplot(hline_data, color=color, linestyle='--', label=ann.get('label', '')))
            
            else:
                # 其他类型需要后处理
                post_annotations.append(ann)
    
    # 确定标题
    if not title:
        # 尝试从stock_list.csv获取中文名称
        stock_name = stock_code
        stock_list_path = "/root/.openclaw/workspace/data/raw/stock_list.csv"
        if os.path.exists(stock_list_path):
            try:
                stock_df = pd.read_csv(stock_list_path, encoding='utf-8-sig', header=None)
                # 查找匹配的股票代码
                for _, row in stock_df.iterrows():
                    if str(row[0]) == stock_code or str(row[1]) == stock_code:
                        stock_name = f"{row[2]} ({stock_code})"  # 中文名称 + 代码
                        break
            except:
                pass
        title = f"{stock_name} K-Line Chart"
    
    # 根据数据量动态调整图片宽度和高度
    n = len(df)
    figsize_width = max(20, n * 0.08)
    # 高度根据面板数量调整：有MACD需要更多空间
    figsize_height = 10
    if panel_count > 0:
        figsize_height = 10 + panel_count * 1.5
    
    # 绘制并获取fig对象（不显示默认标题，用add_text在图内显示）
    fig, axes = mpf.plot(
        df,
        type='candle',
        style=custom_style,
        ylabel='价格',
        ylabel_lower='成交量',
        addplot=apds,
        panel_ratios=panel_ratios,
        figsize=(figsize_width, figsize_height),
        returnfig=True,
        tight_layout=False,
        show_nontrading=False,
        volume=True
    )
    
    # 在图表内部添加标题
    ax = axes[0]
    ax.text(0.05, 0.98, title, transform=ax.transAxes, fontsize=12, 
            verticalalignment='top', fontweight='bold',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    # 后处理标注（vline, text, rect, line）
    if post_annotations and len(axes) > 0:
        ax = axes[0]  # 主图
        
        # 设置中文字体
        from matplotlib.font_manager import FontProperties
        font_path = '/usr/share/fonts/truetype/SimHei.ttf'
        try:
            chinese_font = FontProperties(fname=font_path)
        except:
            chinese_font = None
        
        # 获取y轴范围（用于确定标注位置）
        y_min, y_max = ax.get_ylim()
        
        for ann in post_annotations:
            ann_type = ann.get('type')
            
            if ann_type == 'vline':
                # 垂直线
                date_str = ann.get('date')
                label = ann.get('label', '')
                if date_str:
                    try:
                        # 支持 YYYYMMDD 和 YYYY-MM-DD 两种格式
                        try:
                            date_idx = pd.to_datetime(date_str, format='%Y%m%d')
                        except:
                            date_idx = pd.to_datetime(date_str)
                        # 使用最近交易日
                        date_idx = find_nearest_trade_date(df, date_idx)
                        x_pos = df.index.get_loc(date_idx)
                        color = ann.get('color', 'blue')
                        ax.axvline(x=x_pos, color=color, linestyle='--', linewidth=1)
                        
                        # 添加label文字
                        if label:
                            ax.text(x_pos, y_max * 0.98, label, color=color, fontsize=9,
                                   ha='center', va='top', rotation=0,
                                   fontproperties=chinese_font)
                    except:
                        pass
            
            elif ann_type == 'text':
                # 文字标注
                date_str = ann.get('date')
                price = ann.get('price')
                text = ann.get('text', '')
                color = ann.get('color', 'red')
                
                if date_str and price:
                    try:
                        # 支持 YYYYMMDD 和 YYYY-MM-DD 两种格式
                        try:
                            date_idx = pd.to_datetime(date_str, format='%Y%m%d')
                        except:
                            date_idx = pd.to_datetime(date_str)
                        # 使用最近交易日
                        date_idx = find_nearest_trade_date(df, date_idx)
                        x_pos = df.index.get_loc(date_idx)
                        ax.annotate(text, xy=(x_pos, float(price)), 
                                   color=color, fontsize=10, 
                                   ha='center', va='bottom',
                                   fontproperties=chinese_font)
                    except:
                        pass
            
            elif ann_type == 'rect':
                # 矩形区域 - 修复：使用fill_between限制y轴范围
                start_date = ann.get('start')
                end_date = ann.get('end')
                y1 = ann.get('y1')
                y2 = ann.get('y2')
                alpha = ann.get('alpha', 0.2)
                color = ann.get('color', 'yellow')
                
                if start_date and end_date:
                    try:
                        # 支持 YYYYMMDD 和 YYYY-MM-DD 两种格式
                        try:
                            start_idx = pd.to_datetime(start_date, format='%Y%m%d')
                        except:
                            start_idx = pd.to_datetime(start_date)
                        try:
                            end_idx = pd.to_datetime(end_date, format='%Y%m%d')
                        except:
                            end_idx = pd.to_datetime(end_date)
                        
                        # 使用最近交易日
                        start_idx = find_nearest_trade_date(df, start_idx)
                        end_idx = find_nearest_trade_date(df, end_idx)
                        
                        x1 = df.index.get_loc(start_idx)
                        x2 = df.index.get_loc(end_idx)
                        
                        # 如果指定了y1和y2，使用fill_between限制范围
                        if y1 is not None and y2 is not None:
                            # 创建x轴数据
                            x_data = np.linspace(x1, x2, int(x2 - x1) + 1)
                            ax.fill_between(x_data, y1, y2, alpha=alpha, color=color)
                        else:
                            # 否则使用axvspan填充整个y轴
                            ax.axvspan(x1, x2, alpha=alpha, color=color)
                    except:
                        pass
            
            elif ann_type == 'line':
                # 折线 - 修复：使用最近交易日
                points = ann.get('points', [])
                color = ann.get('color', 'purple')
                label = ann.get('label', '')
                style = ann.get('style', 'solid')
                width = ann.get('width', 1.5)

                # 解析线型
                if style == 'dotted':
                    linestyle = '--'
                elif style == 'dashed':
                    linestyle = '-.'
                else:  # solid
                    linestyle = '-'

                if len(points) >= 2:
                    valid_points = []
                    for pt in points:
                        if len(pt) >= 2:
                            try:
                                date_str = pt[0]
                                price = float(pt[1])
                                # 支持 YYYYMMDD 和 YYYY-MM-DD 两种格式
                                try:
                                    date_idx = pd.to_datetime(date_str, format='%Y%m%d')
                                except:
                                    date_idx = pd.to_datetime(date_str)
                                # 使用最近交易日
                                date_idx = find_nearest_trade_date(df, date_idx)
                                x = df.index.get_loc(date_idx)
                                valid_points.append((x, price))
                            except:
                                pass

                    if len(valid_points) >= 2:
                        xs, ys = zip(*valid_points)
                        ax.plot(xs, ys, color=color, linewidth=width, linestyle=linestyle, label=label)
    
    # 保存
    if output_path:
        # 确保目录存在
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        fig.savefig(output_path, bbox_inches='tight', facecolor='white', dpi=150)
        plt.close(fig)
        return output_path
    
    return None
