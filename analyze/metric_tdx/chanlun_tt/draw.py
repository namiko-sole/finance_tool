"""
绘图函数库 - 输出JSON格式
缠论TT指标专用
"""

import json
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np


# 颜色映射表
COLOR_MAP = {
    'COLOR3300FF': '#3300FF',
    'COLORLIMAGENTA': '#FF00FF',
    'COLORLIGREEN': '#00FF00',
    'COLORRED': '#FF0000',
    'COLORGREEN': '#00FF00',
    'COLORYELLOW': '#FFFF00',
    'COLORFF6600': '#FF6600',
    'COLORFF0000': '#FF0000',
    'COLORFF0066': '#FF0066',
    'COLORCC66FF': '#CC66FF',
    'COLOR00CCFF': '#00CCFF',
    'COLORLICYAN': '#00FFFF',
    'COLORWHITE': '#FFFFFF',
    'COLORBLACK': '#000000',
    'COLORGRAY': '#808080',
}


def parse_color(color_str: str) -> str:
    """解析颜色字符串"""
    if pd.isna(color_str) or color_str is None:
        return '#000000'
    color_str = str(color_str).upper()
    return COLOR_MAP.get(color_str, color_str.lower())


def get_date_from_index(df: pd.DataFrame, idx: int) -> str:
    """根据索引获取日期，格式为YYYYMMDD"""
    if 'date' in df.columns:
        date_val = str(df['date'].iloc[idx])
        # 转换为YYYYMMDD格式
        date_val = date_val.replace('-', '').replace('/', '')
        return date_val
    return str(idx)


def get_price_from_series(series: pd.Series, idx: int) -> float:
    """获取指定索引的价格"""
    if idx < len(series):
        val = series.iloc[idx]
        if pd.notna(val) and val != np.nan:
            return float(val)
    return 0.0


def _to_series(data, length: Optional[int] = None, dtype=None) -> pd.Series:
    """将标量/ndarray/Series统一为RangeIndex的Series，便于按位置(.iloc)访问。"""
    if isinstance(data, pd.Series):
        series = data.reset_index(drop=True)
    else:
        arr = np.asarray(data)
        if arr.ndim == 0:
            if length is None:
                length = 1
            arr = np.full(length, arr.item())
        series = pd.Series(arr)

    if dtype is not None:
        series = series.astype(dtype)
    return series


class AnnotationBuilder:
    """注解构建器"""

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.annotations: List[Dict[str, Any]] = []
        self.indicators: List[Dict[str, Any]] = []

    def add_hline(self, price: float, label: str = None, color: str = 'red'):
        """添加水平线"""
        self.annotations.append({
            'type': 'hline',
            'price': float(price),
            'label': label,
            'color': color
        })

    def add_vline(self, date: str, label: str = None, color: str = 'blue'):
        """添加垂直线"""
        self.annotations.append({
            'type': 'vline',
            'date': date,
            'label': label,
            'color': color
        })

    def add_rect(self, start_idx: int, end_idx: int, y1: float, y2: float,
                 alpha: float = 0.2, color: str = 'yellow', label: str = None,
                 width: int = 1, empty: int = 0):
        """添加矩形区域

        Args:
            start_idx: 起始索引
            end_idx: 结束索引
            y1: 起始价格
            y2: 结束价格
            alpha: 透明度
            color: 颜色
            label: 标签
            width: 宽度 (通达信WIDTH参数)
            empty: 空心柱标识 (0=实心, 1=空心, -1=虚线)
        """
        start_date = get_date_from_index(self.df, start_idx)
        end_date = get_date_from_index(self.df, end_idx)
        self.annotations.append({
            'type': 'rect',
            'start': start_date,
            'end': end_date,
            'y1': float(y1),
            'y2': float(y2),
            'alpha': alpha,
            'color': color,
            'label': label,
            'width': width,
            'empty': empty
        })

    def add_text(self, idx: int, price: float, text: str, color: str = 'red'):
        """添加文本标注"""
        date = get_date_from_index(self.df, idx)
        self.annotations.append({
            'type': 'text',
            'date': date,
            'price': float(price),
            'text': text,
            'color': color
        })

    def add_circle(self, idx: int, price: float, color: str = 'red', label: str = None):
        """添加圆点标注"""
        date = get_date_from_index(self.df, idx)
        self.annotations.append({
            'type': 'circle',
            'date': date,
            'price': float(price),
            'color': color,
            'label': label
        })

    def add_line(self, points: List, color: str = 'purple', label: str = None,
                style: str = 'solid', width: int = 1):
        """添加线段

        Args:
            points: 线段点列表 [[date, price], ...]
            color: 线条颜色
            label: 标签
            style: 线型 'solid'(实线) 或 'dotted'(虚线)
            width: 线宽
        """
        formatted_points = []
        for p in points:
            if len(p) >= 2:
                date = str(p[0]) if isinstance(p[0], (int, str)) else get_date_from_index(self.df, p[0])
                price = float(p[1]) if p[1] is not None else 0.0
                formatted_points.append([date, price])
        # 按日期排序
        formatted_points.sort(key=lambda x: x[0])
        self.annotations.append({
            'type': 'line',
            'points': formatted_points,
            'color': color,
            'label': label,
            'style': style,
            'width': width
        })

    def to_json(self) -> str:
        """输出JSON"""
        result = {
            'indicators': self.indicators,
            'annotations': self.annotations
        }
        return json.dumps(result, ensure_ascii=False, indent=2)


def STICKLINE(cond: pd.Series, price1, price2, width: int, empty: int,
              builder: AnnotationBuilder, color: str = 'COLOR3300FF') -> pd.Series:
    """STICKLINE - 柱状线

    通达信语法: STICKLINE(COND,PRICE1,PRICE2,WIDTH,EMPTY)
    - WIDTH: 宽度 (10为标准间距)
    - EMPTY: 0=实心柱, 1=空心柱, -1=虚线空心柱
    """
    cond_s = _to_series(cond, dtype=bool).fillna(False)
    result = pd.Series(np.nan, index=cond_s.index)
    color_hex = parse_color(color)
    price1_s = _to_series(price1, length=len(cond_s))
    price2_s = _to_series(price2, length=len(cond_s))

    # 计算透明度: width越大越不透明
    alpha = min(1.0, width / 10.0) if width > 0 else 0.5

    for i in range(len(cond_s)):
        if bool(cond_s.iloc[i]):
            result.iloc[i] = price1_s.iloc[i]
            # 添加矩形区域表示柱子
            builder.add_rect(i, i + 1, price2_s.iloc[i], price1_s.iloc[i],
                           alpha=alpha, color=color_hex, width=width, empty=empty)

    return result


def DRAWLINE(cond1: pd.Series, price1, cond2: pd.Series, price2,
             builder: AnnotationBuilder, color: str = 'COLOR3300FF',
             style: str = 'solid', width: int = 1, label: str = None, merge: bool = False) -> pd.Series:
    """DRAWLINE - 画线

    通达信语法: DRAWLINE(COND1,PRICE1,COND2,PRICE2,EXTEND)
    - 从满足COND1的点画到满足COND2的点
    - 按时间顺序成对连接

    缠论用法: 连接所有同类信号点(收集所有点后排序画一条线)

    Args:
        cond1: 条件1
        price1: 价格1
        cond2: 条件2
        price2: 价格2
        builder: 注解构建器
        color: 颜色
        style: 线型 'solid' 或 'dotted'
        width: 线宽
        merge: 是否合并连续相同条件，只保留最后一个点
    """
    cond1_s = _to_series(cond1, dtype=bool).fillna(False)
    cond2_s = _to_series(cond2, dtype=bool).fillna(False)
    price1_s = _to_series(price1, length=len(cond1_s))
    price2_s = _to_series(price2, length=len(cond2_s))

    result = pd.Series(np.nan, index=cond1_s.index)
    color_hex = parse_color(color)

    # 获取所有满足条件的索引
    idx1 = [i for i in range(len(cond1_s)) if bool(cond1_s.iloc[i])]
    idx2 = [i for i in range(len(cond2_s)) if bool(cond2_s.iloc[i])]

    # 收集所有点后按时间排序画一条线(缠论用法)
    points = []

    for i in idx1:
        date = get_date_from_index(builder.df, i)
        p = price1_s.iloc[i]
        if pd.notna(p) and p != np.nan:
            points.append([date, float(p), 1])  # 用第三个元素标记是cond1还是cond2

    for i in idx2:
        date = get_date_from_index(builder.df, i)
        p = price2_s.iloc[i]
        if pd.notna(p) and p != np.nan:
            points.append([date, float(p), 2])  # 用第三个元素标记是cond1还是cond2

    # 按日期排序
    if len(points) >= 2:
        points.sort(key=lambda x: x[0])

        # 如果merge=True，合并连续相同的条件
        if merge:
            merged_points = []
            for p in points:
                if not merged_points or merged_points[-1][2] != p[2]:
                    merged_points.append(p)
                # 如果条件相同，替换最后一个点（即保留连续相同条件的最后一个）
                else:
                    merged_points[-1] = p
            points = merged_points

        # 提取日期和价格用于画线
        line_points = [[p[0], p[1]] for p in points]

        if len(line_points) >= 2:
            builder.add_line(line_points, color=color_hex, style=style, width=width, label=label)

    return result


def DRAWTEXT(cond: pd.Series, price, text: str,
             builder: AnnotationBuilder, color: str = 'COLORRED') -> pd.Series:
    """DRAWTEXT - 文本标注

    通达信语法: DRAWTEXT(COND,PRICE,TEXT)
    - 当COND条件满足时,在PRICE位置显示TEXT文字
    """
    cond_s = _to_series(cond, dtype=bool).fillna(False)
    result = pd.Series(np.nan, index=cond_s.index)
    color_hex = parse_color(color)

    price_s = _to_series(price, length=len(cond_s))

    for i in range(len(cond_s)):
        if bool(cond_s.iloc[i]):
            result.iloc[i] = price_s.iloc[i]
            builder.add_text(i, price_s.iloc[i], text, color=color_hex)

    return result


def CIRCLEDOT(series: pd.Series, builder: AnnotationBuilder,
             color: str = 'COLORRED') -> pd.Series:
    """CIRCLEDOT - 圆点标记

    在series每个有效值位置画圆点标注
    """
    series_s = _to_series(series)
    result = series_s.copy()
    color_hex = parse_color(color)

    for i in range(len(series_s)):
        val = series_s.iloc[i]
        if pd.notna(val) and val != np.nan:
            builder.add_circle(i, float(val), color=color_hex)

    return result


def DOTLINE(series: pd.Series) -> pd.Series:
    """DOTLINE - 虚线标记

    返回虚线样式标识，供DRAWLINE等函数使用
    """
    # 返回带有虚线样式信息的series
    result = series.copy()
    result.attrs['style'] = 'dotted'
    return result


def LINETHICK2(series: pd.Series) -> pd.Series:
    """LINETHICK2 - 线宽2

    返回线宽2标识，供DRAWLINE等函数使用
    """
    result = series.copy()
    result.attrs['width'] = 2
    return result


def DRAWNULL() -> np.nan:
    """DRAWNULL - 返回空值

    通达信中表示无效/空值数据
    """
    return np.nan
