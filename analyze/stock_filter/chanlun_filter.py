"""
缠论买点过滤器

从股票列表中筛选出最近N天缠论买点与BULAO金叉共振的个股，并添加MACD过滤

功能：
1. 从 stock_list.csv 获取所有个股
2. 通过 DataFetcher 获取每个个股的数据
3. 使用 chanlun_tt/main.py 的 get_buy_sell_signals 获取缠论买卖信号
4. 使用 bulao/main.py 的 get_buy_sell_signals 获取BULAO金叉信号
5. 计算MACD并进行零下段背离过滤：
   - 如果MACD > 0，直接通过
   - 如果MACD < 0，检查当前零下段最小值是否大于上一个零下段最小值（背离改善）
6. 筛选最近N天同时出现缠论买点、BULAO金叉且MACD过滤通过的个股并输出
"""

import sys
import os
import argparse
import importlib.util
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Set

# 添加路径
sys.path.insert(0, '/root/.openclaw/workspace/finance_tool/fetchers')
sys.path.insert(0, '/root/.openclaw/workspace/finance_tool/analyze/metric_tdx/chanlun_tt')
sys.path.insert(0, '/root/.openclaw/workspace/finance_tool/analyze/metric_tdx')

# 导入数据获取器
from data_fetcher import DataFetcher

# 导入缠论分析函数
from main import get_buy_sell_signals as get_chanlun_buy_sell_signals


def load_bulao_signal_func():
    """动态加载BULAO模块，避免与chanlun的main.py同名冲突。"""
    bulao_path = '/root/.openclaw/workspace/finance_tool/analyze/metric_tdx/bulao/main.py'
    spec = importlib.util.spec_from_file_location('bulao_main', bulao_path)
    if spec is None or spec.loader is None:
        raise ImportError(f'无法加载BULAO模块: {bulao_path}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.get_buy_sell_signals


get_bulao_buy_sell_signals = load_bulao_signal_func()


def add_virtual_day_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    在数据末尾添加1天虚拟数据

    虚拟数据日期为数据最新日期+1天，OCLH都为最新日期的close

    Args:
        df: 原始K线数据，必须包含 date, open, close, low, high 列

    Returns:
        添加虚拟数据后的DataFrame
    """
    if df is None or df.empty:
        return df

    df = df.copy()

    # 获取最后一行数据
    last_row = df.iloc[-1]
    last_date = pd.to_datetime(last_row['date'])
    last_close = last_row['close']

    # 计算虚拟日期（最新日期+1天）
    virtual_date = (last_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')

    # 创建虚拟数据行
    virtual_row = {
        'date': virtual_date,
        'open': last_close,
        'close': last_close,
        'low': last_close,
        'high': last_close
    }

    # 添加 volume 列（如果存在）
    if 'volume' in df.columns:
        virtual_row['volume'] = 0

    # 使用 pd.concat 添加新行
    virtual_df = pd.DataFrame([virtual_row])
    df = pd.concat([df, virtual_df], ignore_index=True)

    return df


def check_macd_filter(macd_hist_series: pd.Series, current_index: int) -> bool:
    """
    检查MACD是否满足过滤条件

    Args:
        macd_hist_series: MACD柱状图值序列（按时间正序）
        current_index: 当前日期在序列中的索引

    Returns:
        True表示通过（保留），False表示不通过（过滤）

    过滤逻辑：
    1. 如果今日MACD > 0，直接通过
    2. 如果今日MACD < 0，则判断当前零下段最小值是否大于上一个零下段最小值
       - 如果大于，说明背离改善，通过
       - 如果小于或等于，说明仍在恶化，过滤掉
    """
    if current_index >= len(macd_hist_series):
        return False

    current_macd = macd_hist_series.iloc[current_index]

    # 今日MACD > 0，直接通过
    if current_macd > 0:
        return True

    # 今日MACD < 0，需要与上一个零下段最小值比较
    macd_values = macd_hist_series.iloc[:current_index + 1].values

    # 按零为分界线分段
    segments = []
    current_segment = []

    for val in macd_values:
        if not current_segment:
            current_segment.append(val)
        else:
            # 同号则加入当前段，异号则新开一段
            last_val = current_segment[-1]
            same_sign = (last_val > 0 and val > 0) or (last_val < 0 and val < 0) or (last_val == 0 and val == 0)
            if same_sign:
                current_segment.append(val)
            else:
                segments.append(current_segment)
                current_segment = [val]

    if current_segment:
        segments.append(current_segment)

    # 找出当前段
    current_segment = segments[-1]
    current_is_negative = current_segment[-1] < 0

    if not current_is_negative:
        return True  # 当前段是零上段，通过

    # 当前段是零下段，获取其最小值
    current_min = min(current_segment)

    # 查找上一个零下段
    prev_negative_min = None
    for seg in reversed(segments[:-1]):
        if seg[-1] < 0:  # 零下段
            prev_negative_min = min(seg)
            break

    # 如果没有上一个零下段，通过
    if prev_negative_min is None:
        return True

    # 比较两个零下段的最小值：当前段最小值 > 上一个零下段最小值，则通过
    return current_min > prev_negative_min


def _is_st_stock(name: str) -> bool:
    """判断是否为ST类股票。"""
    if not isinstance(name, str):
        return False
    normalized = name.upper().replace(' ', '')
    return 'ST' in normalized


def _is_main_board(ts_code: str) -> bool:
    """
    判断是否为A股主板股票。

    规则:
    - SH主板: 600/601/603/605
    - SZ主板: 000/001/002/003
    - 其他(科创板688、创业板300/301、北交所BJ等)视为非主板
    """
    if not isinstance(ts_code, str) or '.' not in ts_code:
        return False

    code, market = ts_code.split('.', 1)
    if market == 'SH':
        return code.startswith(('600', '601', '603', '605'))
    if market == 'SZ':
        return code.startswith(('000', '001', '002', '003'))
    return False


def filter_stock_pool_mainboard_non_st(stock_list: pd.DataFrame) -> pd.DataFrame:
    """过滤股票池，仅保留非ST的主板个股。"""
    required_cols = {'ts_code', 'name'}
    missing = required_cols - set(stock_list.columns)
    if missing:
        raise ValueError(f"stock_list 缺少必要列: {sorted(missing)}")

    mask_mainboard = stock_list['ts_code'].apply(_is_main_board)
    mask_non_st = ~stock_list['name'].fillna('').apply(_is_st_stock)
    return stock_list[mask_mainboard & mask_non_st].copy()


def get_stock_list(csv_path: str = '/root/.openclaw/workspace/data/raw/stock_list.csv') -> pd.DataFrame:
    """
    读取股票列表CSV文件
    
    Args:
        csv_path: 股票列表CSV文件路径
        
    Returns:
        DataFrame，包含股票信息
    """
    df = pd.read_csv(csv_path)
    return df


def get_stock_personality(csv_path: str = '/root/.openclaw/workspace/data/raw/stock_personality.csv') -> pd.DataFrame:
    """读取股性CSV文件。"""
    df = pd.read_csv(csv_path)
    return df


def filter_by_limit_up_count(stock_list: pd.DataFrame, personality_df: pd.DataFrame, min_count: int = 3) -> pd.DataFrame:
    """按近一年涨停次数过滤股票池。"""
    required_stock_cols = {'ts_code'}
    required_personality_cols = {'ts_code', 'limit_up_count_1y'}

    missing_stock_cols = required_stock_cols - set(stock_list.columns)
    if missing_stock_cols:
        raise ValueError(f"stock_list 缺少必要列: {sorted(missing_stock_cols)}")

    missing_personality_cols = required_personality_cols - set(personality_df.columns)
    if missing_personality_cols:
        raise ValueError(f"stock_personality 缺少必要列: {sorted(missing_personality_cols)}")

    personality_subset = personality_df[['ts_code', 'limit_up_count_1y']].copy()
    personality_subset['limit_up_count_1y'] = pd.to_numeric(
        personality_subset['limit_up_count_1y'],
        errors='coerce'
    ).fillna(0)

    merged = stock_list.merge(personality_subset, on='ts_code', how='left')
    merged['limit_up_count_1y'] = merged['limit_up_count_1y'].fillna(0)
    return merged[merged['limit_up_count_1y'] >= min_count].copy()


def _collect_recent_buy_dates(signals: List[Dict], start_dt, end_dt) -> Set[str]:
    """收集日期区间内的买入信号日期集合（YYYY-MM-DD）。"""
    dates: Set[str] = set()
    for signal in signals:
        signal_date = signal.get('date', '')
        signal_type = signal.get('signal', '')
        if signal_type != '买入':
            continue

        try:
            signal_dt = datetime.strptime(signal_date, '%Y-%m-%d').date()
        except (TypeError, ValueError):
            continue

        if start_dt <= signal_dt <= end_dt:
            dates.add(signal_date)
    return dates


def filter_stocks_with_dual_buy_signal_recent_days(
    stock_list: pd.DataFrame,
    data_fetcher: DataFetcher,
    lookback_days: int = 60,
    recent_days: int = 3,
    add_virtual_day: bool = False
) -> List[Dict]:
    """
    筛选最近N天同时出现缠论买点、BULAO金叉且MACD过滤通过的个股

    MACD过滤规则：
    - 如果当日MACD > 0，直接通过
    - 如果当日MACD < 0，检查当前零下段最小值是否大于上一个零下段最小值
      （即零下段背离改善，通过；否则过滤掉）

    Args:
        stock_list: 股票列表 DataFrame
        data_fetcher: DataFetcher 实例
        lookback_days: 回溯天数，用于获取足够的历史数据计算缠论和MACD
        recent_days: 最近N天（含今天）
        add_virtual_day: 是否添加虚拟交易日数据（最新日期+1天，OHLC=最新close）

    Returns:
        最近N天共振个股列表，每个元素包含股票代码、名称和信号信息
    """
    if recent_days < 1:
        raise ValueError("recent_days 必须大于等于 1")

    today_dt = datetime.now().date()
    start_recent_dt = today_dt - timedelta(days=recent_days - 1)
    today = today_dt.strftime('%Y-%m-%d')
    start_recent = start_recent_dt.strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y%m%d')
    
    stocks_with_buy_signal = []
    total = len(stock_list)
    
    print(f"开始筛选最近{recent_days}天({start_recent} ~ {today})缠论买点+BULAO金叉共振个股...")
    print(f"总股票数: {total}")
    print(f"回溯天数: {lookback_days}")
    if add_virtual_day:
        print("虚拟数据模式: 已启用（将在最新日期+1天添加虚拟交易日）")
    print("-" * 60)

    for processed, (_, row) in enumerate(stock_list.iterrows(), start=1):
        ts_code = row['ts_code']
        symbol = row['symbol']
        name = row.get('name', '')

        if processed % 100 == 0 or processed == total:
            print(f"已处理 {processed}/{total} 只股票...")

        try:
            # 获取K线数据（用于添加虚拟数据）
            base_df = None
            if add_virtual_day:
                base_df = data_fetcher.get_stock_daily(
                    symbol=ts_code,
                    start_date=start_date,
                    end_date=None
                )
                if base_df is not None and not base_df.empty:
                    base_df = add_virtual_day_df(base_df)

            # 获取缠论买卖信号（传入处理后的df）
            chanlun_result = get_chanlun_buy_sell_signals(
                stock_code=ts_code,
                start_date=start_date,
                end_date=None,
                data_fetcher=data_fetcher,
                df=base_df
            )

            if 'error' in chanlun_result:
                continue

            # 获取BULAO买卖信号（金叉对应 signal=买入，传入处理后的df）
            bulao_result = get_bulao_buy_sell_signals(
                stock_code=ts_code,
                start_date=start_date,
                end_date=None,
                data_fetcher=data_fetcher,
                df=base_df
            )

            if 'error' in bulao_result:
                continue

            chanlun_signals = chanlun_result.get('signals', [])
            bulao_signals = bulao_result.get('signals', [])

            chanlun_buy_dates = _collect_recent_buy_dates(chanlun_signals, start_recent_dt, today_dt)
            bulao_buy_dates = _collect_recent_buy_dates(bulao_signals, start_recent_dt, today_dt)

            overlap_dates = sorted(chanlun_buy_dates & bulao_buy_dates)
            if overlap_dates:
                matched_date = overlap_dates[-1]

                chanlun_price = 0.0
                chanlun_jdbl = -1
                for signal in chanlun_signals:
                    if signal.get('date') == matched_date and signal.get('signal') == '买入':
                        chanlun_price = signal.get('price', 0)
                        chanlun_jdbl = signal.get('jdbl', -1)
                        break

                bulao_price = 0.0
                for signal in bulao_signals:
                    if signal.get('date') == matched_date and signal.get('signal') == '买入':
                        bulao_price = signal.get('price', 0)
                        break

                # MACD过滤检查 - 复用缠论分析中已获取的数据
                try:
                    # 从缠论结果中复用K线数据
                    hist_df = chanlun_result.get('df')

                    if hist_df is None or hist_df.empty:
                        print(f"MACD过滤: {ts_code} {name} - 无历史数据，跳过")
                        continue

                    # 确保数据按日期排序（data_fetcher返回的列名是'date'）
                    if 'date' in hist_df.columns:
                        hist_df = hist_df.sort_values('date').reset_index(drop=True)

                    # 导入MACD计算函数
                    from MyTT import MACD

                    # 计算MACD
                    _, _, macd_hist = MACD(hist_df['close'].values)

                    # 找到signal_date对应的索引（matched_date格式: YYYY-MM-DD，hist_df['date']也是相同格式）
                    signal_idx_list = hist_df[hist_df['date'] == matched_date].index.tolist()

                    if not signal_idx_list:
                        print(f"MACD过滤: {ts_code} {name} - 未找到信号日期，跳过")
                        continue

                    signal_idx = signal_idx_list[0]
                    macd_series = pd.Series(macd_hist)

                    # 检查MACD过滤条件
                    if not check_macd_filter(macd_series, signal_idx):
                        current_macd = float(macd_series.iloc[signal_idx]) if signal_idx < len(macd_series) else 0.0
                        print(f"MACD过滤: {ts_code} {name} {matched_date} (MACD={current_macd:.4f}, 当前零下段未改善)")
                        continue  # 跳过此股票

                    # 获取当前MACD值用于输出
                    current_macd_value = float(macd_series.iloc[signal_idx]) if signal_idx < len(macd_series) else 0.0

                except Exception as e:
                    print(f"MACD计算失败: {ts_code} {name} - {e}")
                    continue

                stocks_with_buy_signal.append({
                    'ts_code': ts_code,
                    'symbol': symbol,
                    'name': name,
                    'signal_date': matched_date,
                    'chanlun_price': chanlun_price,
                    'bulao_price': bulao_price,
                    'jdbl': chanlun_jdbl,
                    'macd_value': current_macd_value
                })
                print(f"找到共振: {ts_code} {name} {matched_date} (MACD={current_macd_value:.4f})")
                    
        except Exception as e:
            # 跳过获取失败的股票
            continue
    
    return stocks_with_buy_signal


def save_results(stocks: List[Dict], output_path: str):
    """
    保存筛选结果到文件
    
    Args:
        stocks: 股票列表
        output_path: 输出文件路径
    """
    if not stocks:
        print("没有找到符合条件的个股")
        return
        
    df = pd.DataFrame(stocks)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"结果已保存到: {output_path}")


def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='筛选最近N天缠论买点与BULAO金叉共振的个股')
    parser.add_argument(
        '-n',
        '--days',
        type=int,
        default=3,
        help='最近N天（含今天），默认3'
    )
    parser.add_argument(
        '--add-virtual-day',
        action='store_true',
        help='添加虚拟交易日数据（最新日期+1天，OHLC=最新close）'
    )
    return parser.parse_args()


def main():
    """主函数"""
    args = parse_args()

    if args.days < 1:
        print("参数错误: N 必须大于等于 1")
        sys.exit(1)

    # 配置
    STOCK_LIST_CSV = '/root/.openclaw/workspace/data/raw/stock_list.csv'
    STOCK_PERSONALITY_CSV = '/root/.openclaw/workspace/data/raw/stock_personality.csv'
    MIN_LIMIT_UP_COUNT_1Y = 3
    OUTPUT_DIR = '/root/.openclaw/workspace/output'
    OUTPUT_CSV = os.path.join(OUTPUT_DIR, f'chanlun_bulao_buy_resonance_last_{args.days}_days.csv')
    
    # 确保输出目录存在
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 1. 获取股票列表
    print("=" * 60)
    print("缠论+BULAO共振筛选器（最近N天）")
    print("=" * 60)
    
    stock_list_df = get_stock_list(STOCK_LIST_CSV)
    print(f"共读取 {len(stock_list_df)} 只股票")

    # 过滤非主板与ST个股
    original_count = len(stock_list_df)
    stock_list_df = filter_stock_pool_mainboard_non_st(stock_list_df)
    filtered_count = len(stock_list_df)
    print(
        f"过滤后主板非ST股票: {filtered_count} 只 "
        f"(剔除 {original_count - filtered_count} 只非主板/ST)"
    )

    if not os.path.exists(STOCK_PERSONALITY_CSV):
        raise FileNotFoundError(f"股性文件不存在: {STOCK_PERSONALITY_CSV}")

    personality_df = get_stock_personality(STOCK_PERSONALITY_CSV)
    before_personality_count = len(stock_list_df)
    stock_list_df = filter_by_limit_up_count(
        stock_list=stock_list_df,
        personality_df=personality_df,
        min_count=MIN_LIMIT_UP_COUNT_1Y
    )
    after_personality_count = len(stock_list_df)
    print(
        f"过滤后近一年涨停次数>={MIN_LIMIT_UP_COUNT_1Y}股票: {after_personality_count} 只 "
        f"(剔除 {before_personality_count - after_personality_count} 只股性不足个股)"
    )
    print()
    
    # 2. 初始化数据获取器
    fetcher = DataFetcher()
    
    # 3. 筛选最近N天同时出现缠论买点和BULAO金叉的个股
    stocks_with_buy = filter_stocks_with_dual_buy_signal_recent_days(
        stock_list=stock_list_df,
        data_fetcher=fetcher,
        lookback_days=365*2,  # 缠论需要足够的历史数据
        recent_days=args.days,
        add_virtual_day=args.add_virtual_day
    )
    
    # 4. 输出结果
    print()
    print("=" * 60)
    print(f"筛选结果: 最近{args.days}天共 {len(stocks_with_buy)} 只个股出现缠论+BULAO共振买点（含MACD过滤）")
    print("=" * 60)

    if stocks_with_buy:
        print(f"\n{'代码':<12} {'名称':<15} {'缠论价格':<12} {'BULAO价格':<12} {'MACD':<10} {'信号日期'}")
        print("-" * 75)
        for stock in stocks_with_buy:
            print(
                f"{stock['ts_code']:<12} {stock['name']:<15} "
                f"{stock['chanlun_price']:<12.2f} {stock['bulao_price']:<12.2f} "
                f"{stock.get('macd_value', 0):<10.4f} {stock['signal_date']}"
            )
        
        # 保存结果
        save_results(stocks_with_buy, OUTPUT_CSV)
    else:
        print(f"最近{args.days}天没有个股同时出现缠论买点和BULAO金叉")
    
    return stocks_with_buy


if __name__ == "__main__":
    main()
