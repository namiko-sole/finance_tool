"""
缠论买点过滤器

从股票列表中筛选出最近N天缠论买点与BULAO金叉共振的个股

功能：
1. 从 stock_list.csv 获取所有个股
2. 通过 DataFetcher 获取每个个股的数据
3. 使用 chanlun_tt/main.py 的 get_buy_sell_signals 获取缠论买卖信号
4. 使用 bulao/main.py 的 get_buy_sell_signals 获取BULAO金叉信号
5. 筛选最近N天同时出现缠论买点和BULAO金叉的个股并输出
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
    recent_days: int = 3
) -> List[Dict]:
    """
    筛选最近N天同时出现缠论买点和BULAO金叉的个股
    
    Args:
        stock_list: 股票列表 DataFrame
        data_fetcher: DataFetcher 实例
        lookback_days: 回溯天数，用于获取足够的历史数据计算缠论
        recent_days: 最近N天（含今天）
        
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
    print("-" * 60)
    
    for idx, row in stock_list.iterrows():
        ts_code = row['ts_code']
        symbol = row['symbol']
        name = row.get('name', '')
        
        if (idx + 1) % 100 == 0:
            print(f"已处理 {idx + 1}/{total} 只股票...")
        
        try:
            # 获取缠论买卖信号
            chanlun_result = get_chanlun_buy_sell_signals(
                stock_code=ts_code,
                start_date=start_date,
                end_date=None,
                data_fetcher=data_fetcher
            )

            if 'error' in chanlun_result:
                continue

            # 获取BULAO买卖信号（金叉对应 signal=买入）
            bulao_result = get_bulao_buy_sell_signals(
                stock_code=ts_code,
                start_date=start_date,
                end_date=None,
                data_fetcher=data_fetcher
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

                stocks_with_buy_signal.append({
                    'ts_code': ts_code,
                    'symbol': symbol,
                    'name': name,
                    'signal_date': matched_date,
                    'chanlun_price': chanlun_price,
                    'bulao_price': bulao_price,
                    'jdbl': chanlun_jdbl
                })
                print(f"找到共振: {ts_code} {name} {matched_date}")
                    
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
        recent_days=args.days
    )
    
    # 4. 输出结果
    print()
    print("=" * 60)
    print(f"筛选结果: 最近{args.days}天共 {len(stocks_with_buy)} 只个股出现缠论+BULAO共振买点")
    print("=" * 60)
    
    if stocks_with_buy:
        print(f"\n{'代码':<12} {'名称':<15} {'缠论价格':<12} {'BULAO价格':<12} {'信号日期'}")
        print("-" * 60)
        for stock in stocks_with_buy:
            print(
                f"{stock['ts_code']:<12} {stock['name']:<15} "
                f"{stock['chanlun_price']:<12.2f} {stock['bulao_price']:<12.2f} {stock['signal_date']}"
            )
        
        # 保存结果
        save_results(stocks_with_buy, OUTPUT_CSV)
    else:
        print(f"最近{args.days}天没有个股同时出现缠论买点和BULAO金叉")
    
    return stocks_with_buy


if __name__ == "__main__":
    main()
