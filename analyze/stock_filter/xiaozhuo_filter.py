#!/usr/bin/env python3
"""
小卓策略(v5.1) — 当日扫描选股
扫描全市场，返回今日满足小卓策略入场条件的信号列表。

两个池子：
  正式池: pos_20d < 0.5（严格满足）
  观察池: 0.5 <= pos_20d < 0.8（持续监控，pos降到<0.5自动升级）

注意：扫描器用当天收盘价作为参考价，实际入场为次日开盘价（v5.1）
"""
import os
import glob
import numpy as np
import pandas as pd

DATA_DIR = "/root/.openclaw/workspace/data/raw/stock_daily/"
STOCK_LIST_PATH = "/root/.openclaw/workspace/data/raw/stock_list.csv"


def _load_stock_list() -> dict:
    """加载股票列表，返回 {ts_code: name}"""
    df = pd.read_csv(STOCK_LIST_PATH, encoding='utf-8-sig')
    return dict(zip(df['ts_code'], df['name']))


def _should_skip(ts_code: str, name: str) -> bool:
    """判断是否应跳过该股票（ST/科创板/创业板/北交所）"""
    # 科创板 688xxx
    if ts_code[:3] == '688':
        return True
    # 创业板 300xxx/301xxx
    if ts_code[:3] in ('300', '301'):
        return True
    # 北交所: 8开头/4开头 以及 .BJ后缀
    if ts_code[0] in ('8', '4'):
        return True
    if ts_code.endswith('.BJ'):
        return True
    # ST
    if 'ST' in name.upper():
        return True
    return False


def scan_xiaozhuo_signals(current_trade_day: str, lookback_days: int = 15,
                           pos_threshold: float = 0.5) -> list[dict]:
    """
    扫描全市场，返回今日满足小卓策略(v5.1)入场条件的信号列表。

    Args:
        current_trade_day: 当天交易日 YYYYMMDD格式
        lookback_days: 往前找涨停的天数，默认15
        pos_threshold: 20日区间位置上限，默认0.5（正式池），
                       设为0.8可获取正式池+观察池

    Returns:
        list of signal dicts, 每个dict包含:
        {
            'ts_code': str,
            'name': str,
            'zt_date': str,           # 涨停日期 YYYYMMDD
            'zt_vol': float,          # 涨停日成交量（手）
            'signal_date': str,       # 信号触发日期(=current_trade_day)
            'entry_price': float,     # 当天收盘价
            'min_shrink_vol': float,  # 缩量期间最小成交量
            'min_shrink_ratio': float,# 最小缩量比例(相对涨停日量)
            'min_amount': float,      # 缩量期间最小成交额(千元)
            'price_vs_ma10': float,   # 收盘价/MA10 - 1 的百分比
            'pos_20d': float,         # 20日区间位置
            'day_after_zt': int,      # 涨停后第几天
        }
    """
    stock_name_map = _load_stock_list()
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    signals = []
    total_scanned = 0

    print(f"[小卓扫描] 开始扫描，目标日期={current_trade_day}，共 {len(files)} 只股票")

    for fi, f in enumerate(files):
        ts_code = os.path.basename(f).replace('.csv', '')
        name = stock_name_map.get(ts_code, '')

        # 跳过不符合条件的股票
        if _should_skip(ts_code, name):
            continue

        try:
            df = pd.read_csv(f, encoding='utf-8-sig')
            if len(df) < 60:
                continue

            # 排序：日期升序
            df = df.sort_values('trade_date').reset_index(drop=True)
            # 只取最近60条
            if len(df) > 60:
                df = df.tail(60).reset_index(drop=True)

            n = len(df)
            c = df['close'].values
            l = df['low'].values
            v = df['vol'].values
            amt = df['amount'].values  # 单位：千元

            # 均线
            ma5 = pd.Series(c).rolling(5).mean().values
            ma10 = pd.Series(c).rolling(10).mean().values
            ma20 = pd.Series(c).rolling(20).mean().values

            # 多头排列标记
            bull = (ma5 > ma10) & (ma10 > ma20)

            # 涨停标记
            is_zt = df['pct_chg'].values >= 9.8
            zt_indices = np.where(is_zt)[0]

            if len(zt_indices) == 0:
                continue

            # 只保留 lookback_days 内的涨停日
            # 找到 current_trade_day 在 df 中的位置
            day_mask = df['trade_date'].astype(str) == current_trade_day
            day_locs = np.where(day_mask)[0]
            if len(day_locs) == 0:
                continue
            current_idx = day_locs[0]

            # 股票信号收集（同一个涨停只取第一个）
            stock_signals = []

            for zt_idx in zt_indices:
                zt_date = str(df['trade_date'].iloc[zt_idx])

                # 涨停必须在 current_trade_day 之前
                if zt_date >= current_trade_day:
                    continue

                # 涨停必须在 lookback_days 天内
                days_diff = 0
                # 计算交易日天数差
                between = df[(df['trade_date'].astype(str) > zt_date) &
                             (df['trade_date'].astype(str) <= current_trade_day)]
                days_diff = len(between)
                if days_diff > lookback_days:
                    continue

                # 多头排列检查 — 在涨停日判断（确保涨停发生在多头趋势中）
                if not bull[zt_idx]:
                    continue

                zt_vol = v[zt_idx]
                if zt_vol <= 0:
                    continue

                shrink_target = zt_vol * 0.5

                # 从涨停后1天到15天检查
                found_signal = False
                for day_offset in range(1, 16):
                    check_idx = zt_idx + day_offset
                    if check_idx >= n or check_idx < 2:
                        break

                    check_date = str(df['trade_date'].iloc[check_idx])

                    # 只检测当天！不是回测每一天
                    if check_date != current_trade_day:
                        continue

                    # 条件1: 缩量检查 — 涨停后到当天区间内，至少有1天 vol <= 涨停日vol * 0.5
                    post_zt_vols = v[zt_idx + 1:check_idx + 1]
                    post_zt_amts = amt[zt_idx + 1:check_idx + 1]

                    has_shrunk = np.any(post_zt_vols <= shrink_target)
                    if not has_shrunk:
                        continue

                    # 条件3: 缩量期间最小成交额 <= 1亿 (100000千元)
                    min_amt = float(np.min(post_zt_amts))
                    if min_amt > 100000:
                        continue

                    # 条件4: 回到MA10附近 — 收盘价 >= MA10 * 0.98
                    if ma10[check_idx] <= 0:
                        continue
                    price_vs_ma10 = c[check_idx] / ma10[check_idx]
                    if price_vs_ma10 < 0.98:
                        continue

                    # 条件5: 底分型 — 前一根low < 前前一根low 且 前一根low < 当天low
                    is_bottom = (
                        l[check_idx - 1] < l[check_idx - 2] and
                        l[check_idx - 1] < l[check_idx]
                    )
                    if not is_bottom:
                        continue

                    # 条件5b: 底分型最后一根K线（当天）必须是阳线
                    o = df['open'].values
                    if c[check_idx] <= o[check_idx]:
                        continue

                    # 条件6: 20日区间位置 < 0.5（至少回调到20日区间的下半区）
                    window_start = max(0, check_idx - 19)
                    high_20d = float(np.max(c[window_start:check_idx + 1]))
                    low_20d = float(np.min(c[window_start:check_idx + 1]))
                    if high_20d > low_20d:
                        pos_20d = (c[check_idx] - low_20d) / (high_20d - low_20d)
                    else:
                        pos_20d = 0.5
                    if pos_20d >= pos_threshold:
                        continue

                    # 入场价 = 当天收盘价
                    entry_price = float(c[check_idx])
                    if entry_price <= 0:
                        continue

                    min_shrink_vol = float(np.min(post_zt_vols))
                    min_shrink_ratio = min_shrink_vol / zt_vol * 100  # 百分比

                    stock_signals.append({
                        'ts_code': ts_code,
                        'name': name,
                        'zt_date': zt_date,
                        'zt_vol': float(zt_vol),
                        'signal_date': current_trade_day,
                        'entry_price': entry_price,
                        'min_shrink_vol': min_shrink_vol,
                        'min_shrink_ratio': min_shrink_ratio,
                        'min_amount': min_amt,
                        'price_vs_ma10': (price_vs_ma10 - 1) * 100,  # 百分比
                        'pos_20d': float(pos_20d),  # 20日区间位置
                        'day_after_zt': day_offset,
                    })

                    # 同一个涨停只取第一个信号
                    found_signal = True
                    break

                if found_signal:
                    break  # 涨停日循环 break，但实际上我们收集所有涨停日信号后去重

            # 去重：同一只股票有多个涨停日都产生信号，只保留 day_after_zt 最小的
            if stock_signals:
                best = min(stock_signals, key=lambda x: x['day_after_zt'])
                signals.append(best)

        except Exception as e:
            continue

        total_scanned += 1
        if total_scanned % 1000 == 0:
            print(f"  [小卓扫描] 已扫描 {total_scanned} 只，当前信号数: {len(signals)}")

    print(f"[小卓扫描] 扫描完成，共扫描 {total_scanned} 只，发现 {len(signals)} 个信号")
    return {"signals": signals, "total_scanned": total_scanned}


if __name__ == "__main__":
    import sys
    # 独立测试用
    day = sys.argv[1] if len(sys.argv) > 1 else "20260415"
    result = scan_xiaozhuo_signals(day)
    signals = result["signals"] if isinstance(result, dict) else result
    for s in signals:
        print(f"  {s['ts_code']} {s['name']} | 涨停{s['zt_date']} | 缩量{s['min_shrink_ratio']:.1f}% | MA10偏离{s['price_vs_ma10']:+.2f}%")
