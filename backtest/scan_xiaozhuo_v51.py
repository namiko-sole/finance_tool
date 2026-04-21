#!/usr/bin/env python3
"""
小卓策略v5.1 选股筛选器 — 指定日期范围
找出指定时间段内满足所有入场条件的个股
"""
import os, glob, warnings
import numpy as np
import pandas as pd
warnings.filterwarnings('ignore')

DATA_DIR = "/root/.openclaw/workspace/data/raw/stock_daily/"
SCAN_START = "20260409"
SCAN_END = "20260420"


def scan():
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    results = []
    all_zt_dates = set()  # 收集所有涨停日（用于判断市场热度）

    # Pass 1: 收集涨停信息
    for f in files:
        ts_code = os.path.basename(f).replace('.csv', '')
        if ts_code[:3] in ('688', '300', '301') or ts_code[0] in ('8', '4') or ts_code.endswith('.BJ'):
            continue
        try:
            df = pd.read_csv(f, encoding='utf-8-sig').sort_values('trade_date').reset_index(drop=True)
            zt_mask = df['pct_chg'].values >= 9.8
            for i in np.where(zt_mask)[0]:
                d = str(df['trade_date'].iloc[i])
                if SCAN_START <= d <= SCAN_END:
                    all_zt_dates.add(d)
        except:
            continue

    # Pass 2: 信号扫描
    for fi, f in enumerate(files):
        ts_code = os.path.basename(f).replace('.csv', '')
        if ts_code[:3] in ('688', '300', '301') or ts_code[0] in ('8', '4') or ts_code.endswith('.BJ'):
            continue
        try:
            df = pd.read_csv(f, encoding='utf-8-sig').sort_values('trade_date').reset_index(drop=True)
            if len(df) < 60:
                continue

            c = df['close'].values
            o = df['open'].values
            h = df['high'].values
            l = df['low'].values
            v = df['vol'].values
            amt = df['amount'].values
            n = len(df)

            ma5 = pd.Series(c).rolling(5).mean().values
            ma10 = pd.Series(c).rolling(10).mean().values
            ma20 = pd.Series(c).rolling(20).mean().values
            bull = (ma5 > ma10) & (ma10 > ma20)
            is_zt = df['pct_chg'].values >= 9.8
            zt_indices = np.where(is_zt)[0]

            for zt_idx in zt_indices:
                zt_date = str(df['trade_date'].iloc[zt_idx])
                # 涨停日可以在扫描期之前（因为信号日在15天内都算）
                if zt_date < SCAN_START and zt_date < "20260320":
                    continue
                if zt_date > SCAN_END:
                    continue

                if not bull[zt_idx]:
                    continue
                zt_vol = v[zt_idx]
                if zt_vol <= 0:
                    continue
                shrink_target = zt_vol * 0.5

                for day_offset in range(1, 16):
                    check_idx = zt_idx + day_offset
                    if check_idx >= n or check_idx < 2:
                        break
                    signal_date = str(df['trade_date'].iloc[check_idx])
                    if signal_date > SCAN_END:
                        break
                    if signal_date < SCAN_START:
                        continue

                    # 缩量
                    post_zt_vols = v[zt_idx + 1:check_idx + 1]
                    post_zt_amts = amt[zt_idx + 1:check_idx + 1]
                    if not np.any(post_zt_vols <= shrink_target):
                        continue
                    min_amt = float(np.min(post_zt_amts))
                    if min_amt > 100000:
                        continue

                    # MA10附近
                    if ma10[check_idx] <= 0:
                        continue
                    price_vs_ma10 = c[check_idx] / ma10[check_idx]
                    if price_vs_ma10 < 0.98:
                        continue

                    # 底分型
                    if not (l[check_idx - 1] < l[check_idx - 2] and l[check_idx - 1] < l[check_idx]):
                        continue

                    # 阳线
                    if c[check_idx] <= o[check_idx]:
                        continue

                    # pos_20d < 0.5
                    ws = max(0, check_idx - 19)
                    high_20d = float(np.max(c[ws:check_idx + 1]))
                    low_20d = float(np.min(c[ws:check_idx + 1]))
                    pos_20d = (c[check_idx] - low_20d) / (high_20d - low_20d) if high_20d > low_20d else 0.5
                    if pos_20d >= 0.5:
                        continue

                    # 次日开盘价
                    next_idx = check_idx + 1
                    if next_idx >= n:
                        continue
                    next_open = o[next_idx]
                    if next_open <= 0:
                        continue
                    next_gap = (next_open - c[check_idx]) / c[check_idx]
                    if next_gap > 0.07 or next_gap < -0.05:
                        continue

                    # 涨停日涨幅
                    zt_pct = df['pct_chg'].iloc[zt_idx]

                    # 缩量比例
                    min_shrink_ratio = float(np.min(post_zt_vols)) / zt_vol

                    results.append({
                        'code': ts_code,
                        'signal_date': signal_date,
                        'entry_date': str(df['trade_date'].iloc[next_idx]),
                        'entry_price': float(next_open),
                        'zt_date': zt_date,
                        'zt_pct': float(zt_pct),
                        'days_after_zt': day_offset,
                        'pos_20d': round(pos_20d, 3),
                        'min_shrink': round(min_shrink_ratio, 3),
                        'min_amt_wan': round(min_amt, 0),  # 千元→直接显示
                        'price_vs_ma10': round(price_vs_ma10, 4),
                        'next_gap': round(next_gap, 4),
                    })
                    break
        except:
            continue

    return results


def main():
    print("=" * 70)
    print(f"  小卓策略v5.1 选股筛选 — {SCAN_START} ~ {SCAN_END}")
    print("=" * 70)

    results = scan()
    if not results:
        print("\n该时间段内没有满足条件的个股。")
        return

    df = pd.DataFrame(results)
    # 去重
    df = df.drop_duplicates(subset=['code', 'entry_date'], keep='first')
    df = df.sort_values(['signal_date', 'code']).reset_index(drop=True)

    print(f"\n共筛选出 {len(df)} 只个股：\n")
    print(f"{'信号日':<12} {'入场日':<12} {'代码':<12} {'入场价':>8} {'pos':>6} "
          f"{'缩量比':>6} {'成交额(万)':>10} {'MA10比':>6} {'跳空':>6} {'涨停后':>6}")
    print("-" * 90)
    for _, r in df.iterrows():
        print(f"{r['signal_date']:<12} {r['entry_date']:<12} {r['code']:<12} "
              f"{r['entry_price']:>7.2f} {r['pos_20d']:>6.3f} "
              f"{r['min_shrink']:>6.1%} {r['min_amt_wan']:>10.0f} "
              f"{r['price_vs_ma10']:>6.3f} {r['next_gap']:>+5.2%} {r['days_after_zt']:>4d}天")

    # 按信号日汇总
    print(f"\n{'='*70}")
    print("  按信号日汇总")
    print(f"{'='*70}")
    for d, sub in df.groupby('signal_date'):
        codes = sub['code'].tolist()
        print(f"  {d}: {len(sub)}只 → {', '.join(codes)}")

    # 如果有后续数据，看看入场后的表现
    print(f"\n{'='*70}")
    print("  入场后表现（如有数据）")
    print(f"{'='*70}")
    for _, r in df.iterrows():
        ts_code = r['code']
        entry_date = r['entry_date']
        entry_price = r['entry_price']
        csv_path = os.path.join(DATA_DIR, f"{ts_code}.csv")
        try:
            raw = pd.read_csv(csv_path, encoding='utf-8-sig')
            raw['trade_date'] = raw['trade_date'].astype(str)
            after = raw[raw['trade_date'] > entry_date].head(5)
            if len(after) > 0:
                prices = []
                for _, row in after.iterrows():
                    chg = (row['close'] - entry_price) / entry_price
                    prices.append(f"{row['trade_date']}:{chg:+.1%}")
                max_h = (after['high'].max() - entry_price) / entry_price
                min_l = (after['low'].min() - entry_price) / entry_price
                print(f"  {ts_code} 入场{entry_price:.2f} → 最大+{max_h:.1%} 最小{min_l:+.1%} | {' → '.join(prices)}")
            else:
                print(f"  {ts_code} 入场后暂无数据")
        except:
            print(f"  {ts_code} 数据读取失败")


if __name__ == "__main__":
    main()
