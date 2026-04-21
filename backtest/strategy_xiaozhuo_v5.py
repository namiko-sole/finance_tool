#!/usr/bin/env python3
"""
小卓策略v5 — 唯一改进：次日开盘价入场

基于v4_batch_tp.py，改动点：
  - entry_price = 次日开盘价（v4是当天收盘价）
  - future从入场日次日开始计算（即check_idx+2起）
  - 次日高开>7%或低开>5%时放弃该信号
  - 其他条件、参数、分批止盈逻辑完全不变

参数同v4最优：
  止损-8% / 赚5%卖30% / 剩余等20%清仓 / 最大持仓15天
"""
import os, glob, warnings, json
import numpy as np
import pandas as pd
from itertools import product
warnings.filterwarnings('ignore')

DATA_DIR = "/root/.openclaw/workspace/data/raw/stock_daily/"
BACKTEST_START = "20230101"
BACKTEST_END = "20260417"


def generate_signals():
    """v4信号生成 + 次日开盘入场改动"""
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    signals = []
    skip_gap = 0

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
                if zt_date < BACKTEST_START or zt_date > BACKTEST_END:
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
                    date = str(df['trade_date'].iloc[check_idx])
                    if date > BACKTEST_END:
                        break

                    post_zt_vols = v[zt_idx + 1:check_idx + 1]
                    post_zt_amts = amt[zt_idx + 1:check_idx + 1]
                    if not np.any(post_zt_vols <= shrink_target):
                        continue
                    min_amt = float(np.min(post_zt_amts))
                    if min_amt > 100000:
                        continue
                    if ma10[check_idx] <= 0:
                        continue
                    price_vs_ma10 = c[check_idx] / ma10[check_idx]
                    if price_vs_ma10 < 0.98:
                        continue
                    if not (l[check_idx - 1] < l[check_idx - 2] and l[check_idx - 1] < l[check_idx]):
                        continue
                    if c[check_idx] <= o[check_idx]:
                        continue

                    window_start = max(0, check_idx - 19)
                    high_20d = float(np.max(c[window_start:check_idx + 1]))
                    low_20d = float(np.min(c[window_start:check_idx + 1]))
                    pos_20d = (c[check_idx] - low_20d) / (high_20d - low_20d) if high_20d > low_20d else 0.5
                    if pos_20d >= 0.5:
                        continue

                    signal_close = c[check_idx]

                    # ─── v5唯一改动：次日开盘价入场 ───
                    next_idx = check_idx + 1
                    if next_idx >= n:
                        continue

                    entry_price = o[next_idx]
                    if entry_price <= 0:
                        continue

                    # 次日跳空过滤
                    next_gap = (entry_price - signal_close) / signal_close
                    if next_gap > 0.07 or next_gap < -0.05:
                        skip_gap += 1
                        continue

                    # future从next_idx+1开始（入场日的次日）
                    future = []
                    for j in range(1, 16):
                        idx = next_idx + j
                        if idx < n:
                            future.append({
                                'd': str(df['trade_date'].iloc[idx]),
                                'o': float(o[idx]),
                                'h': float(h[idx]),
                                'l': float(l[idx]),
                                'c': float(c[idx]),
                            })
                        else:
                            break

                    signals.append({
                        'code': ts_code,
                        'signal_date': date,
                        'entry_date': str(df['trade_date'].iloc[next_idx]),
                        'entry_price': float(entry_price),
                        'signal_close': float(signal_close),
                        'next_gap': float(next_gap),
                        'day_after_zt': day_offset,
                        'future': future,
                    })
                    break
        except:
            continue
        if (fi + 1) % 1000 == 0:
            print(f"  已扫描 {fi+1}/{len(files)} 文件, 信号数: {len(signals)}")

    # 去重：同一天同一股票只保留一个信号（用entry_date去重）
    seen = set()
    unique = []
    for s in signals:
        key = (s['code'], s['entry_date'])
        if key not in seen:
            seen.add(key)
            unique.append(s)

    print(f"\n[信号生成] {len(unique)} 个信号 (跳过跳空: {skip_gap})")
    return unique


def backtest_batch(signals, sl, tp1, tp2, sell_ratio, mh, daily_limit=3):
    """分批止盈回测（与v4完全一致）"""
    sig_sorted = sorted(signals, key=lambda x: (x['entry_date'], x['code']))
    daily_count = {}
    trades = []
    trade_details = []

    for sig in sig_sorted:
        d = sig['entry_date']
        daily_count.setdefault(d, 0)
        if daily_count[d] >= daily_limit:
            continue
        daily_count[d] += 1

        ep = sig['entry_price']
        future = sig['future']
        remaining = 1.0
        realized = 0.0
        closed = False
        exit_reason = ''
        tp1_done = False

        for day_idx, fp in enumerate(future):
            hd = day_idx + 1
            day_low = (fp['l'] - ep) / ep
            day_high = (fp['h'] - ep) / ep

            # 止损
            if day_low <= sl:
                realized += remaining * sl
                remaining = 0
                exit_reason = '止损'
                closed = True
                break

            # tp1: 卖出一部分
            if not tp1_done and remaining > (1 - sell_ratio) + 0.001 and day_high >= tp1:
                realized += sell_ratio * tp1
                remaining -= sell_ratio
                tp1_done = True

            # tp2: 全部清仓
            if remaining > 0 and day_high >= tp2:
                realized += remaining * tp2
                remaining = 0
                exit_reason = '止盈'
                closed = True
                break

            # 超时
            if hd >= mh:
                if remaining > 0:
                    realized += remaining * ((fp['c'] - ep) / ep)
                    remaining = 0
                exit_reason = f'持{mh}天'
                closed = True
                break

        if not closed:
            if remaining > 0 and future:
                realized += remaining * ((future[-1]['c'] - ep) / ep)
                exit_reason = '数据截止'
            else:
                exit_reason = '无数据'

        trades.append(realized)
        trade_details.append({
            'code': sig['code'],
            'signal_date': sig['signal_date'],
            'entry_date': sig['entry_date'],
            'entry': ep,
            'signal_close': sig.get('signal_close', ep),
            'next_gap': sig.get('next_gap', 0),
            'pnl': realized,
            'reason': exit_reason,
            'tp1_done': tp1_done,
            'day_after_zt': sig['day_after_zt'],
        })

    if not trades:
        return None, []

    arr = np.array(trades)
    wins = arr[arr > 0]
    losses = arr[arr <= 0]
    n = len(arr)
    wr = len(wins) / n
    avg_w = wins.mean() if len(wins) > 0 else 0
    avg_l = losses.mean() if len(losses) > 0 else 0
    rr = abs(avg_w / avg_l) if avg_l != 0 else 999
    ev = wr * avg_w + (1 - wr) * avg_l
    pf = abs(wins.sum() / losses.sum()) if losses.sum() != 0 else 999

    result = {
        'n': n, 'wr': wr, 'avg_w': avg_w, 'avg_l': avg_l, 'rr': rr,
        'ev': ev, 'pf': pf, 'total': arr.sum(), 'med': np.median(arr),
    }
    return result, trade_details


def main():
    print("=" * 60)
    print("  小卓策略v5 — 次日开盘价入场")
    print("  唯一改动: entry_price = 次日open (v4是当天close)")
    print("  参数: 止损-8% / 赚5%卖30% / 剩余等20% / 持仓15天")
    print("=" * 60)

    print("\n[1] 生成信号...")
    signals = generate_signals()

    if not signals:
        print("没有信号！")
        return

    # v5最优参数（与v4一致）
    print("\n[2] 回测: -8% / +5%卖30% / 剩余+20% / 15天")
    result, details = backtest_batch(signals, -0.08, 0.05, 0.20, 0.3, 15)

    if not result:
        print("回测无结果！")
        return

    print("\n" + "=" * 60)
    print("  v5 回测结果")
    print("=" * 60)
    print(f"  总交易:   {result['n']} 笔")
    print(f"  胜率:     {result['wr']:.1%}")
    print(f"  盈亏比:   {result['rr']:.2f}:1")
    print(f"  期望值:   {result['ev']:.2%}")
    print(f"  盈利因子: {result['pf']:.2f}")
    print(f"  累计收益: {result['total']:.1%}")
    print(f"  中位数:   {result['med']:.2%}")

    # 次日跳空统计
    df = pd.DataFrame(details)
    print("\n" + "=" * 60)
    print("  次日跳空分析")
    print("=" * 60)
    print(f"  均值: {df['next_gap'].mean():.2%}  中位: {df['next_gap'].median():.2%}")
    for q in [10, 25, 50, 75, 90]:
        print(f"  P{q}: {df['next_gap'].quantile(q/100):.2%}")

    # 出场原因
    print("\n" + "=" * 60)
    print("  出场原因统计")
    print("=" * 60)
    for reason, sub in df.groupby('reason'):
        wr = (sub['pnl'] > 0).mean()
        print(f"  {reason:>6s}: {len(sub):4d}笔 ({len(sub)/len(df):5.1%})  胜率{wr:.1%}  均收{sub['pnl'].mean():.2%}")

    # 年度
    print("\n" + "=" * 60)
    print("  年度统计")
    print("=" * 60)
    df['year'] = df['signal_date'].astype(str).str[:4]
    for yr, sub in df.groupby('year'):
        wr = (sub['pnl'] > 0).mean()
        print(f"  {yr}: {len(sub):4d}笔  胜率{wr:.1%}  均收{sub['pnl'].mean():.2%}  累计{sub['pnl'].sum():.1%}")

    # tp1触发率
    print("\n" + "=" * 60)
    print("  tp1(+5%卖30%)触发分析")
    print("=" * 60)
    tp1_rate = df['tp1_done'].mean()
    tp1_wr = df[df['tp1_done']]['pnl'].mean() if df['tp1_done'].any() else 0
    no_tp1_wr = df[~df['tp1_done']]['pnl'].mean() if (~df['tp1_done']).any() else 0
    print(f"  tp1触发: {df['tp1_done'].sum()}/{len(df)} ({tp1_rate:.1%})")
    print(f"  触发tp1的均收: {tp1_wr:.2%}")
    print(f"  未触发tp1的均收: {no_tp1_wr:.2%}")

    # 保存
    out_dir = "/root/.openclaw/workspace/finance_tool/backtest/results/"
    os.makedirs(out_dir, exist_ok=True)
    df.to_csv(os.path.join(out_dir, 'xiaozhuo_v5_trades.csv'), index=False)
    print(f"\n已保存: {out_dir}xiaozhuo_v5_trades.csv")


if __name__ == "__main__":
    main()
