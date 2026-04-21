#!/usr/bin/env python3
"""
小卓策略v4参数优化 — 网格搜索
基于v4(涨停日检查多头)的信号，测试不同出场参数组合。

优化维度:
  1. 止损: -2%, -3%, -4%, -5%, -6%, -8%
  2. 止盈: +5%, +8%, +10%, +15%, +20%
  3. 最大持仓天数: 3, 5, 7, 10, 15天

目标: 找到胜率 > 45% 且期望值 > 0 的最优组合
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
    """v4信号生成：涨停日检查多头排列"""
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    signals = []

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

                # 涨停日多头
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

                    entry_price = c[check_idx]
                    if entry_price <= 0:
                        continue

                    # 收集15天未来数据
                    future = []
                    for j in range(1, 16):
                        if check_idx + j < n:
                            future.append({
                                'd': str(df['trade_date'].iloc[check_idx + j]),
                                'o': float(o[check_idx + j]),
                                'h': float(h[check_idx + j]),
                                'l': float(l[check_idx + j]),
                                'c': float(c[check_idx + j]),
                            })
                        else:
                            break

                    signals.append({
                        'code': ts_code,
                        'entry_date': date,
                        'entry_price': float(entry_price),
                        'day_after_zt': day_offset,
                        'future': future,
                    })
                    break
        except:
            continue
        if (fi + 1) % 1000 == 0:
            print(f"  已扫描 {fi+1}/{len(files)} 文件, 信号数: {len(signals)}")

    # 去重
    seen = set()
    unique = []
    for s in signals:
        key = (s['code'], s['entry_date'])
        if key not in seen:
            seen.add(key)
            unique.append(s)
    return unique


def backtest(signals, sl, tp, mh, daily_limit=3):
    """回测，返回统计指标"""
    sig_sorted = sorted(signals, key=lambda x: (x['entry_date'], x['code']))
    daily_count = {}
    trades = []

    for sig in sig_sorted:
        d = sig['entry_date']
        daily_count.setdefault(d, 0)
        if daily_count[d] >= daily_limit:
            continue
        daily_count[d] += 1

        ep = sig['entry_price']
        future = sig['future']
        closed = False
        pnl = 0

        for day_idx, fp in enumerate(future):
            hd = day_idx + 1
            if (fp['l'] - ep) / ep <= sl:
                pnl = sl; closed = True; break
            if (fp['h'] - ep) / ep >= tp:
                pnl = tp; closed = True; break
            if hd >= mh:
                pnl = (fp['c'] - ep) / ep; closed = True; break

        if not closed and future:
            pnl = (future[-1]['c'] - ep) / ep
        elif not closed:
            pnl = 0

        trades.append(pnl)

    if not trades:
        return None

    trades = np.array(trades)
    wins = trades[trades > 0]
    losses = trades[trades <= 0]
    n = len(trades)
    wr = len(wins) / n if n > 0 else 0
    avg_w = wins.mean() if len(wins) > 0 else 0
    avg_l = losses.mean() if len(losses) > 0 else 0
    rr = abs(avg_w / avg_l) if avg_l != 0 else 999
    ev = wr * avg_w + (1 - wr) * avg_l
    pf = abs(wins.sum() / losses.sum()) if losses.sum() != 0 else 999

    return {
        'n': n, 'wr': wr, 'avg_w': avg_w, 'avg_l': avg_l, 'rr': rr,
        'ev': ev, 'pf': pf, 'total': trades.sum(), 'med': np.median(trades),
        'sl': sl, 'tp': tp, 'mh': mh,
    }


def main():
    print("=" * 60)
    print("小卓策略v4 参数网格搜索")
    print(f"回测区间: {BACKTEST_START} ~ {BACKTEST_END}")
    print("=" * 60)

    # 1. 生成信号
    print("\n📊 生成v4信号...")
    signals = generate_signals()
    print(f"  去重后信号数: {len(signals)}")

    # 2. 参数网格
    stop_losses = [-0.02, -0.03, -0.04, -0.05, -0.06, -0.08]
    take_profits = [0.05, 0.08, 0.10, 0.15, 0.20]
    max_holds = [3, 5, 7, 10, 15]

    print(f"\n🔬 开始网格搜索...")
    print(f"  止损: {[f'{s:.0%}' for s in stop_losses]}")
    print(f"  止盈: {[f'{t:.0%}' for t in take_profits]}")
    print(f"  持仓: {max_holds} 天")
    print(f"  总组合数: {len(stop_losses) * len(take_profits) * len(max_holds)}")

    results = []
    total_combos = len(stop_losses) * len(take_profits) * len(max_holds)
    count = 0

    for sl, tp, mh in product(stop_losses, take_profits, max_holds):
        count += 1
        r = backtest(signals, sl, tp, mh)
        if r:
            results.append(r)
        if count % 30 == 0:
            print(f"  进度: {count}/{total_combos}")

    print(f"\n  完成! 共 {len(results)} 个有效组合")

    # 3. 排序输出
    # 按不同维度排序
    print("\n" + "=" * 70)
    print("🏆 TOP 15 — 按胜率排序（胜率>40%且期望值>0）")
    print("=" * 70)
    filtered = [r for r in results if r['ev'] > 0 and r['wr'] > 0.40]
    by_wr = sorted(filtered, key=lambda x: -x['wr'])
    print(f"{'止损':>6} {'止盈':>6} {'持仓':>4} {'笔数':>5} {'胜率':>7} {'盈亏比':>7} {'期望值':>8} {'总收益':>8} {'盈亏因子':>7}")
    print("-" * 70)
    for r in by_wr[:15]:
        print(f"{r['sl']:>6.0%} {r['tp']:>6.0%} {r['mh']:>4d}天 {r['n']:>5d} "
              f"{r['wr']:>6.1%} {r['rr']:>7.2f} {r['ev']:>7.2%} {r['total']:>7.1%} {r['pf']:>7.2f}")

    print("\n" + "=" * 70)
    print("🏆 TOP 15 — 按期望值排序（胜率>40%）")
    print("=" * 70)
    by_ev = sorted(filtered, key=lambda x: -x['ev'])
    print(f"{'止损':>6} {'止盈':>6} {'持仓':>4} {'笔数':>5} {'胜率':>7} {'盈亏比':>7} {'期望值':>8} {'总收益':>8} {'盈亏因子':>7}")
    print("-" * 70)
    for r in by_ev[:15]:
        print(f"{r['sl']:>6.0%} {r['tp']:>6.0%} {r['mh']:>4d}天 {r['n']:>5d} "
              f"{r['wr']:>6.1%} {r['rr']:>7.2f} {r['ev']:>7.2%} {r['total']:>7.1%} {r['pf']:>7.2f}")

    print("\n" + "=" * 70)
    print("🏆 TOP 15 — 按总收益排序（胜率>40%）")
    print("=" * 70)
    by_total = sorted(filtered, key=lambda x: -x['total'])
    print(f"{'止损':>6} {'止盈':>6} {'持仓':>4} {'笔数':>5} {'胜率':>7} {'盈亏比':>7} {'期望值':>8} {'总收益':>8} {'盈亏因子':>7}")
    print("-" * 70)
    for r in by_total[:15]:
        print(f"{r['sl']:>6.0%} {r['tp']:>6.0%} {r['mh']:>4d}天 {r['n']:>5d} "
              f"{r['wr']:>6.1%} {r['rr']:>7.2f} {r['ev']:>7.2%} {r['total']:>7.1%} {r['pf']:>7.2f}")

    # 4. 宽松条件：胜率>35%也看看
    print("\n" + "=" * 70)
    print("📊 TOP 15 — 按期望值排序（放宽胜率>35%，看到更多高收益组合）")
    print("=" * 70)
    filtered2 = [r for r in results if r['ev'] > 0.001 and r['wr'] > 0.35]
    by_ev2 = sorted(filtered2, key=lambda x: -x['ev'])
    print(f"{'止损':>6} {'止盈':>6} {'持仓':>4} {'笔数':>5} {'胜率':>7} {'盈亏比':>7} {'期望值':>8} {'总收益':>8} {'盈亏因子':>7}")
    print("-" * 70)
    for r in by_ev2[:15]:
        print(f"{r['sl']:>6.0%} {r['tp']:>6.0%} {r['mh']:>4d}天 {r['n']:>5d} "
              f"{r['wr']:>6.1%} {r['rr']:>7.2f} {r['ev']:>7.2%} {r['total']:>7.1%} {r['pf']:>7.2f}")

    # 5. 当前参数(原版 -3%/+20%/5天)作为基准
    print("\n" + "=" * 70)
    print("📏 基准对比（当前参数 -3%/+20%/5天）")
    print("=" * 70)
    baseline = backtest(signals, -0.03, 0.20, 5)
    if baseline:
        print(f"  笔数: {baseline['n']}  胜率: {baseline['wr']:.1%}  "
              f"盈亏比: {baseline['rr']:.2f}  期望值: {baseline['ev']:.2%}  "
              f"总收益: {baseline['total']:.1%}")

    # 6. 保存完整结果
    out_path = "/root/.openclaw/workspace/finance_tool/backtest/v4_optimization_results.json"
    with open(out_path, 'w') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n📁 完整结果已保存: {out_path}")


if __name__ == "__main__":
    main()
