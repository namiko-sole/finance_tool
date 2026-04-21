#!/usr/bin/env python3
"""
小卓策略v4 — 分批止盈优化
核心思路：到达第一止盈位卖一半锁利润，剩余继续持有等更高目标。

分批逻辑：
  - 持仓量 = 1.0
  - 触发 tp1 → 卖出 sell_ratio，锁住 tp1 收益
  - 剩余继续持，触发 tp2 全部卖出
  - 跌到止损 → 剩余全部止损
  - 超过 max_hold → 剩余按收盘价结算
  - 最终收益 = sell_ratio * tp1 + (1-sell_ratio) * 剩余部分收益率
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


def backtest_batch(signals, sl, tp1, tp2, sell_ratio, mh, daily_limit=3):
    """
    分批止盈回测。
    sl: 止损 (负数如 -0.08)
    tp1: 第一止盈位 (如 0.05)
    tp2: 第二止盈位 (如 0.15)
    sell_ratio: 第一止盈卖出比例 (如 0.5 = 卖一半)
    mh: 最大持仓天数
    """
    sig_sorted = sorted(signals, key=lambda x: (x['entry_date'], x['code']))
    daily_count = {}
    trades = []  # 每笔的综合收益率
    exit_stats = {'tp1_hit': 0, 'tp2_hit': 0, 'sl_hit': 0, 'timeout': 0}

    for sig in sig_sorted:
        d = sig['entry_date']
        daily_count.setdefault(d, 0)
        if daily_count[d] >= daily_limit:
            continue
        daily_count[d] += 1

        ep = sig['entry_price']
        future = sig['future']

        position = 1.0  # 剩余仓位
        realized = 0.0  # 已实现收益（按比例加权）
        remaining = 1.0  # 剩余仓位比例
        closed = False

        for day_idx, fp in enumerate(future):
            hd = day_idx + 1
            day_return_low = (fp['l'] - ep) / ep
            day_return_high = (fp['h'] - ep) / ep

            # 止损 — 全部仓位
            if day_return_low <= sl:
                realized += remaining * sl
                remaining = 0
                exit_stats['sl_hit'] += 1
                closed = True
                break

            # 第一止盈 — 卖出一部分
            if remaining > (1 - sell_ratio) + 0.001 and day_return_high >= tp1:
                # tp1触发，卖出 sell_ratio
                realized += sell_ratio * tp1
                remaining -= sell_ratio
                exit_stats['tp1_hit'] += 1
                # 继续持有剩余仓位

            # 第二止盈 — 全部清仓
            if remaining > 0 and day_return_high >= tp2:
                realized += remaining * tp2
                remaining = 0
                exit_stats['tp2_hit'] += 1
                closed = True
                break

            # 超时
            if hd >= mh:
                if remaining > 0:
                    realized += remaining * ((fp['c'] - ep) / ep)
                    remaining = 0
                exit_stats['timeout'] += 1
                closed = True
                break

        # 数据截止
        if not closed:
            if remaining > 0 and future:
                realized += remaining * ((future[-1]['c'] - ep) / ep)
            elif not future:
                pass  # 无数据

        trades.append(realized)

    if not trades:
        return None, exit_stats

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

    result = {
        'n': n, 'wr': wr, 'avg_w': avg_w, 'avg_l': avg_l, 'rr': rr,
        'ev': ev, 'pf': pf, 'total': trades.sum(), 'med': np.median(trades),
        'sl': sl, 'tp1': tp1, 'tp2': tp2, 'sell_ratio': sell_ratio, 'mh': mh,
    }
    return result, exit_stats


def main():
    print("=" * 70)
    print("小卓策略v4 · 分批止盈优化")
    print(f"回测区间: {BACKTEST_START} ~ {BACKTEST_END}")
    print("=" * 70)

    print("\n📊 生成v4信号...")
    signals = generate_signals()
    print(f"  去重后信号数: {len(signals)}")

    # 方案设计
    # 止损: -6%, -8%
    # tp1(第一止盈): +3%, +5%, +8%
    # tp2(第二止盈): +10%, +15%, +20%
    # sell_ratio: 0.5(卖半), 0.3(卖三成)
    # 持仓: 10天, 15天
    combos = list(product(
        [-0.06, -0.08],          # 止损
        [0.03, 0.05, 0.08],      # tp1
        [0.10, 0.15, 0.20],      # tp2
        [0.5, 0.3],              # 卖出比例
        [10, 15],                # 持仓天数
    ))
    # 过滤：tp1必须 < tp2
    combos = [(sl, tp1, tp2, sr, mh) for sl, tp1, tp2, sr, mh in combos if tp1 < tp2]
    print(f"\n🔬 网格搜索 {len(combos)} 组分批方案...")

    results = []
    for i, (sl, tp1, tp2, sr, mh) in enumerate(combos):
        r, stats = backtest_batch(signals, sl, tp1, tp2, sr, mh)
        if r:
            results.append(r)
        if (i + 1) % 20 == 0:
            print(f"  进度: {i+1}/{len(combos)}")

    print(f"  完成! 共 {len(results)} 个有效组合")

    # 对比基准：不分批，直接-8%/+20%/15天
    print("\n📏 基准（不分批 -8%/+20%/15天）...")
    baseline = backtest_batch(signals, -0.08, 0.20, 0.20, 0.0, 15)
    if baseline[0]:
        b = baseline[0]
        print(f"  笔数:{b['n']} 胜率:{b['wr']:.1%} 盈亏比:{b['rr']:.2f} "
              f"期望值:{b['ev']:.2%} 总收益:{b['total']:.1%}")

    # 排序输出
    print("\n" + "=" * 70)
    print("🏆 TOP 20 — 按期望值排序（胜率>40%）")
    print("=" * 70)
    filtered = [r for r in results if r['ev'] > 0 and r['wr'] > 0.40]
    by_ev = sorted(filtered, key=lambda x: -x['ev'])
    print(f"{'止损':>5} {'tp1':>5} {'tp2':>5} {'卖出':>4} {'持仓':>4} "
          f"{'笔数':>4} {'胜率':>6} {'盈亏比':>6} {'期望值':>7} {'总收益':>8} {'盈亏因子':>6}")
    print("-" * 76)
    for r in by_ev[:20]:
        print(f"{r['sl']:>5.0%} {r['tp1']:>5.0%} {r['tp2']:>5.0%} "
              f"{'%d%%' % (r['sell_ratio']*100):>4} {r['mh']:>3}天 "
              f"{r['n']:>4} {r['wr']:>5.1%} {r['rr']:>6.2f} "
              f"{r['ev']:>6.2%} {r['total']:>7.1%} {r['pf']:>6.2f}")

    print("\n" + "=" * 70)
    print("🏆 TOP 20 — 按胜率排序（期望值>0）")
    print("=" * 70)
    by_wr = sorted(filtered, key=lambda x: -x['wr'])
    print(f"{'止损':>5} {'tp1':>5} {'tp2':>5} {'卖出':>4} {'持仓':>4} "
          f"{'笔数':>4} {'胜率':>6} {'盈亏比':>6} {'期望值':>7} {'总收益':>8} {'盈亏因子':>6}")
    print("-" * 76)
    for r in by_wr[:20]:
        print(f"{r['sl']:>5.0%} {r['tp1']:>5.0%} {r['tp2']:>5.0%} "
              f"{'%d%%' % (r['sell_ratio']*100):>4} {r['mh']:>3}天 "
              f"{r['n']:>4} {r['wr']:>5.1%} {r['rr']:>6.2f} "
              f"{r['ev']:>6.2%} {r['total']:>7.1%} {r['pf']:>6.2f}")

    print("\n" + "=" * 70)
    print("🏆 TOP 20 — 按总收益排序（胜率>40%）")
    print("=" * 70)
    by_total = sorted(filtered, key=lambda x: -x['total'])
    print(f"{'止损':>5} {'tp1':>5} {'tp2':>5} {'卖出':>4} {'持仓':>4} "
          f"{'笔数':>4} {'胜率':>6} {'盈亏比':>6} {'期望值':>7} {'总收益':>8} {'盈亏因子':>6}")
    print("-" * 76)
    for r in by_total[:20]:
        print(f"{r['sl']:>5.0%} {r['tp1']:>5.0%} {r['tp2']:>5.0%} "
              f"{'%d%%' % (r['sell_ratio']*100):>4} {r['mh']:>3}天 "
              f"{r['n']:>4} {r['wr']:>5.1%} {r['rr']:>6.2f} "
              f"{r['ev']:>6.2%} {r['total']:>7.1%} {r['pf']:>6.2f}")

    # 精选方案对比
    print("\n" + "=" * 70)
    print("⭐ 精选方案详细对比")
    print("=" * 70)

    pick_combos = [
        (-0.08, 0.05, 0.15, 0.5, 15, "赚5%卖半，剩等15%"),
        (-0.08, 0.05, 0.20, 0.5, 15, "赚5%卖半，剩等20%"),
        (-0.08, 0.08, 0.15, 0.5, 15, "赚8%卖半，剩等15%"),
        (-0.08, 0.08, 0.20, 0.5, 15, "赚8%卖半，剩等20%"),
        (-0.08, 0.05, 0.10, 0.5, 15, "赚5%卖半，剩等10%"),
        (-0.06, 0.05, 0.15, 0.5, 15, "止损6%/赚5%卖半/剩等15%"),
        (-0.06, 0.05, 0.20, 0.5, 15, "止损6%/赚5%卖半/剩等20%"),
    ]

    for sl, tp1, tp2, sr, mh, desc in pick_combos:
        r, stats = backtest_batch(signals, sl, tp1, tp2, sr, mh)
        if r:
            print(f"\n  📌 {desc}")
            print(f"     止损{sl:.0%} / tp1={tp1:.0%}卖{sr:.0%} / tp2={tp2:.0%}清仓 / 持{mh}天")
            print(f"     胜率 {r['wr']:.1%} | 盈亏比 {r['rr']:.2f} | 期望值 {r['ev']:.2%} | 总收益 {r['total']:.1%}")
            print(f"     出场: tp1触发{stats['tp1_hit']}次 tp2触发{stats['tp2_hit']}次 止损{stats['sl_hit']}次 超时{stats['timeout']}次")

    # 保存
    out_path = "/root/.openclaw/workspace/finance_tool/backtest/v4_batch_optimization.json"
    with open(out_path, 'w') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n📁 完整结果: {out_path}")


if __name__ == "__main__":
    main()
