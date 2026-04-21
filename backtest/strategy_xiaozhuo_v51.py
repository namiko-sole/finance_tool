#!/usr/bin/env python3
"""
小卓策略v5.1 — 次日开盘入场 + 时间阶梯止损

基于v5，增加时间阶梯止损：
  tp1(+5%卖30%)触发后，剩余70%的止损线随持仓时间收紧：
    持仓1-5天：  止损-8%（给足时间等tp2）
    持仓6-10天： 止损-3%（开始收紧）
    持仓11-15天：止损 0%（保本）

其他条件同v5：
  - 次日开盘价入场（高开>7%/低开>5%放弃）
  - 止损-8% / 赚5%卖30% / 剩余等20%清仓 / 最大持仓15天
"""
import os, glob, warnings, json
import numpy as np
import pandas as pd
warnings.filterwarnings('ignore')

DATA_DIR = "/root/.openclaw/workspace/data/raw/stock_daily/"
BACKTEST_START = "20230101"
BACKTEST_END = "20260417"

# 时间阶梯止损参数
TIER1_DAYS = 5    # 1-5天
TIER1_SL = -0.08
TIER2_DAYS = 10   # 6-10天
TIER2_SL = -0.03
TIER3_SL = 0.0    # 11-15天


def get_stop_loss(hold_day, tp1_done):
    """根据持仓天数和tp1是否触发，返回当前止损线"""
    if not tp1_done:
        return -0.08  # tp1未触发，保持原始止损
    # tp1已触发，时间阶梯收紧
    if hold_day <= TIER1_DAYS:
        return TIER1_SL
    elif hold_day <= TIER2_DAYS:
        return TIER2_SL
    else:
        return TIER3_SL


def generate_signals():
    """v5信号生成（次日开盘入场）"""
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
                    next_idx = check_idx + 1
                    if next_idx >= n:
                        continue
                    entry_price = o[next_idx]
                    if entry_price <= 0:
                        continue
                    next_gap = (entry_price - signal_close) / signal_close
                    if next_gap > 0.07 or next_gap < -0.05:
                        skip_gap += 1
                        continue

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

    seen = set()
    unique = []
    for s in signals:
        key = (s['code'], s['entry_date'])
        if key not in seen:
            seen.add(key)
            unique.append(s)
    print(f"\n[信号生成] {len(unique)} 个信号 (跳过跳空: {skip_gap})")
    return unique


def backtest_batch(signals, sl_base, tp1, tp2, sell_ratio, mh, daily_limit=3):
    """分批止盈 + 时间阶梯止损"""
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

            # 当前止损线（时间阶梯）
            current_sl = get_stop_loss(hd, tp1_done)

            # 检查止损
            if day_low <= current_sl:
                realized += remaining * current_sl
                remaining = 0
                # 标注止损类型
                if not tp1_done:
                    exit_reason = '止损(初始-8%)'
                elif current_sl == TIER1_SL:
                    exit_reason = '时间止损(1-5天,-8%)'
                elif current_sl == TIER2_SL:
                    exit_reason = f'时间止损(6-10天,-3%)'
                else:
                    exit_reason = f'时间止损(11-15天,保本)'
                closed = True
                break

            # tp1: 赚5%卖30%
            if not tp1_done and remaining > (1 - sell_ratio) + 0.001 and day_high >= tp1:
                realized += sell_ratio * tp1
                remaining -= sell_ratio
                tp1_done = True

            # tp2: 赚20%清仓
            if remaining > 0 and day_high >= tp2:
                realized += remaining * tp2
                remaining = 0
                exit_reason = '止盈(+20%)'
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
    print("  小卓策略v5.1 — 时间阶梯止损")
    print("  tp1触发后止损线随时间收紧:")
    print("    1-5天: -8% | 6-10天: -3% | 11-15天: 0%")
    print("  其他: 次日开盘入场 / 赚5%卖30% / 剩余等20%")
    print("=" * 60)

    print("\n[1] 生成信号...")
    signals = generate_signals()
    if not signals:
        print("没有信号！")
        return

    # ─── 对比回测 ───
    print("\n[2] v5.1回测（时间阶梯止损）...")
    r_new, d_new = backtest_batch(signals, -0.08, 0.05, 0.20, 0.3, 15)

    print("\n[3] v5基准回测（固定-8%止损，无阶梯）...")
    r_base, d_base = backtest_batch(signals, -0.08, 0.05, 0.20, 0.3, 15)

    if not r_new:
        print("回测无结果！")
        return

    # ─── 对比输出 ───
    print("\n" + "=" * 60)
    print("  v5(固定止损) vs v5.1(时间阶梯止损)")
    print("=" * 60)

    # v5基准没有时间阶梯，需要用原始backtest逻辑
    # 重新跑一个无阶梯版本
    sig_sorted = sorted(signals, key=lambda x: (x['entry_date'], x['code']))
    daily_count = {}
    base_trades = []
    base_details = []
    for sig in sig_sorted:
        d = sig['entry_date']
        daily_count.setdefault(d, 0)
        if daily_count[d] >= 3:
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
            if day_low <= -0.08:
                realized += remaining * (-0.08)
                remaining = 0
                exit_reason = '止损(-8%)'
                closed = True
                break
            if not tp1_done and remaining > 0.7 + 0.001 and day_high >= 0.05:
                realized += 0.3 * 0.05
                remaining -= 0.3
                tp1_done = True
            if remaining > 0 and day_high >= 0.20:
                realized += remaining * 0.20
                remaining = 0
                exit_reason = '止盈(+20%)'
                closed = True
                break
            if hd >= 15:
                if remaining > 0:
                    realized += remaining * ((fp['c'] - ep) / ep)
                    remaining = 0
                exit_reason = '持15天'
                closed = True
                break
        if not closed:
            if remaining > 0 and future:
                realized += remaining * ((future[-1]['c'] - ep) / ep)
                exit_reason = '数据截止'
            else:
                exit_reason = '无数据'
        base_trades.append(realized)
        base_details.append({'pnl': realized, 'reason': exit_reason, 'tp1_done': tp1_done})

    arr_b = np.array(base_trades)
    wins_b = arr_b[arr_b > 0]
    losses_b = arr_b[arr_b <= 0]
    r_base = {
        'n': len(arr_b), 'wr': len(wins_b)/len(arr_b),
        'rr': abs(wins_b.mean()/losses_b.mean()) if len(losses_b)>0 and losses_b.mean()!=0 else 999,
        'ev': arr_b.mean(), 'pf': abs(wins_b.sum()/losses_b.sum()) if losses_b.sum()!=0 else 999,
        'total': arr_b.sum(), 'med': np.median(arr_b),
    }

    print(f"{'指标':<12} {'v5(固定-8%)':<20} {'v5.1(时间阶梯)':<20} {'差异':<15}")
    print("-" * 67)
    for label, key, fmt in [
        ('交易笔数', 'n', 'd'),
        ('胜率', 'wr', '.1%'),
        ('盈亏比', 'rr', '.2f'),
        ('期望值', 'ev', '.2%'),
        ('盈利因子', 'pf', '.2f'),
        ('累计收益', 'total', '.1%'),
        ('中位数', 'med', '.2%'),
    ]:
        vb = r_base[key]
        vn = r_new[key]
        if fmt == 'd':
            diff = int(vn) - int(vb)
            print(f"{label:<12} {int(vb):<20} {int(vn):<20} {diff:+d}")
        elif fmt == '.1%':
            diff = vn - vb
            print(f"{label:<12} {vb:.1%}{'':>15} {vn:.1%}{'':>15} {diff:+.1%}")
        else:
            diff = vn - vb
            print(f"{label:<12} {vb:{fmt}}{'':>{16-len(f'{vb:{fmt}}')}} {vn:{fmt}}{'':>{16-len(f'{vn:{fmt}}')}} {diff:+{fmt}}")

    # ─── v5.1详细分析 ───
    df = pd.DataFrame(d_new)

    print("\n" + "=" * 60)
    print("  v5.1 出场原因统计")
    print("=" * 60)
    for reason, sub in df.groupby('reason'):
        wr = (sub['pnl'] > 0).mean()
        print(f"  {reason:>22s}: {len(sub):4d}笔 ({len(sub)/len(df):5.1%})  "
              f"胜率{wr:.1%}  均收{sub['pnl'].mean():.2%}  累计{sub['pnl'].sum():.1%}")

    # ─── 关键对比：tp1触发后被时间止损救回来的 ───
    print("\n" + "=" * 60)
    print("  关键分析：tp1触发后的命运")
    print("=" * 60)
    tp1_yes = df[df['tp1_done']]
    tp1_no = df[~df['tp1_done']]
    print(f"\n  tp1触发: {len(tp1_yes)}笔  胜率{(tp1_yes['pnl']>0).mean():.1%}  "
          f"均收{tp1_yes['pnl'].mean():.2%}  累计{tp1_yes['pnl'].sum():.1%}")
    print(f"  tp1未触发: {len(tp1_no)}笔  胜率{(tp1_no['pnl']>0).mean():.1%}  "
          f"均收{tp1_no['pnl'].mean():.2%}  累计{tp1_no['pnl'].sum():.1%}")

    # tp1触发后按出场原因
    print(f"\n  tp1触发后出场分布:")
    for reason, sub in tp1_yes.groupby('reason'):
        wr = (sub['pnl'] > 0).mean()
        print(f"    {reason:>22s}: {len(sub):3d}笔  均收{sub['pnl'].mean():.2%}  累计{sub['pnl'].sum():.1%}")

    # ─── 年度对比 ───
    print("\n" + "=" * 60)
    print("  年度对比")
    print("=" * 60)
    df['year'] = df['signal_date'].astype(str).str[:4]
    df_b = pd.DataFrame(base_details)
    df_b['year'] = pd.DataFrame(signals).sort_values(['entry_date','code']).reset_index(drop=True).loc[:len(df_b)-1, 'signal_date'].astype(str).str[:4] if len(df_b) > 0 else ''

    # 简单年度对比
    print(f"\n  {'年度':<6} {'v5笔数':>6} {'v5胜率':>8} {'v5累计':>10} │ {'v5.1笔数':>8} {'v5.1胜率':>9} {'v5.1累计':>10}")
    print("  " + "-" * 65)
    df_b_full = pd.DataFrame(base_details)
    sig_sorted_df = pd.DataFrame(signals).sort_values(['entry_date','code']).reset_index(drop=True)
    df_b_full['year'] = sig_sorted_df.loc[:len(df_b_full)-1, 'signal_date'].astype(str).str[:4]

    for yr in sorted(df['year'].unique()):
        s_new = df[df['year'] == yr]
        s_base = df_b_full[df_b_full['year'] == yr]
        if len(s_base) > 0:
            print(f"  {yr:<6} {len(s_base):>6} {(s_base['pnl']>0).mean():>7.1%} {s_base['pnl'].sum():>9.1%} │ "
                  f"{len(s_new):>8} {(s_new['pnl']>0).mean():>8.1%} {s_new['pnl'].sum():>9.1%}")

    # 保存
    out_dir = "/root/.openclaw/workspace/finance_tool/backtest/results/"
    os.makedirs(out_dir, exist_ok=True)
    df.to_csv(os.path.join(out_dir, 'xiaozhuo_v51_trades.csv'), index=False)
    print(f"\n已保存: {out_dir}xiaozhuo_v51_trades.csv")


if __name__ == "__main__":
    main()
