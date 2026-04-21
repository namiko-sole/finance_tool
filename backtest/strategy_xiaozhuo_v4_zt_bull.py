#!/usr/bin/env python3
"""
小卓策略v4回测 — 对比多头排列检查时机
v3: 信号日(check_idx)检查多头排列
v4: 涨停日(zt_idx)检查多头排列（更合理：确保涨停发生在多头趋势中）

其他条件完全相同：
  - 近15天有涨停，非ST/非科创/非创业板/非北交所
  - 缩量到涨停日量能50%以下
  - 缩量期间最小成交额 <= 1亿
  - 价格回到MA10附近（收盘价 >= MA10*0.98）
  - 底分型 + 阳线确认
  - 20日区间位置 < 0.5

出场: 止损-3% / 止盈+20% / 最大持仓5天
"""
import os, glob, warnings
import numpy as np
import pandas as pd
warnings.filterwarnings('ignore')

DATA_DIR = "/root/.openclaw/workspace/data/raw/stock_daily/"
BACKTEST_START = "20230101"
BACKTEST_END = "20260417"


def generate_signals(bull_check_mode='zt_day'):
    """
    bull_check_mode:
      'signal_day' - v3原版：信号日(check_idx)检查多头排列
      'zt_day'     - v4新版：涨停日(zt_idx)检查多头排列
    """
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    signals = []

    for fi, f in enumerate(files):
        ts_code = os.path.basename(f).replace('.csv', '')
        # 过滤：科创/创业板/北交所
        if ts_code[:3] in ('688', '300', '301') or ts_code[0] in ('8', '4'):
            continue
        if ts_code.endswith('.BJ'):
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
            amt = df['amount'].values  # 千元
            n = len(df)

            # 均线
            ma5 = pd.Series(c).rolling(5).mean().values
            ma10 = pd.Series(c).rolling(10).mean().values
            ma20 = pd.Series(c).rolling(20).mean().values

            # 多头排列
            bull = (ma5 > ma10) & (ma10 > ma20)

            # 涨停标记
            is_zt = df['pct_chg'].values >= 9.8
            zt_indices = np.where(is_zt)[0]

            for zt_idx in zt_indices:
                zt_date = str(df['trade_date'].iloc[zt_idx])
                if zt_date < BACKTEST_START or zt_date > BACKTEST_END:
                    continue

                zt_vol = v[zt_idx]
                if zt_vol <= 0:
                    continue

                # ─── v4核心改动：在涨停日检查多头排列 ───
                if bull_check_mode == 'zt_day':
                    if not bull[zt_idx]:
                        continue

                shrink_target = zt_vol * 0.5

                for day_offset in range(1, 16):
                    check_idx = zt_idx + day_offset
                    if check_idx >= n or check_idx < 2:
                        break

                    date = str(df['trade_date'].iloc[check_idx])
                    if date > BACKTEST_END:
                        break

                    # ─── v3原版：在信号日检查多头 ───
                    if bull_check_mode == 'signal_day':
                        if not bull[check_idx]:
                            continue

                    # 条件: 缩量检查
                    post_zt_vols = v[zt_idx + 1:check_idx + 1]
                    post_zt_amts = amt[zt_idx + 1:check_idx + 1]

                    has_shrunk = np.any(post_zt_vols <= shrink_target)
                    if not has_shrunk:
                        continue

                    # 条件: 最小成交额 <= 1亿
                    min_amt = float(np.min(post_zt_amts))
                    if min_amt > 100000:
                        continue

                    # 条件: MA10附近
                    if ma10[check_idx] <= 0:
                        continue
                    price_vs_ma10 = c[check_idx] / ma10[check_idx]
                    if price_vs_ma10 < 0.98:
                        continue

                    # 条件: 底分型
                    is_bottom = (
                        l[check_idx - 1] < l[check_idx - 2] and
                        l[check_idx - 1] < l[check_idx]
                    )
                    if not is_bottom:
                        continue

                    # 条件: 阳线确认
                    if c[check_idx] <= o[check_idx]:
                        continue

                    # 条件: 20日区间位置 < 0.5
                    window_start = max(0, check_idx - 19)
                    high_20d = float(np.max(c[window_start:check_idx + 1]))
                    low_20d = float(np.min(c[window_start:check_idx + 1]))
                    if high_20d > low_20d:
                        pos_20d = (c[check_idx] - low_20d) / (high_20d - low_20d)
                    else:
                        pos_20d = 0.5
                    if pos_20d >= 0.5:
                        continue

                    entry_price = c[check_idx]
                    if entry_price <= 0:
                        continue

                    # 收集后续价格
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
                        'zt_vol': float(zt_vol),
                        'min_shrink_vol': float(np.min(post_zt_vols)),
                        'min_amt': float(min_amt),
                        'price_vs_ma10': float(price_vs_ma10),
                        'pos_20d': float(pos_20d),
                        'day_after_zt': day_offset,
                        'mode': bull_check_mode,
                        'future': future,
                    })

                    break  # 这个涨停只取第一个信号

        except Exception as e:
            continue

        if (fi + 1) % 1000 == 0:
            print(f"  [{bull_check_mode}] 已扫描 {fi+1}/{len(files)} 文件, 信号数: {len(signals)}")

    return signals


def backtest(signals, sl=-0.03, tp=0.20, mh=5, daily_limit=3):
    """回测：止损/止盈/最大持仓天数"""
    sig_sorted = sorted(signals, key=lambda x: (x['entry_date'], x['code']))
    # 去重：同一天同一股票只保留一个信号
    seen = set()
    unique_sigs = []
    for sig in sig_sorted:
        key = (sig['code'], sig['entry_date'])
        if key not in seen:
            seen.add(key)
            unique_sigs.append(sig)
    sig_sorted = unique_sigs

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
        closed = False
        exit_reason = ''
        pnl = 0

        for day_idx, fp in enumerate(future):
            hd = day_idx + 1
            if (fp['l'] - ep) / ep <= sl:
                pnl = sl
                exit_reason = '止损'
                closed = True
                break
            if (fp['h'] - ep) / ep >= tp:
                pnl = tp
                exit_reason = '止盈'
                closed = True
                break
            if hd >= mh:
                pnl = (fp['c'] - ep) / ep
                exit_reason = f'持{mh}天'
                closed = True
                break

        if not closed and future:
            pnl = (future[-1]['c'] - ep) / ep
            exit_reason = '数据截止'
        elif not closed:
            pnl = 0
            exit_reason = '无数据'

        trades.append(pnl)
        trade_details.append({
            'code': sig['code'],
            'date': sig['entry_date'],
            'entry': ep,
            'pnl': pnl,
            'reason': exit_reason,
            'day_after_zt': sig['day_after_zt'],
        })

    if not trades:
        return None, []

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
    }
    return result, trade_details


def main():
    print("=" * 60)
    print("小卓策略回测：多头排列检查时机对比")
    print(f"回测区间: {BACKTEST_START} ~ {BACKTEST_END}")
    print(f"出场规则: 止损-3% / 止盈+20% / 最大持仓5天")
    print("=" * 60)

    # v3: 信号日检查多头
    print("\n📊 v3: 信号日检查多头排列...")
    sig_v3 = generate_signals('signal_day')
    print(f"  信号数: {len(sig_v3)}")
    res_v3, det_v3 = backtest(sig_v3)

    # v4: 涨停日检查多头
    print("\n📊 v4: 涨停日检查多头排列...")
    sig_v4 = generate_signals('zt_day')
    print(f"  信号数: {len(sig_v4)}")
    res_v4, det_v4 = backtest(sig_v4)

    # 对比输出
    print("\n" + "=" * 60)
    print("📊 对比结果")
    print("=" * 60)
    print(f"{'指标':<12} {'v3(信号日多头)':<18} {'v4(涨停日多头)':<18}")
    print("-" * 48)

    if res_v3 and res_v4:
        metrics = [
            ('信号数', 'n', 'd'),
            ('胜率', 'wr', '.1%'),
            ('平均盈利', 'avg_w', '.2%'),
            ('平均亏损', 'avg_l', '.2%'),
            ('盈亏比', 'rr', '.2f'),
            ('期望值', 'ev', '.2%'),
            ('盈亏因子', 'pf', '.2f'),
            ('总收益', 'total', '.1%'),
            ('中位数', 'med', '.2%'),
        ]
        for label, key, fmt in metrics:
            v3_val = res_v3[key]
            v4_val = res_v4[key]
            if fmt == 'd':
                print(f"{label:<12} {int(v3_val):<18} {int(v4_val):<18}")
            elif fmt == '.1%':
                print(f"{label:<12} {v3_val:.1%}{'':>13} {v4_val:.1%}")
            else:
                print(f"{label:<12} {v3_val:{fmt}}{'':>{14-len(f'{v3_val:{fmt}}')}} {v4_val:{fmt}}")

    # v4详细交易
    if det_v4:
        print(f"\n{'='*60}")
        print(f"📋 v4(涨停日多头) 最近20笔交易")
        print(f"{'='*60}")
        for t in det_v4[-20:]:
            emoji = '✅' if t['pnl'] > 0 else '❌'
            print(f"  {emoji} {t['code']} {t['date']} 入{t['entry']:.2f} "
                  f"收{t['pnl']:.1%} {t['reason']} 涨停后{t['day_after_zt']}天")

    # v4按年度统计
    if det_v4:
        print(f"\n{'='*60}")
        print(f"📈 v4 年度统计")
        print(f"{'='*60}")
        by_year = {}
        for t in det_v4:
            yr = t['date'][:4]
            by_year.setdefault(yr, []).append(t['pnl'])
        for yr in sorted(by_year.keys()):
            arr = np.array(by_year[yr])
            wr = (arr > 0).mean()
            print(f"  {yr}: {len(arr)}笔 | 胜率{wr:.0%} | 总{arr.sum():.1%} | 均{arr.mean():.2%}")


if __name__ == "__main__":
    main()
