#!/usr/bin/env python3
"""
小卓策略v3回测 — 对比两种缩量基准
标准A: 缩到涨停当天量能的50%
标准B: 缩到涨停前后上涨波段中最大量能的50%

入场条件:
  - 近15天有涨停，非ST/非科创(688)/非创业板(300)/非北交所
  - 流通市值<50亿（用成交额近似）
  - MA5 > MA10 > MA20 多头排列
  - 缩量到基准量的50%以下
  - 缩量期间最小成交额 <= 1亿
  - 价格回到10日均线附近（收盘价 >= MA10*0.98）
  - 底分型（中间K线最低，两边高）

出场: 止损/止盈/最大持仓天数
"""
import os, glob, warnings
import numpy as np
import pandas as pd
warnings.filterwarnings('ignore')

DATA_DIR = "/root/.openclaw/workspace/data/raw/stock_daily/"
BACKTEST_START = "20230101"
BACKTEST_END = "20260415"


def generate_signals(vol_base_mode='zt_day'):
    """
    vol_base_mode:
      'zt_day'  - 基准=涨停当天的量
      'swing_max' - 基准=涨停前5天到涨停日(含)中量最大的那天
    """
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    signals = []
    
    for fi, f in enumerate(files):
        ts_code = os.path.basename(f).replace('.csv', '')
        if ts_code[:3] in ('688', '300') or ts_code[0] in ('8', '4'):
            continue
        try:
            df = pd.read_csv(f, encoding='utf-8-sig').sort_values('trade_date').reset_index(drop=True)
            if len(df) < 60: continue
            
            c = df['close'].values
            o = df['open'].values
            h = df['high'].values
            l = df['low'].values
            v = df['vol'].values
            amt = df['amount'].values  # 单位：千元
            n = len(df)
            
            # 均线
            ma5 = pd.Series(c).rolling(5).mean().values
            ma10 = pd.Series(c).rolling(10).mean().values
            ma20 = pd.Series(c).rolling(20).mean().values
            
            # 多头排列
            bull = (ma5 > ma10) & (ma10 > ma20)
            
            # 涨停标记
            is_zt = df['pct_chg'].values >= 9.8
            
            # 找所有涨停日
            zt_indices = np.where(is_zt)[0]
            
            for zt_idx in zt_indices:
                zt_date = str(df['trade_date'].iloc[zt_idx])
                if zt_date < BACKTEST_START or zt_date > BACKTEST_END:
                    continue
                
                zt_vol = v[zt_idx]  # 涨停当天量
                
                # ─── 确定缩量基准 ───
                if vol_base_mode == 'zt_day':
                    vol_base = zt_vol
                else:  # swing_max
                    # 涨停前5天到涨停日(含)，量最大的一天
                    start_idx = max(0, zt_idx - 5)
                    vol_base = np.max(v[start_idx:zt_idx + 1])
                
                if vol_base <= 0:
                    continue
                
                shrink_target = vol_base * 0.5  # 缩到50%
                
                # ─── 在涨停后1~15天内寻找入场信号 ───
                for day_offset in range(1, 16):  # 涨停后1-15天
                    check_idx = zt_idx + day_offset
                    if check_idx >= n or check_idx < 2:
                        break
                    
                    date = str(df['trade_date'].iloc[check_idx])
                    if date > BACKTEST_END:
                        break
                    
                    # 条件1: 多头排列
                    if not bull[check_idx]:
                        continue
                    
                    # 条件2: 缩量检查 - 从涨停后到当前，是否有至少1天量缩到基准50%以下
                    post_zt_vols = v[zt_idx + 1:check_idx + 1]
                    post_zt_amts = amt[zt_idx + 1:check_idx + 1]
                    
                    has_shrunk = np.any(post_zt_vols <= shrink_target)
                    if not has_shrunk:
                        continue
                    
                    # 条件3: 缩量期间最小成交额 <= 1亿 (1亿 = 100000千元)
                    min_amt = np.min(post_zt_amts)
                    if min_amt > 100000:  # 1亿
                        continue
                    
                    # 条件4: 10日均线附近 - 收盘价 >= MA10 * 0.98
                    if ma10[check_idx] <= 0:
                        continue
                    price_vs_ma10 = c[check_idx] / ma10[check_idx]
                    if price_vs_ma10 < 0.98:
                        continue
                    
                    # 条件5: 底分型（中间K线最低，两边高）
                    # check_idx 作为中间K线，check_idx-1 和 check_idx+1 作为两边
                    # 但我们需要在确认日（第三根K线）入场
                    # 标准底分型：bar[i-1] low < bar[i-2] low 且 bar[i-1] low < bar[i] low
                    # 入场点在 bar[i]（第三根确认K线）
                    
                    # 这里 check_idx 是我们要检查的位置
                    # 底分型：前一根(check_idx-1)的low比前前一根和当前都低
                    if check_idx < 2:
                        continue
                    
                    is_bottom = (
                        l[check_idx - 1] < l[check_idx - 2] and
                        l[check_idx - 1] < l[check_idx]
                    )
                    if not is_bottom:
                        continue
                    
                    # 条件5b: 底分型最后一根K线必须是阳线
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
                    if pos_20d >= 0.5:
                        continue
                    
                    # ─── 入场价 = 当天收盘价 ───
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
                        'vol_base': float(vol_base),
                        'zt_vol': float(zt_vol),
                        'min_shrink_vol': float(np.min(post_zt_vols)),
                        'min_amt': float(min_amt),
                        'price_vs_ma10': float(price_vs_ma10),
                        'day_after_zt': day_offset,
                        'mode': vol_base_mode,
                        'future': future,
                    })
                    
                    # 只取第一个满足条件的信号（最近的入场点）
                    break  # 这个涨停只取第一个信号
            
        except Exception as e:
            continue
        
        if (fi + 1) % 1000 == 0:
            print(f"  [{vol_base_mode}] 已扫描 {fi+1}/{len(files)} 文件, 信号数: {len(signals)}")
    
    return signals


def test_params(signals, sl, tp, mh, daily_limit=3):
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
        
        for day_idx, fp in enumerate(future):
            hd = day_idx + 1
            if (fp['l'] - ep) / ep <= sl:
                trades.append(sl); closed = True; break
            if (fp['h'] - ep) / ep >= tp:
                trades.append(tp); closed = True; break
            if hd >= mh:
                trades.append((fp['c'] - ep) / ep); closed = True; break
        
        if not closed and future:
            trades.append((future[-1]['c'] - ep) / ep)
        elif not closed:
            trades.append(0)
    
    if not trades:
        return None
    trades = np.array(trades)
    wins = trades[trades > 0]
    losses = trades[trades <= 0]
    n = len(trades)
    wr = len(wins) / n
    avg_w = wins.mean() if len(wins) > 0 else 0
    avg_l = losses.mean() if len(losses) > 0 else 0
    rr = abs(avg_w / avg_l) if avg_l != 0 else 999
    ev = wr * avg_w + (1 - wr) * avg_l
    pf = abs(wins.sum() / losses.sum()) if losses.sum() != 0 else 999
    
    return {'n': n, 'wr': wr, 'avg_w': avg_w, 'avg_l': avg_l, 'rr': rr,
            'ev': ev, 'pf': pf, 'total': trades.sum(), 'med': np.median(trades),
            'sl': sl, 'tp': tp, 'mh': mh}


# ═══════════════════════════════════════════
# 主程序
# ═══════════════════════════════════════════

print("=" * 80)
print("对比两种缩量基准")
print("A: 涨停当天量能的50%")
print("B: 涨停前后上涨波段最大量能的50%")
print("=" * 80)

# 生成两种信号
print("\n--- 生成标准A信号（涨停当天量）---")
signals_a = generate_signals('zt_day')
print(f"标准A信号数: {len(signals_a)}")

print("\n--- 生成标准B信号（波段最大量）---")
signals_b = generate_signals('swing_max')
print(f"标准B信号数: {len(signals_b)}")

# ─── 自然走势对比 ───
print(f"\n{'='*80}")
print("自然走势对比（无止盈止损）")
print(f"{'='*80}")
for label, signals in [("A(涨停日量)", signals_a), ("B(波段最大量)", signals_b)]:
    print(f"\n  {label}: {len(signals)}信号")
    for hd in [1, 3, 5, 7]:
        pnls = np.array([(s['future'][hd-1]['c'] - s['entry_price']) / s['entry_price']
                          for s in signals if len(s['future']) >= hd])
        if len(pnls) > 0:
            print(f"    持{hd}天: 平均{pnls.mean():+.2%} 中位{np.median(pnls):+.2%} 胜率{(pnls>0).mean():.1%}")

# ─── 参数网格搜索 ───
print(f"\n{'='*80}")
print("参数网格搜索对比")
print(f"{'='*80}")

sl_range = [-0.03, -0.04, -0.05, -0.06, -0.07]
tp_range = [0.08, 0.10, 0.15, 0.20, 0.25, 0.30]
mh_range = [2, 3, 5, 7]

results_a = []
results_b = []

for sl in sl_range:
    for tp in tp_range:
        for mh in mh_range:
            r = test_params(signals_a, sl, tp, mh)
            if r: 
                r['mode'] = 'A'
                results_a.append(r)
            r = test_params(signals_b, sl, tp, mh)
            if r: 
                r['mode'] = 'B'
                results_b.append(r)

all_results = results_a + results_b
rdf = pd.DataFrame(all_results)

# ─── 各模式最优 ───
for mode, label in [('A', 'A-涨停日量基准'), ('B', 'B-波段最大量基准')]:
    sub = rdf[rdf['mode'] == mode]
    viable = sub[(sub['ev'] > 0) & (sub['pf'] > 1)]
    print(f"\n  {label}:")
    print(f"    信号数来源: {len(signals_a) if mode=='A' else len(signals_b)}")
    print(f"    可行组合(EV>0 & PF>1): {len(viable)}/{len(sub)}")
    
    if len(viable) > 0:
        best = viable.sort_values('ev', ascending=False).iloc[0]
        print(f"    最优(EV): 止损{best['sl']:.0%} 止盈{best['tp']:.0%} 持{best['mh']:.0f}天")
        print(f"      {best['n']:.0f}笔 胜率{best['wr']:.1%} 盈亏比{best['rr']:.2f}:1 EV{best['ev']:+.2%} PF{best['pf']:.2f}")
        
        best_pf = viable.sort_values('pf', ascending=False).iloc[0]
        print(f"    最优(PF): 止损{best_pf['sl']:.0%} 止盈{best_pf['tp']:.0%} 持{best_pf['mh']:.0f}天")
        print(f"      {best_pf['n']:.0f}笔 胜率{best_pf['wr']:.1%} 盈亏比{best_pf['rr']:.2f}:1 EV{best_pf['ev']:+.2%} PF{best_pf['pf']:.2f}")
    else:
        best = sub.sort_values('ev', ascending=False).iloc[0]
        print(f"    ⚠️ 无可行组合, 最接近: EV{best['ev']:+.2%} PF{best['pf']:.2f}")

# ─── 统一参数下直接对比 ───
print(f"\n{'='*80}")
print("统一参数下直接对比")
print(f"{'='*80}")
print(f"{'参数':>20} | {'标准A(涨停日量)':>35} | {'标准B(波段最大量)':>35}")
print("-" * 100)

for sl, tp, mh in [(-0.04, 0.20, 5), (-0.04, 0.30, 3), (-0.05, 0.20, 5), 
                     (-0.05, 0.30, 5), (-0.04, 0.15, 5), (-0.03, 0.30, 5)]:
    ra = test_params(signals_a, sl, tp, mh)
    rb = test_params(signals_b, sl, tp, mh)
    if ra and rb:
        pa = f"{ra['n']:.0f}笔 WR{ra['wr']:.1%} RR{ra['rr']:.2f}:1 EV{ra['ev']:+.2%} PF{ra['pf']:.2f}"
        pb = f"{rb['n']:.0f}笔 WR{rb['wr']:.1%} RR{rb['rr']:.2f}:1 EV{rb['ev']:+.2%} PF{rb['pf']:.2f}"
        print(f"  SL{sl:.0%} TP{tp:.0%} {mh}天 | {pa:>35} | {pb:>35}")

# ─── Top 10 对比 ───
print(f"\n{'='*80}")
print("标准A Top 10 (按EV)")
print(f"{'='*80}")
print(f"{'止损':>6} {'止盈':>6} {'持仓':>4} | {'笔数':>5} {'胜率':>6} {'盈亏比':>7} {'EV':>7} {'PF':>6} {'累计':>9}")
print("-" * 80)
sub_a = rdf[rdf['mode'] == 'A']
viable_a = sub_a[(sub_a['ev'] > 0) & (sub_a['pf'] > 1)]
for _, r in (viable_a.sort_values('ev', ascending=False).head(10) if len(viable_a) > 0 else sub_a.sort_values('ev', ascending=False).head(10)).iterrows():
    print(f"{r['sl']:>6.0%} {r['tp']:>6.0%} {r['mh']:>4.0f}天 | {r['n']:>5.0f} {r['wr']:>5.1%} {r['rr']:>5.2f}:1 {r['ev']:>+6.2%} {r['pf']:>5.2f} {r['total']:>+8.1f}%")

print(f"\n{'='*80}")
print("标准B Top 10 (按EV)")
print(f"{'='*80}")
print(f"{'止损':>6} {'止盈':>6} {'持仓':>4} | {'笔数':>5} {'胜率':>6} {'盈亏比':>7} {'EV':>7} {'PF':>6} {'累计':>9}")
print("-" * 80)
sub_b = rdf[rdf['mode'] == 'B']
viable_b = sub_b[(sub_b['ev'] > 0) & (sub_b['pf'] > 1)]
for _, r in (viable_b.sort_values('ev', ascending=False).head(10) if len(viable_b) > 0 else sub_b.sort_values('ev', ascending=False).head(10)).iterrows():
    print(f"{r['sl']:>6.0%} {r['tp']:>6.0%} {r['mh']:>4.0f}天 | {r['n']:>5.0f} {r['wr']:>5.1%} {r['rr']:>5.2f}:1 {r['ev']:>+6.2%} {r['pf']:>5.2f} {r['total']:>+8.1f}%")
