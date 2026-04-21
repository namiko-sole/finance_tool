#!/usr/bin/env python3
"""
小卓策略改进测试 — 只用vol_state==2的信号 + 不同参数组合
"""
import json
import numpy as np

with open('/root/.openclaw/workspace/finance_tool/backtest/results/xiaozhuo_signals.json') as f:
    all_signals = json.load(f)

# 只取vol_state==2的信号
signals = [s for s in all_signals if s['vol_state'] == 2]
print(f"vol_state==2 信号数: {len(signals)} (总{len(all_signals)})")

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
            pnl_low = (fp['l'] - ep) / ep
            pnl_high = (fp['h'] - ep) / ep
            pnl_close = (fp['c'] - ep) / ep
            
            if pnl_low <= sl:
                trades.append(sl)
                closed = True
                break
            if pnl_high >= tp:
                trades.append(tp)
                closed = True
                break
            if hd >= mh:
                trades.append(pnl_close)
                closed = True
                break
        
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
    wr = len(wins) / n if n > 0 else 0
    avg_w = wins.mean() if len(wins) > 0 else 0
    avg_l = losses.mean() if len(losses) > 0 else 0
    rr = abs(avg_w / avg_l) if avg_l != 0 else 999
    ev = wr * avg_w + (1 - wr) * avg_l
    pf = abs(wins.sum() / losses.sum()) if losses.sum() != 0 else 999
    
    return {'n': n, 'wr': wr, 'avg_w': avg_w, 'avg_l': avg_l, 'rr': rr, 
            'ev': ev, 'pf': pf, 'total': trades.sum(), 'med': np.median(trades),
            'sl': sl, 'tp': tp, 'mh': mh}


# Test broader grid for vol_state==2
results = []
sl_range = [-0.03, -0.04, -0.05, -0.06, -0.07, -0.08, -0.10]
tp_range = [0.05, 0.08, 0.10, 0.12, 0.15, 0.20, 0.25, 0.30]
mh_range = [2, 3, 5, 7, 10]

for sl in sl_range:
    for tp in tp_range:
        for mh in mh_range:
            r = test_params(signals, sl, tp, mh)
            if r:
                results.append(r)

rdf = __import__('pandas').DataFrame(results)

print(f"\n测试了 {len(results)} 种参数组合")

# ─── 可行区域 ───
viable = rdf[(rdf['ev'] > 0) & (rdf['pf'] > 1)]
print(f"\n可行区域（EV>0 且 PF>1）: {len(viable)} / {len(rdf)} 组合")

if len(viable) > 0:
    print(f"\n{'='*90}")
    print("✅ 可行方案 Top 30（按EV排序）")
    print(f"{'='*90}")
    print(f"{'止损':>6} {'止盈':>6} {'持仓':>4} | {'笔数':>5} {'胜率':>6} {'盈亏比':>7} {'EV':>7} {'PF':>6} {'累计':>9}")
    print("-" * 90)
    for _, r in viable.sort_values('ev', ascending=False).head(30).iterrows():
        print(f"{r['sl']:>6.0%} {r['tp']:>6.0%} {r['mh']:>4.0f}天 | {r['n']:>5.0f} {r['wr']:>5.1%} {r['rr']:>5.2f}:1 {r['ev']:>+6.2%} {r['pf']:>5.2f} {r['total']:>+8.1f}%")
    
    # Best per hold period
    print(f"\n{'='*90}")
    print("各持仓天数最优可行参数")
    print(f"{'='*90}")
    for mh in sorted(viable['mh'].unique()):
        sub = viable[viable['mh'] == mh].sort_values('ev', ascending=False)
        if len(sub) > 0:
            b = sub.iloc[0]
            print(f"  {mh}天: 止损{b['sl']:.0%} 止盈{b['tp']:.0%} → {b['n']:.0f}笔 胜率{b['wr']:.1%} 盈亏比{b['rr']:.2f}:1 EV{b['ev']:+.2%} PF{b['pf']:.2f}")
    
    # PF top
    print(f"\n{'='*90}")
    print("按盈利因子(PF)排序 Top 15")
    print(f"{'='*90}")
    print(f"{'止损':>6} {'止盈':>6} {'持仓':>4} | {'笔数':>5} {'胜率':>6} {'盈亏比':>7} {'EV':>7} {'PF':>6} {'累计':>9}")
    print("-" * 90)
    for _, r in viable.sort_values('pf', ascending=False).head(15).iterrows():
        print(f"{r['sl']:>6.0%} {r['tp']:>6.0%} {r['mh']:>4.0f}天 | {r['n']:>5.0f} {r['wr']:>5.1%} {r['rr']:>5.2f}:1 {r['ev']:>+6.2%} {r['pf']:>5.2f} {r['total']:>+8.1f}%")
else:
    print("  ⚠️ vol_state==2也没有找到可行组合！")
    best = rdf.sort_values('ev', ascending=False).iloc[0]
    print(f"  最接近: 止损{best['sl']:.0%} 止盈{best['tp']:.0%} 持{best['mh']:.0f}天 EV{best['ev']:+.2%} PF{best['pf']:.2f}")
    
    print(f"\nTop 10:")
    for _, r in rdf.sort_values('ev', ascending=False).head(10).iterrows():
        print(f"  止损{r['sl']:.0%} 止盈{r['tp']:.0%} 持{r['mh']:.0f}天 → 胜率{r['wr']:.1%} 盈亏比{r['rr']:.2f}:1 EV{r['ev']:+.2%} PF{r['pf']:.2f}")

# ─── 额外：对比原始 vs 改进 ───
print(f"\n{'='*90}")
print("📊 原始策略 vs 改进策略 对比")
print(f"{'='*90}")

# Original: all signals, -7% SL, +15% TP, 5 days
r_orig = test_params(all_signals, -0.07, 0.15, 5)
# Improved: vol_state==2 only, best params
if len(viable) > 0:
    best_params = viable.sort_values('ev', ascending=False).iloc[0]
    r_impr = test_params(signals, best_params['sl'], best_params['tp'], int(best_params['mh']))
else:
    # Use closest
    bp = rdf.sort_values('ev', ascending=False).iloc[0]
    r_impr = test_params(signals, bp['sl'], bp['tp'], int(bp['mh']))

print(f"\n  原始(全信号, -7%/+15%/5天):")
print(f"    {r_orig['n']:.0f}笔 胜率{r_orig['wr']:.1%} 盈亏比{r_orig['rr']:.2f}:1 EV{r_orig['ev']:+.2%} PF{r_orig['pf']:.2f}")

print(f"\n  改进(vol_state==2, {best_params['sl']:.0%}/{best_params['tp']:.0%}/{best_params['mh']:.0f}天):")
print(f"    {r_impr['n']:.0f}笔 胜率{r_impr['wr']:.1%} 盈亏比{r_impr['rr']:.2f}:1 EV{r_impr['ev']:+.2%} PF{r_impr['pf']:.2f}")
