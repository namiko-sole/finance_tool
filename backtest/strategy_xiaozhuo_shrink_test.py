#!/usr/bin/env python3
"""
测试不同的缩量确认标准：
A) 放量后出现1天缩量（原始标准）
B) 放量后连续2天缩量
C) 放量后连续3天缩量
D) 放量后缩量的天数 >= 调整天数的一半（缩量占主导）
"""
import json, os, glob, warnings
import numpy as np
import pandas as pd
warnings.filterwarnings('ignore')

DATA_DIR = "/root/.openclaw/workspace/data/raw/stock_daily/"
BACKTEST_START = "20230101"
BACKTEST_END = "20260415"

def load_and_signal(shrink_days_required=1, shrink_ratio=0.7, breakout_ratio=1.8):
    """
    生成信号，控制缩量标准
    shrink_days_required: 需要连续缩量多少天才算到位
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
            
            c, o, h, l, v = df['close'].values, df['open'].values, df['high'].values, df['low'].values, df['vol'].values
            n = len(df)
            
            ma5 = pd.Series(c).rolling(5).mean().values
            ma10 = pd.Series(c).rolling(10).mean().values
            ma20 = pd.Series(c).rolling(20).mean().values
            vol_ma20 = pd.Series(v).rolling(20).mean().values
            
            bull = (ma5 > ma10) & (ma10 > ma20)
            zt_10d = pd.Series((df['pct_chg'].values >= 9.8).astype(float)).rolling(10, min_periods=1).max().values > 0
            vol_break = v >= vol_ma20 * breakout_ratio
            vol_shrink = v <= vol_ma20 * shrink_ratio
            
            # 底分型+阳线
            bottom_signal = np.zeros(n, dtype=bool)
            for i in range(2, n):
                if l[i-1] < l[i-2] and l[i-1] < l[i] and c[i] > o[i]:
                    bottom_signal[i] = True
            
            # 状态机：追踪放量→连续缩量
            # shrink_count: 放量后连续缩量的天数
            breakout_seen = False
            shrink_count = 0
            
            for i in range(n):
                if vol_break[i]:
                    breakout_seen = True
                    shrink_count = 0  # 放量当天重置缩量计数
                elif breakout_seen and vol_shrink[i]:
                    shrink_count += 1
                elif breakout_seen and not vol_shrink[i]:
                    # 非缩量日，根据是否严格要求连续来决定
                    # 如果不是缩量天，重置连续计数
                    shrink_count = 0
                
                # 检查信号
                date = str(df['trade_date'].iloc[i])
                if date < BACKTEST_START or date > BACKTEST_END:
                    continue
                if not bottom_signal[i]: continue
                if not bull[i]: continue
                if not zt_10d[i]: continue
                if c[i] <= 0: continue
                
                # 缩量条件：连续缩量天数 >= shrink_days_required
                if shrink_count >= shrink_days_required:
                    future_prices = []
                    for j in range(1, 16):
                        if i + j < n:
                            future_prices.append({
                                'd': str(df['trade_date'].iloc[i+j]),
                                'o': float(o[i+j]), 'h': float(h[i+j]),
                                'l': float(l[i+j]), 'c': float(c[i+j]),
                            })
                        else: break
                    
                    signals.append({
                        'code': ts_code,
                        'entry_date': date,
                        'entry_price': float(c[i]),
                        'shrink_count': shrink_count,
                        'future': future_prices
                    })
        except:
            continue
    
    return signals


def test_params(signals, sl, tp, mh, daily_limit=3):
    sig_sorted = sorted(signals, key=lambda x: (x['entry_date'], x['code']))
    daily_count = {}
    trades = []
    
    for sig in sig_sorted:
        d = sig['entry_date']
        daily_count.setdefault(d, 0)
        if daily_count[d] >= daily_limit: continue
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
    
    if not trades: return None
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
    
    return {'n': n, 'wr': wr, 'rr': rr, 'ev': ev, 'pf': pf, 
            'total': trades.sum(), 'sl': sl, 'tp': tp, 'mh': mh}


# ─── 测试不同缩量标准 ───
print("=" * 90)
print("不同缩量确认标准对比")
print("出场参数统一: 止损-4% / 止盈+20% / 持5天")
print("=" * 90)

best_sl, best_tp, best_mh = -0.04, 0.20, 5

for shrink_days in [1, 2, 3]:
    print(f"\n--- 连续缩量 {shrink_days} 天 ---")
    signals = load_and_signal(shrink_days_required=shrink_days)
    print(f"信号数: {len(signals)}")
    
    if not signals:
        print("  无信号")
        continue
    
    r = test_params(signals, best_sl, best_tp, best_mh)
    if r:
        print(f"  {r['n']:.0f}笔 | 胜率{r['wr']:.1%} | 盈亏比{r['rr']:.2f}:1 | EV{r['ev']:+.2%} | PF{r['pf']:.2f} | 累计{r['total']:+.1f}%")
    
    # 也测最优参数 -4%/30%/3天
    r2 = test_params(signals, -0.04, 0.30, 3)
    if r2:
        print(f"  (-4%/30%/3天) {r2['n']:.0f}笔 | 胜率{r2['wr']:.1%} | 盈亏比{r2['rr']:.2f}:1 | EV{r2['ev']:+.2%} | PF{r2['pf']:.2f}")
    
    # 自然走势
    pnls_3d = np.array([(s['future'][2]['c'] - s['entry_price']) / s['entry_price'] 
                         for s in signals if len(s['future']) >= 3])
    pnls_5d = np.array([(s['future'][4]['c'] - s['entry_price']) / s['entry_price'] 
                         for s in signals if len(s['future']) >= 5])
    if len(pnls_3d) > 0:
        print(f"  自然走势: 持3天{pnls_3d.mean():+.2%}(胜率{(pnls_3d>0).mean():.1%}) 持5天{pnls_5d.mean():+.2%}(胜率{(pnls_5d>0).mean():.1%})")

# ─── 另一种标准：调整期间缩量天数占比 ───
print(f"\n\n{'='*90}")
print("另一种标准：不要求连续，但要求调整期间多数天是缩量的")
print("=" * 90)
# 这个比较复杂，先看连续的结果就够了

# ─── 最佳参数再测一轮（连续2天缩量）───
print(f"\n{'='*90}")
print("连续缩量2天 + 参数网格搜索")
print(f"{'='*90}")

signals_2 = load_and_signal(shrink_days_required=2)
print(f"信号数: {len(signals_2)}")

results = []
for sl in [-0.03, -0.04, -0.05, -0.06]:
    for tp in [0.10, 0.15, 0.20, 0.25, 0.30]:
        for mh in [2, 3, 5, 7]:
            r = test_params(signals_2, sl, tp, mh)
            if r: results.append(r)

rdf = pd.DataFrame(results)
viable = rdf[(rdf['ev'] > 0) & (rdf['pf'] > 1)]
print(f"可行组合: {len(viable)}/{len(rdf)}")

if len(viable) > 0:
    print(f"\nTop 15 (按EV):")
    print(f"{'止损':>6} {'止盈':>6} {'持仓':>4} | {'笔数':>5} {'胜率':>6} {'盈亏比':>7} {'EV':>7} {'PF':>6} {'累计':>9}")
    print("-" * 80)
    for _, r in viable.sort_values('ev', ascending=False).head(15).iterrows():
        print(f"{r['sl']:>6.0%} {r['tp']:>6.0%} {r['mh']:>4.0f}天 | {r['n']:>5.0f} {r['wr']:>5.1%} {r['rr']:>5.2f}:1 {r['ev']:>+6.2%} {r['pf']:>5.2f} {r['total']:>+8.1f}%")
else:
    print("没有可行组合")
    for _, r in rdf.sort_values('ev', ascending=False).head(5).iterrows():
        print(f"  止损{r['sl']:.0%} 止盈{r['tp']:.0%} 持{r['mh']:.0f}天 → EV{r['ev']:+.2%} PF{r['pf']:.2f}")

# ─── 连续3天缩量 ───
print(f"\n{'='*90}")
print("连续缩量3天 + 参数网格搜索")
print(f"{'='*90}")

signals_3 = load_and_signal(shrink_days_required=3)
print(f"信号数: {len(signals_3)}")

results3 = []
for sl in [-0.03, -0.04, -0.05, -0.06]:
    for tp in [0.10, 0.15, 0.20, 0.25, 0.30]:
        for mh in [2, 3, 5, 7]:
            r = test_params(signals_3, sl, tp, mh)
            if r: results3.append(r)

rdf3 = pd.DataFrame(results3)
viable3 = rdf3[(rdf3['ev'] > 0) & (rdf3['pf'] > 1)]
print(f"可行组合: {len(viable3)}/{len(rdf3)}")

if len(viable3) > 0:
    print(f"\nTop 15 (按EV):")
    print(f"{'止损':>6} {'止盈':>6} {'持仓':>4} | {'笔数':>5} {'胜率':>6} {'盈亏比':>7} {'EV':>7} {'PF':>6} {'累计':>9}")
    print("-" * 80)
    for _, r in viable3.sort_values('ev', ascending=False).head(15).iterrows():
        print(f"{r['sl']:>6.0%} {r['tp']:>6.0%} {r['mh']:>4.0f}天 | {r['n']:>5.0f} {r['wr']:>5.1%} {r['rr']:>5.2f}:1 {r['ev']:>+6.2%} {r['pf']:>5.2f} {r['total']:>+8.1f}%")
else:
    print("没有可行组合")
    for _, r in rdf3.sort_values('ev', ascending=False).head(5).iterrows():
        print(f"  止损{r['sl']:.0%} 止盈{r['tp']:.0%} 持{r['mh']:.0f}天 → EV{r['ev']:+.2%} PF{r['pf']:.2f}")
