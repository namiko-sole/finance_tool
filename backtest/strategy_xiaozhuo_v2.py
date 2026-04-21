#!/usr/bin/env python3
"""
小卓策略参数分析 v2
先一次性生成所有入场信号，再用向量化方式测试不同止盈止损
"""
import os, glob, warnings, json
import numpy as np
import pandas as pd
warnings.filterwarnings('ignore')

DATA_DIR = "/root/.openclaw/workspace/data/raw/stock_daily/"
BACKTEST_START = "20230101"
BACKTEST_END = "20260415"


def generate_signals():
    """生成所有入场信号（只跑一次）"""
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
            zt = df['pct_chg'].values >= 9.8
            zt_10d = pd.Series(zt.astype(float)).rolling(10, min_periods=1).max().values > 0
            vol_break = v >= vol_ma20 * 1.8
            vol_shrink = v <= vol_ma20 * 0.7
            
            # Volume state machine
            vstate = np.zeros(n, dtype=int)
            st = 0
            for i in range(n):
                if vol_break[i]: st = 1
                elif st == 1 and vol_shrink[i]: st = 2
                elif st == 2 and vol_break[i]: st = 1
                vstate[i] = st
            
            # Bottom fractal + yang candle
            for i in range(2, n):
                if l[i-1] < l[i-2] and l[i-1] < l[i] and c[i] > o[i]:
                    date = str(df['trade_date'].iloc[i])
                    if date < BACKTEST_START or date > BACKTEST_END:
                        continue
                    if not bull[i]: continue
                    if not zt_10d[i]: continue
                    if vstate[i] < 1: continue
                    if c[i] <= 0: continue
                    
                    # Collect future prices for exit simulation
                    future_prices = []
                    for j in range(1, 16):  # 最多看15天
                        if i + j < n:
                            future_prices.append({
                                'd': str(df['trade_date'].iloc[i+j]),
                                'o': float(o[i+j]),
                                'h': float(h[i+j]),
                                'l': float(l[i+j]),
                                'c': float(c[i+j]),
                            })
                        else:
                            break
                    
                    signals.append({
                        'code': ts_code,
                        'entry_date': date,
                        'entry_price': float(c[i]),
                        'vol_state': int(vstate[i]),
                        'future': future_prices
                    })
        except:
            continue
        
        if (fi + 1) % 500 == 0:
            print(f"  已扫描 {fi+1}/{len(files)} 文件, 信号数: {len(signals)}")
    
    print(f"\n总信号数: {len(signals)}")
    return signals


def test_exit_params(signals, sl, tp, max_hold, daily_limit=3):
    """测试特定出场参数"""
    # Sort signals by date, limit to 3 per day
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
            hold_days = day_idx + 1
            # Check stop loss on intraday low
            pnl_low = (fp['l'] - ep) / ep
            # Check take profit on intraday high
            pnl_high = (fp['h'] - ep) / ep
            pnl_close = (fp['c'] - ep) / ep
            
            # Stop loss
            if pnl_low <= sl:
                # Assume worst case: exit at stop price
                trades.append(sl)
                closed = True
                break
            
            # Take profit
            if pnl_high >= tp:
                trades.append(tp)
                closed = True
                break
            
            # Max hold
            if hold_days >= max_hold:
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
    
    return {
        'n': n, 'wr': wr, 'avg_w': avg_w, 'avg_l': avg_l,
        'rr': rr, 'ev': ev, 'pf': pf, 'total': trades.sum(),
        'med': np.median(trades), 'sl': sl, 'tp': tp, 'mh': max_hold
    }


if __name__ == '__main__':
    print("阶段1: 生成入场信号...")
    signals = generate_signals()
    
    # Save signals for reuse
    with open('/root/.openclaw/workspace/finance_tool/backtest/results/xiaozhuo_signals.json', 'w') as f:
        json.dump(signals, f)
    print(f"信号已保存 ({len(signals)} 条)")
    
    print("\n阶段2: 参数网格搜索...")
    results = []
    
    sl_range = [-0.03, -0.05, -0.07, -0.10]
    tp_range = [0.05, 0.08, 0.10, 0.15, 0.20, 0.30]
    mh_range = [3, 5, 7, 10]
    
    total_combos = len(sl_range) * len(tp_range) * len(mh_range)
    done = 0
    for sl in sl_range:
        for tp in tp_range:
            for mh in mh_range:
                r = test_exit_params(signals, sl, tp, mh)
                if r:
                    results.append(r)
                done += 1
    
    print(f"完成 {done} 组合测试\n")
    
    rdf = pd.DataFrame(results)
    
    # ─── 按EV排序 ───
    print("=" * 90)
    print("按期望值(EV)排序 Top 25")
    print("=" * 90)
    print(f"{'止损':>6} {'止盈':>6} {'持仓':>4} | {'笔数':>5} {'胜率':>6} {'盈亏比':>7} {'EV':>7} {'PF':>6} {'累计':>9}")
    print("-" * 90)
    for _, r in rdf.sort_values('ev', ascending=False).head(25).iterrows():
        print(f"{r['sl']:>6.0%} {r['tp']:>6.0%} {r['mh']:>4.0f}天 | {r['n']:>5.0f} {r['wr']:>5.1%} {r['rr']:>5.2f}:1 {r['ev']:>+6.2%} {r['pf']:>5.2f} {r['total']:>+8.1f}%")
    
    # ─── 按PF排序 ───
    print(f"\n{'='*90}")
    print("按盈利因子(PF)排序 Top 25")
    print(f"{'='*90}")
    print(f"{'止损':>6} {'止盈':>6} {'持仓':>4} | {'笔数':>5} {'胜率':>6} {'盈亏比':>7} {'EV':>7} {'PF':>6} {'累计':>9}")
    print("-" * 90)
    for _, r in rdf.sort_values('pf', ascending=False).head(25).iterrows():
        print(f"{r['sl']:>6.0%} {r['tp']:>6.0%} {r['mh']:>4.0f}天 | {r['n']:>5.0f} {r['wr']:>5.1%} {r['rr']:>5.2f}:1 {r['ev']:>+6.2%} {r['pf']:>5.2f} {r['total']:>+8.1f}%")
    
    # ─── 各持仓天数最优 ───
    print(f"\n{'='*90}")
    print("各持仓天数最优参数")
    print(f"{'='*90}")
    for mh in mh_range:
        sub = rdf[rdf['mh'] == mh].sort_values('ev', ascending=False)
        if len(sub) > 0:
            b = sub.iloc[0]
            print(f"  {mh}天: 止损{b['sl']:.0%} 止盈{b['tp']:.0%} → {b['n']:.0f}笔 胜率{b['wr']:.1%} 盈亏比{b['rr']:.2f}:1 EV{b['ev']:+.2%} PF{b['pf']:.2f}")
    
    # ─── 策略可行区域（EV>0 且 PF>1）───
    viable = rdf[(rdf['ev'] > 0) & (rdf['pf'] > 1)]
    print(f"\n可行区域（EV>0 且 PF>1）: {len(viable)} / {len(rdf)} 组合")
    if len(viable) > 0:
        for _, r in viable.sort_values('ev', ascending=False).head(10).iterrows():
            print(f"  止损{r['sl']:.0%} 止盈{r['tp']:.0%} 持{r['mh']:.0f}天 → 胜率{r['wr']:.1%} 盈亏比{r['rr']:.2f}:1 EV{r['ev']:+.2%}")
    else:
        print("  ⚠️ 没有找到EV>0的组合！")
        # Find closest to positive
        best = rdf.sort_values('ev', ascending=False).iloc[0]
        print(f"  最接近可行: 止损{best['sl']:.0%} 止盈{best['tp']:.0%} 持{best['mh']:.0f}天 EV{best['ev']:+.2%} PF{best['pf']:.2f}")
