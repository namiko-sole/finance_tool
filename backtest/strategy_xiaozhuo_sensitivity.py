#!/usr/bin/env python3
"""小卓策略参数敏感性分析 - 测试不同止盈/止损/持仓天数组合"""
import os, glob, warnings
import numpy as np
import pandas as pd
warnings.filterwarnings('ignore')

DATA_DIR = "/root/.openclaw/workspace/data/raw/stock_daily/"
BACKTEST_START = "20230101"
BACKTEST_END = "20260415"
VOL_BREAKOUT_RATIO = 1.8
VOL_SHRINK_RATIO = 0.7

def load_and_precompute():
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    all_stocks = {}
    for f in files:
        ts_code = os.path.basename(f).replace('.csv', '')
        if ts_code[:3] == '688' or ts_code[0] in ('8', '4') or ts_code[:3] == '300':
            continue
        try:
            df = pd.read_csv(f, encoding='utf-8-sig').sort_values('trade_date').reset_index(drop=True)
            if len(df) < 60:
                continue
            # Precompute indicators
            c, o, l, v = df['close'].values, df['open'].values, df['low'].values, df['vol'].values
            ma5 = pd.Series(c).rolling(5).mean().values
            ma10 = pd.Series(c).rolling(10).mean().values
            ma20 = pd.Series(c).rolling(20).mean().values
            vol_ma20 = pd.Series(v).rolling(20).mean().values
            
            df['bull'] = (ma5 > ma10) & (ma10 > ma20)
            df['vol_ma20'] = vol_ma20
            df['is_zt'] = df['pct_chg'].values >= 9.8
            df['zt_10d'] = pd.Series((df['pct_chg'].values >= 9.8).astype(float)).rolling(10, min_periods=1).max().values > 0
            df['vol_break'] = v >= vol_ma20 * VOL_BREAKOUT_RATIO
            df['vol_shrink'] = v <= vol_ma20 * VOL_SHRINK_RATIO
            
            # Volume state machine
            vb, vs = df['vol_break'].values, df['vol_shrink'].values
            vol_state = np.zeros(len(df), dtype=int)
            st = 0
            for i in range(len(df)):
                if vb[i]: st = 1
                elif st == 1 and vs[i]: st = 2
                elif st == 2 and vb[i]: st = 1
                vol_state[i] = st
            df['vol_state'] = vol_state
            
            # Bottom fractal signal
            signal = np.zeros(len(df), dtype=bool)
            for i in range(2, len(df)):
                if l[i-1] < l[i-2] and l[i-1] < l[i] and c[i] > o[i]:
                    signal[i] = True
            df['signal'] = signal
            
            all_stocks[ts_code] = df.set_index('trade_date')
        except:
            continue
    return all_stocks


def run_variant(all_stocks, stop_loss, take_profit, max_hold):
    """Run backtest with specific parameters"""
    all_dates = set()
    for df in all_stocks.values():
        all_dates.update(df.index)
    dates = sorted([d for d in all_dates if BACKTEST_START <= str(d) <= BACKTEST_END])
    
    trades = []
    active = []
    
    for date in dates:
        # Entries
        new = []
        for ts_code, df in all_stocks.items():
            if date not in df.index: continue
            r = df.loc[date]
            if not r.get('bull', False): continue
            if not r.get('zt_10d', False): continue
            if not r.get('signal', False): continue
            if r.get('vol_state', 0) < 1: continue
            ep = r['close']
            if ep <= 0 or pd.isna(ep): continue
            if any(p[0] == ts_code for p in active): continue
            new.append((ts_code, date, ep))
        
        for ts_code, entry_date, entry_price in new[:3]:
            active.append([ts_code, entry_date, entry_price, 0])
        
        # Exits
        closed = []
        for idx, (ts_code, entry_date, entry_price, hold_days) in enumerate(active):
            if ts_code not in all_stocks: 
                active[idx] = (ts_code, entry_date, entry_price, hold_days + 1)
                continue
            df = all_stocks[ts_code]
            if date not in df.index:
                active[idx] = (ts_code, entry_date, entry_price, hold_days + 1)
                continue
            price = df.loc[date, 'close']
            hd = hold_days + 1
            pnl = (price - entry_price) / entry_price
            
            active[idx] = (ts_code, entry_date, entry_price, hd)
            
            reason = None
            if pnl <= stop_loss: reason = 'sl'
            elif pnl >= take_profit: reason = 'tp'
            elif hd >= max_hold: reason = 'mh'
            
            if reason:
                trades.append(pnl)
                closed.append(idx)
        
        for idx in sorted(closed, reverse=True):
            active.pop(idx)
    
    # Force close
    for ts_code, entry_date, entry_price, hd in active:
        if ts_code in all_stocks:
            df = all_stocks[ts_code]
            price = df.iloc[-1]['close']
            trades.append((price - entry_price) / entry_price)
    
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
        'med': np.median(trades)
    }


if __name__ == '__main__':
    print("加载并预处理数据...")
    stocks = load_and_precompute()
    print(f"完成: {len(stocks)} 只股票\n")
    
    # Test parameter grid
    sl_range = [-0.03, -0.05, -0.07, -0.10]
    tp_range = [0.08, 0.10, 0.15, 0.20, 0.30]
    mh_range = [3, 5, 7, 10]
    
    results = []
    for sl in sl_range:
        for tp in tp_range:
            for mh in mh_range:
                r = run_variant(stocks, sl, tp, mh)
                if r:
                    r['sl'] = sl
                    r['tp'] = tp
                    r['mh'] = mh
                    results.append(r)
    
    rdf = pd.DataFrame(results)
    rdf = rdf.sort_values('ev', ascending=False)
    
    print("=" * 80)
    print("参数敏感性分析 - 按期望值(EV)排序 Top 20")
    print("=" * 80)
    print(f"{'止损':>6} {'止盈':>6} {'持仓':>4} | {'笔数':>5} {'胜率':>6} {'盈亏比':>6} {'EV':>7} {'PF':>6} {'累计':>8}")
    print("-" * 80)
    for _, r in rdf.head(20).iterrows():
        print(f"{r['sl']:>6.0%} {r['tp']:>6.0%} {r['mh']:>4.0f}天 | {r['n']:>5.0f} {r['wr']:>5.1%} {r['rr']:>5.2f}:1 {r['ev']:>+6.2%} {r['pf']:>5.2f} {r['total']:>+7.1%}")
    
    print(f"\n{'='*80}")
    print("参数敏感性分析 - 按盈利因子(PF)排序 Top 20")
    print(f"{'='*80}")
    print(f"{'止损':>6} {'止盈':>6} {'持仓':>4} | {'笔数':>5} {'胜率':>6} {'盈亏比':>6} {'EV':>7} {'PF':>6} {'累计':>8}")
    print("-" * 80)
    rdf2 = rdf.sort_values('pf', ascending=False)
    for _, r in rdf2.head(20).iterrows():
        print(f"{r['sl']:>6.0%} {r['tp']:>6.0%} {r['mh']:>4.0f}天 | {r['n']:>5.0f} {r['wr']:>5.1%} {r['rr']:>5.2f}:1 {r['ev']:>+6.2%} {r['pf']:>5.2f} {r['total']:>+7.1%}")
    
    # Best per max_hold
    print(f"\n{'='*80}")
    print("不同持仓天数下最优参数")
    print(f"{'='*80}")
    for mh in mh_range:
        sub = rdf[rdf['mh'] == mh].sort_values('ev', ascending=False)
        if len(sub) > 0:
            b = sub.iloc[0]
            print(f"  持仓{mh}天最优: 止损{b['sl']:.0%} 止盈{b['tp']:.0%} | {b['n']:.0f}笔 胜率{b['wr']:.1%} 盈亏比{b['rr']:.2f}:1 EV{b['ev']:+.2%} PF{b['pf']:.2f}")
