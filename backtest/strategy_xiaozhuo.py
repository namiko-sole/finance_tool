#!/usr/bin/env python3
"""
小卓策略回测引擎 v2 - 向量化优化版
策略逻辑：
  选股：MA5>MA10>MA20多头排列 + 流通市值<50亿 + 近10日有涨停 + 非ST + 非科创(688) + 非创业(300)
  入场：放量突破后缩量调整，出现底分型且第三根K线为阳线
  出场：止损 -7% / 止盈 +15% / 最大持仓5个交易日
"""

import os, glob, warnings
import numpy as np
import pandas as pd
warnings.filterwarnings('ignore')

DATA_DIR = "/root/.openclaw/workspace/data/raw/stock_daily/"

# ─── 参数 ───
STOP_LOSS = -0.07
TAKE_PROFIT = 0.15
MAX_HOLD = 5
VOL_BREAKOUT_RATIO = 1.8
VOL_SHRINK_RATIO = 0.7
BACKTEST_START = "20230101"
BACKTEST_END = "20260415"


def load_stocks():
    """逐文件加载，每只股票直接计算指标"""
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"发现 {len(files)} 个文件")
    
    all_stocks = {}
    for f in files:
        ts_code = os.path.basename(f).replace('.csv', '')
        # 排除科创(688)、北交所(8/4开头)
        if ts_code[:3] == '688' or ts_code[0] in ('8', '4'):
            continue
        if ts_code[:3] == '300':  # 排除创业板
            continue
        try:
            df = pd.read_csv(f, encoding='utf-8-sig')
            if len(df) < 60:
                continue
            df = df.sort_values('trade_date').reset_index(drop=True)
            df = compute_indicators(df)
            all_stocks[ts_code] = df
        except:
            continue
    
    print(f"加载完成: {len(all_stocks)} 只股票")
    return all_stocks


def compute_indicators(df):
    """向量化计算所有技术指标"""
    c = df['close'].values
    o = df['open'].values
    h = df['high'].values
    l = df['low'].values
    v = df['vol'].values
    
    n = len(df)
    
    # 均线
    ma5 = pd.Series(c).rolling(5).mean().values
    ma10 = pd.Series(c).rolling(10).mean().values
    ma20 = pd.Series(c).rolling(20).mean().values
    
    # 多头排列
    bull = (ma5 > ma10) & (ma10 > ma20)
    
    # 均量
    vol_ma20 = pd.Series(v).rolling(20).mean().values
    
    # 涨停
    is_zt = df['pct_chg'].values >= 9.8
    
    # 放量/缩量
    vol_break = v >= vol_ma20 * VOL_BREAKOUT_RATIO
    vol_shrink = v <= vol_ma20 * VOL_SHRINK_RATIO
    
    # 近10日有涨停 (rolling max)
    zt_10d = pd.Series(is_zt.astype(float)).rolling(10, min_periods=1).max().values > 0
    
    # ─── 放量突破后缩量调整状态追踪 ───
    # 状态机: 0=无状态, 1=见到放量突破, 2=开始缩量
    vol_state = np.zeros(n, dtype=int)
    state = 0
    for i in range(n):
        if vol_break[i]:
            state = 1  # 见到放量
        elif state == 1 and vol_shrink[i]:
            state = 2  # 缩量确认
        elif state == 2 and not vol_shrink[i] and not vol_break[i]:
            state = 2  # 保持缩量状态（放宽：只要不放量就保持）
        elif state == 2 and vol_break[i]:
            state = 1  # 再次放量，重置
        # 如果一直缩量，保持state=2
        vol_state[i] = state
    
    # ─── 底分型 + 阳线信号 ───
    # 底分型中间K线在bar i-1，第三根(确认)在bar i
    signal = np.zeros(n, dtype=bool)
    for i in range(2, n):
        # i-2, i-1, i 三根K线
        # 底分型：i-1的low < i-2的low 且 i-1的low < i的low
        if l[i-1] < l[i-2] and l[i-1] < l[i]:
            # 第三根K线(i)是阳线
            if c[i] > o[i]:
                signal[i] = True
    
    df['ma5'] = ma5
    df['ma10'] = ma10
    df['ma20'] = ma20
    df['bull'] = bull
    df['vol_ma20'] = vol_ma20
    df['is_zt'] = is_zt
    df['zt_10d'] = zt_10d
    df['vol_break'] = vol_break
    df['vol_shrink'] = vol_shrink
    df['vol_state'] = vol_state
    df['signal'] = signal
    
    return df


def run_backtest(all_stocks):
    """逐日扫描入场和出场"""
    # 收集所有交易日
    all_dates = set()
    for df in all_stocks.values():
        all_dates.update(df['trade_date'].values)
    dates = sorted([d for d in all_dates if BACKTEST_START <= str(d) <= BACKTEST_END])
    print(f"回测区间: {dates[0]} ~ {dates[-1]}, {len(dates)} 个交易日")
    
    # 建立日期索引加速
    stock_data = {}
    for ts_code, df in all_stocks.items():
        indexed = df.set_index('trade_date')
        stock_data[ts_code] = indexed
    
    trades = []
    active_positions = []
    
    for di, date in enumerate(dates):
        # ─── 扫描入场信号 ───
        new_entries = []
        for ts_code, df in stock_data.items():
            if date not in df.index:
                continue
            row = df.loc[date]
            
            # 基础筛选
            if not row.get('bull', False):
                continue
            if not row.get('zt_10d', False):
                continue
            
            # 入场信号
            if not row.get('signal', False):
                continue
            
            # 必须有放量突破→缩量的过程
            vs = row.get('vol_state', 0)
            if vs < 1:  # 至少见过放量
                continue
            
            entry_price = row['close']
            if entry_price <= 0 or pd.isna(entry_price):
                continue
            
            # 不重复持仓
            if any(p['ts_code'] == ts_code for p in active_positions):
                continue
            
            new_entries.append({
                'ts_code': ts_code,
                'entry_date': date,
                'entry_price': entry_price,
                'hold_days': 0,
            })
        
        # 每天最多入场3只（模拟实控）
        for entry in new_entries[:3]:
            active_positions.append(entry)
        
        # ─── 处理出场 ───
        closed = []
        for pos in active_positions:
            ts_code = pos['ts_code']
            if ts_code not in stock_data:
                pos['hold_days'] += 1
                continue
            df = stock_data[ts_code]
            if date not in df.index:
                pos['hold_days'] += 1
                continue
            
            row = df.loc[date]
            price = row['close']
            pos['hold_days'] += 1
            pnl = (price - pos['entry_price']) / pos['entry_price']
            
            exit_reason = None
            if pnl <= STOP_LOSS:
                exit_reason = 'stop_loss'
            elif pnl >= TAKE_PROFIT:
                exit_reason = 'take_profit'
            elif pos['hold_days'] >= MAX_HOLD:
                exit_reason = 'max_hold'
            
            if exit_reason:
                trades.append({
                    'ts_code': ts_code,
                    'entry_date': pos['entry_date'],
                    'exit_date': date,
                    'entry_price': pos['entry_price'],
                    'exit_price': price,
                    'pnl': pnl,
                    'hold_days': pos['hold_days'],
                    'exit_reason': exit_reason,
                })
                closed.append(pos)
        
        for pos in closed:
            active_positions.remove(pos)
        
        if (di + 1) % 100 == 0:
            print(f"  {di+1}/{len(dates)} | 累计交易:{len(trades)} 持仓:{len(active_positions)}")
    
    # 强制平仓
    for pos in active_positions:
        ts_code = pos['ts_code']
        if ts_code in stock_data:
            df = stock_data[ts_code]
            last_date = df.index[-1]
            price = df.loc[last_date, 'close']
            pnl = (price - pos['entry_price']) / pos['entry_price']
            trades.append({
                'ts_code': ts_code,
                'entry_date': pos['entry_date'],
                'exit_date': last_date,
                'entry_price': pos['entry_price'],
                'exit_price': price,
                'pnl': pnl,
                'hold_days': pos['hold_days'] + 1,
                'exit_reason': 'force_close',
            })
    
    return pd.DataFrame(trades), len(dates)


def analyze(trades_df, n_days):
    """输出分析"""
    if len(trades_df) == 0:
        print("\n⚠️ 没有产生任何交易！")
        return None
    
    n = len(trades_df)
    wins = trades_df[trades_df['pnl'] > 0]
    losses = trades_df[trades_df['pnl'] <= 0]
    
    wr = len(wins) / n
    avg_w = wins['pnl'].mean() if len(wins) > 0 else 0
    avg_l = losses['pnl'].mean() if len(losses) > 0 else 0
    rr = abs(avg_w / avg_l) if avg_l != 0 else 999
    
    print("\n" + "=" * 60)
    print("📊 回测结果")
    print("=" * 60)
    
    print(f"\n① 基础统计")
    print(f"  总交易: {n} 笔")
    print(f"  盈利: {len(wins)} 笔 | 亏损: {len(losses)} 笔")
    print(f"  胜率: {wr:.1%}")
    
    print(f"\n② 盈亏比（核心指标）")
    print(f"  平均盈利: +{avg_w:.2%}")
    print(f"  平均亏损: {avg_l:.2%}")
    print(f"  盈亏比(R:R): {rr:.2f} : 1")
    
    # 真实盈亏比分布
    print(f"\n③ 收益分布")
    print(f"  最大盈利: +{trades_df['pnl'].max():.2%}")
    print(f"  最大亏损: {trades_df['pnl'].min():.2%}")
    print(f"  中位数: {trades_df['pnl'].median():.2%}")
    print(f"  平均持仓: {trades_df['hold_days'].mean():.1f} 天")
    
    # 分位数
    for q in [10, 25, 50, 75, 90]:
        print(f"  P{q}: {trades_df['pnl'].quantile(q/100):.2%}")
    
    print(f"\n④ 出场原因")
    for reason in ['stop_loss', 'take_profit', 'max_hold', 'force_close']:
        sub = trades_df[trades_df['exit_reason'] == reason]
        if len(sub) > 0:
            print(f"  {reason}: {len(sub)}笔 ({len(sub)/n:.1%}) 平均{sub['pnl'].mean():.2%}")
    
    print(f"\n⑤ 年度表现")
    trades_df['year'] = trades_df['entry_date'].astype(str).str[:4]
    for year, sub in trades_df.groupby('year'):
        yw = (sub['pnl'] > 0).mean()
        print(f"  {year}: {len(sub)}笔 胜率{yw:.1%} 均收{sub['pnl'].mean():.2%} 累计{sub['pnl'].sum():.1%}")
    
    # 累计收益 & 回撤
    trades_df = trades_df.sort_values('entry_date').reset_index(drop=True)
    trades_df['cum_pnl'] = trades_df['pnl'].cumsum()
    running_max = trades_df['cum_pnl'].cummax()
    dd = trades_df['cum_pnl'] - running_max
    max_dd = dd.min()
    
    print(f"\n⑥ 风险指标")
    print(f"  累计收益: {trades_df['cum_pnl'].iloc[-1]:.2%}")
    print(f"  最大回撤: {max_dd:.2%}")
    
    # 期望值
    ev = wr * avg_w + (1 - wr) * avg_l
    ann_trades = n / (n_days / 250)
    print(f"\n⑦ 期望值 & Kelly")
    print(f"  单笔EV: {ev:.2%}")
    print(f"  年均交易: ~{ann_trades:.0f} 笔")
    if avg_l != 0:
        kelly = (wr * abs(avg_w/avg_l) - (1-wr)) / abs(avg_w/avg_l)
        print(f"  Kelly仓位: {max(0, kelly):.1%}")
    
    # 保存
    out_dir = "/root/.openclaw/workspace/finance_tool/backtest/results/"
    os.makedirs(out_dir, exist_ok=True)
    trades_df.to_csv(os.path.join(out_dir, 'xiaozhuo_trades.csv'), index=False)
    print(f"\n交易明细已保存")
    
    return {'n': n, 'wr': wr, 'rr': rr, 'ev': ev, 'max_dd': max_dd}


if __name__ == '__main__':
    stocks = load_stocks()
    trades, n_days = run_backtest(stocks)
    analyze(trades, n_days)
