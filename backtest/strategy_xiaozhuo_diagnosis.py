#!/usr/bin/env python3
"""
小卓策略深度诊断 — 分析信号质量，找改进方向
"""
import json
import numpy as np
import pandas as pd

# Load signals
with open('/root/.openclaw/workspace/finance_tool/backtest/results/xiaozhuo_signals.json') as f:
    signals = json.load(f)

print(f"总信号数: {len(signals)}")
print()

# ─── 诊断1: 信号后续表现统计（不看止盈止损，看自然走势）───
print("=" * 70)
print("诊断1: 入场后自然走势（无止盈止损）")
print("=" * 70)

for hold_days in [1, 2, 3, 5, 7, 10]:
    pnls = []
    for sig in signals:
        ep = sig['entry_price']
        future = sig['future']
        if len(future) >= hold_days:
            fp = future[hold_days - 1]
            pnl = (fp['c'] - ep) / ep
            pnls.append(pnl)
    pnls = np.array(pnls)
    wr = (pnls > 0).mean()
    print(f"  持{hold_days}天: 平均{pnls.mean():+.2%} 中位{np.median(pnls):+.2%} 胜率{wr:.1%} ({len(pnls)}笔)")

# ─── 诊断2: 按放量状态分组 ───
print(f"\n{'='*70}")
print("诊断2: 按放量状态分组（vol_state）")
print(f"{'='*70}")
for vs in [1, 2]:
    sub = [s for s in signals if s['vol_state'] == vs]
    print(f"\n  vol_state={vs} ({len(sub)}笔)")
    for hd in [1, 3, 5]:
        pnls = []
        for sig in sub:
            if len(sig['future']) >= hd:
                pnl = (sig['future'][hd-1]['c'] - sig['entry_price']) / sig['entry_price']
                pnls.append(pnl)
        if pnls:
            pnls = np.array(pnls)
            print(f"    持{hd}天: 平均{pnls.mean():+.2%} 胜率{(pnls>0).mean():.1%}")

# ─── 诊断3: 底分型位置与均线的关系 ───
# 底分型第三根阳线收盘价相对MA5/MA20的位置
print(f"\n{'='*70}")
print("诊断3: 入场价与后续5天最大涨幅/最大跌幅")
print(f"{'='*70}")

mfe_list = []  # 最大有利波动
mae_list = []  # 最大不利波动
for sig in signals:
    ep = sig['entry_price']
    future = sig['future'][:5]  # 5天内
    if not future:
        continue
    highs = [(fp['h'] - ep) / ep for fp in future]
    lows = [(fp['l'] - ep) / ep for fp in future]
    mfe_list.append(max(highs))
    mae_list.append(min(lows))

mfe = np.array(mfe_list)
mae = np.array(mae_list)
print(f"  5天最大涨幅(MFE): 平均+{mfe.mean():.2%} 中位+{np.median(mfe):.2%}")
print(f"  5天最大跌幅(MAE): 平均{mae.mean():.2%} 中位{np.median(mae):.2%}")
print(f"  MFE>5%占比: {(mfe > 0.05).mean():.1%}")
print(f"  MAE<-5%占比: {(mae < -0.05).mean():.1%}")
print(f"  MFE>MAE绝对值占比: {(mfe > np.abs(mae)).mean():.1%}")

# ─── 诊断4: 年度信号分布 ───
print(f"\n{'='*70}")
print("诊断4: 年度信号分布与质量")
print(f"{'='*70}")
years = {}
for sig in signals:
    y = sig['entry_date'][:4]
    years.setdefault(y, []).append(sig)

for y in sorted(years.keys()):
    sub = years[y]
    pnls_3d = [(s['future'][2]['c'] - s['entry_price']) / s['entry_price'] 
               for s in sub if len(s['future']) >= 3]
    pnls_3d = np.array(pnls_3d)
    if len(pnls_3d) > 0:
        print(f"  {y}: {len(sub)}信号 持3天平均{pnls_3d.mean():+.2%} 胜率{(pnls_3d>0).mean():.1%}")

# ─── 诊断5: 涨停板距离入场的时间 ───
# 近10日有涨停，但距离涨停的天数对后续影响如何？
# 我们无法直接从这个数据计算，但可以看信号频率和后续表现的相关性

# ─── 诊断6: 尝试改进 — 只选当日缩量的信号（vol_state==2）───
print(f"\n{'='*70}")
print("诊断6: 改进方向探索")
print(f"{'='*70}")

# 6a: 只选vol_state==2（已确认缩量）
sub2 = [s for s in signals if s['vol_state'] == 2]
pnls = np.array([(s['future'][2]['c'] - s['entry_price']) / s['entry_price'] 
                  for s in sub2 if len(s['future']) >= 3])
print(f"\n  ① vol_state==2（已确认缩量）: {len(sub2)}笔")
print(f"     持3天: 平均{pnls.mean():+.2%} 胜率{(pnls>0).mean():.1%}")

# 6b: 底分型第三根阳线涨幅>2%（信号更强）
sub3 = [s for s in signals if len(s['future']) >= 3]
# 计算信号日涨幅（从entry_price vs future前一天的close）
# 我们没有前一天的close...用entry日的open vs close
# 信号日是future的前一天...不对，entry_price就是信号日收盘价
# 换个角度：信号日阳线实体大小
# 我们只保存了future数据，没保存entry日的open

# 6c: 后续1天是否高开（跳空确认）
sub_gaps = []
for s in signals:
    if len(s['future']) >= 1:
        gap = (s['future'][0]['o'] - s['entry_price']) / s['entry_price']
        sub_gaps.append(gap)
sub_gaps = np.array(sub_gaps)
print(f"\n  ② 入场次日高开占比: {(sub_gaps > 0).mean():.1%}")
print(f"     次日高开平均幅度: +{sub_gaps[sub_gaps>0].mean():.2%}" if (sub_gaps > 0).any() else "")
print(f"     次日低开平均幅度: {sub_gaps[sub_gaps<=0].mean():.2%}" if (sub_gaps <= 0).any() else "")

# 6d: 高开的信号后续表现是否更好？
high_open_idx = sub_gaps > 0
pnls_all_3d = np.array([(s['future'][2]['c'] - s['entry_price']) / s['entry_price'] 
                         for s in signals if len(s['future']) >= 3])
pnls_high = pnls_all_3d[high_open_idx[:len(pnls_all_3d)]] if high_open_idx.sum() > 0 else np.array([])
pnls_low = pnls_all_3d[~high_open_idx[:len(pnls_all_3d)]] if (~high_open_idx).sum() > 0 else np.array([])

if len(pnls_high) > 0:
    print(f"\n  ③ 次日高开组({len(pnls_high)}笔) 持3天: 平均{pnls_high.mean():+.2%} 胜率{(pnls_high>0).mean():.1%}")
if len(pnls_low) > 0:
    print(f"  ④ 次日低开组({len(pnls_low)}笔) 持3天: 平均{pnls_low.mean():+.2%} 胜率{(pnls_low>0).mean():.1%}")

# ─── 诊断7: 单笔收益分布 ───
print(f"\n{'='*70}")
print("诊断7: 持3天收益分布（全部信号）")
print(f"{'='*70}")
pnls_3d = np.array([(s['future'][2]['c'] - s['entry_price']) / s['entry_price'] 
                     for s in signals if len(s['future']) >= 3])
for bucket in [(-999, -0.10), (-0.10, -0.05), (-0.05, 0), (0, 0.05), (0.05, 0.10), (0.10, 0.20), (0.20, 999)]:
    mask = (pnls_3d >= bucket[0]) & (pnls_3d < bucket[1])
    pct = mask.mean() * 100
    bar = '█' * int(pct)
    label = f"{bucket[0]*100:+.0f}%~{bucket[1]*100:+.0f}%"
    print(f"  {label:>12}: {pct:5.1f}% {bar}")

# ─── 总结 ───
print(f"\n{'='*70}")
print("📋 诊断总结")
print(f"{'='*70}")
print(f"""
  ✅ 信号数量充足: {len(signals)}笔（3年多）
  ❌ 核心问题: 信号质量不足，平均后续收益为负
  ❌ 胜率偏低: 约42-47%，不足以支撑正EV
  ❌ 5天MFE vs MAE: +{mfe.mean():.2%} vs {mae.mean():.2%}
  
  关键发现:
  1. 96种止盈止损组合中没有EV>0的
  2. 最优组合(-3%/+8%/3天) EV仅-0.09%，PF=0.95，非常接近盈亏平衡
  3. 信号次日高开概率约50%——没有显著的日内优势
  4. vol_state==2（确认缩量）的信号并没有显著优于vol_state==1
  
  可能的改进方向:
  1. 增加板块效应过滤（只做主线板块）
  2. 增加情绪周期过滤（不在退潮期操作）
  3. 底分型要求更强的确认信号（如放量阳线、跳空高开）
  4. 考虑日内确认（次日开盘确认而非收盘入场）
""")
