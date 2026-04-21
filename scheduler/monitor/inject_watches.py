#!/usr/bin/env python3
"""
策略A信号注入器 — 将小卓策略A扫描出的买入信号注入到 monitor/watches.yaml
让监控引擎在次日盘中实时盯盘，触发买点立刻推送。

用法:
    python inject_watches.py          # 从最新信号文件读取
    python inject_watches.py --clean  # 清除所有策略A注入的监控项
"""

import json
import os
import sys
import re
from datetime import datetime

WATCHES_PATH = "/root/.openclaw/workspace/finance_tool/scheduler/monitor/watches.yaml"
SIGNALS_DIR = "/root/.openclaw/workspace/finance_tool/analyze/stock_filter"

# 标记：策略A注入的监控项会带 __source: strategy_a
STRATEGY_A_MARKER = "# __source: strategy_a"

# 默认通知渠道
DEFAULT_NOTIFY = ["feishu_xiaozhuo", "weixin_xiaozhuo"]


def load_watches_yaml():
    """读取 watches.yaml 原始内容"""
    with open(WATCHES_PATH, "r", encoding="utf-8") as f:
        return f.read()


def save_watches_yaml(content):
    """保存 watches.yaml"""
    with open(WATCHES_PATH, "w", encoding="utf-8") as f:
        f.write(content)


def clean_strategy_a_entries():
    """清除所有策略A注入的监控项"""
    lines = load_watches_yaml().split("\n")
    cleaned = []
    skip = False
    for line in lines:
        if STRATEGY_A_MARKER in line:
            skip = True
            continue
        if skip:
            # 跳过注入的块直到遇到非缩进行或下一个 ts_code
            if line.strip().startswith("- ts_code:") and STRATEGY_A_MARKER not in line:
                skip = False
                cleaned.append(line)
            elif line.strip() == "" and len(cleaned) > 0 and cleaned[-1].strip() == "":
                skip = False
            else:
                continue
        else:
            cleaned.append(line)
    
    # 清理连续空行
    result = []
    prev_empty = False
    for line in cleaned:
        if line.strip() == "":
            if not prev_empty:
                result.append(line)
            prev_empty = True
        else:
            prev_empty = False
            result.append(line)
    
    save_watches_yaml("\n".join(result))
    print(f"[inject] 已清除策略A注入的监控项")


def inject_signals(signals):
    """
    将信号列表注入到 watches.yaml
    signals: list of dict, 每个包含 ts_code, name, entry_price 等
    """
    if not signals:
        print("[inject] 无信号需要注入")
        return

    # 先清除旧的策略A注入
    clean_strategy_a_entries()

    content = load_watches_yaml()
    
    # 构建新的 watch 条目
    new_entries = []
    for sig in signals:
        ts_code = sig["ts_code"]
        name = sig["name"]
        entry_price = sig.get("entry_price", 0)
        
        # 策略A选出的股，用 MA回踩策略 盯盘
        # 买点条件：回踩到MA10附近 + 底分型
        # 设置回踩区间为入场价上下3%
        zone_high = round(entry_price * 1.03, 2)
        zone_low = round(entry_price * 0.97, 2)
        stop_loss = round(entry_price * 0.93, 2)  # 止损-7%
        
        entry_yaml = f"""  - ts_code: "{ts_code}"
    name: "{name}"
    strategies:
      - type: pullback_zone
        params:
          zone_low: {zone_low}
          zone_high: {zone_high}
          stop_loss: {stop_loss}
        notify: {DEFAULT_NOTIFY}  {STRATEGY_A_MARKER}
      - type: ma_proximity
        params:
          mas: [5, 10, 20]
          proximity_pct: 1.5
        notify: {DEFAULT_NOTIFY}  {STRATEGY_A_MARKER}
"""
        new_entries.append(entry_yaml)
    
    # 在 watches: 的最后一条后面插入
    # 找到 watches 段的末尾（notify_channels: 之前）
    lines = content.split("\n")
    insert_idx = None
    for i, line in enumerate(lines):
        if line.strip().startswith("notify_channels:"):
            insert_idx = i
            break
    
    if insert_idx is None:
        print("[inject] 未找到 notify_channels 段，追加到末尾")
        content += "\n" + "\n".join(new_entries)
    else:
        # 在 notify_channels: 前插入
        all_entries = "".join(new_entries)
        lines.insert(insert_idx, all_entries.rstrip())
        content = "\n".join(lines)
    
    save_watches_yaml(content)
    print(f"[inject] 已注入 {len(signals)} 只策略A信号到 watches.yaml")
    for sig in signals:
        print(f"  → {sig['ts_code']} {sig['name']} | 入场价 {sig.get('entry_price', '?')}")


def find_latest_signals():
    """查找最新的策略A信号文件"""
    # 先看 xiaozhuo_signals.json
    signals_path = os.path.join(SIGNALS_DIR, "xiaozhuo_signals.json")
    if os.path.exists(signals_path):
        with open(signals_path, "r") as f:
            data = json.load(f)
        signals = data if isinstance(data, list) else data.get("signals", [])
        if signals:
            print(f"[inject] 从 {signals_path} 读取到 {len(signals)} 个信号")
            return signals
    
    # 没有信号文件，返回空
    print("[inject] 未找到策略A信号文件")
    return []


if __name__ == "__main__":
    if "--clean" in sys.argv:
        clean_strategy_a_entries()
    else:
        signals = find_latest_signals()
        if signals:
            inject_signals(signals)
        else:
            print("[inject] 没有需要注入的信号")
