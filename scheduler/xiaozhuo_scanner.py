#!/usr/bin/env python3
"""
小卓策略(v5.1)收盘后扫描器
由 history_scheduler.py 尾部调用，或独立运行。

两个池子：
  🟢 正式池: pos_20d < 0.5（严格满足，次日开盘入场）
  👀 观察池: 0.5 <= pos_20d < 0.8（持续监控，pos降到<0.5自动升级→推送到小八交流群）

出场规则(v5.1时间阶梯止损):
  止损-8% | 赚5%卖30% | 剩余等20%清仓 | 持15天
  tp1触发后: 1-5天止损-8% → 6-10天止损-3% → 11-15天保本
"""
import os
import sys
import json
import time
import numpy as np
import pandas as pd
import argparse
import subprocess

# 路径常量
DATA_DIR = "/root/.openclaw/workspace/data/raw/"
STATE_FILE = "/root/.openclaw/workspace/finance_tool/scheduler/xiaozhuo_scanner_state.json"
FEISHU_SCRIPT = "/root/.openclaw/workspace/skills/feishu-webhook/scripts/send_message.sh"
FEISHU_TARGET = "ou_8ca22888f5b7e33129fd69b193436ffd"  # 小卓

POS_STRICT = 0.5   # 正式信号阈值
POS_WATCH = 0.8    # 观察池阈值


def _get_trade_day_from_calendar() -> str:
    """从 trade_calendar_info.json 读取当前交易日"""
    cal_path = os.path.join(DATA_DIR, "trade_calendar_info.json")
    if not os.path.exists(cal_path):
        raise FileNotFoundError(f"交易日历信息文件不存在: {cal_path}")
    with open(cal_path, 'r', encoding='utf-8') as f:
        info = json.load(f)
    day = info.get("current_trade_day")
    if not day:
        raise ValueError("交易日历信息中缺少 current_trade_day 字段")
    return str(day)


def _load_state() -> dict:
    """加载状态文件"""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"last_scan_date": "", "notified": {}, "watch_pool": {}}


def _save_state(state: dict):
    """保存状态文件"""
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _get_stock_name(ts_code: str) -> str:
    """从stock_list获取股票名称"""
    import glob
    stock_list_path = os.path.join(DATA_DIR, "stock_list.csv")
    if os.path.exists(stock_list_path):
        try:
            df = pd.read_csv(stock_list_path, encoding='utf-8-sig')
            row = df[df['ts_code'] == ts_code]
            if len(row) > 0:
                return str(row.iloc[0]['name'])
        except:
            pass
    return ts_code


def _check_watch_pool_upgrades(current_trade_day: str, state: dict) -> list[dict]:
    """
    检查观察池中的股票，看哪些pos已经降到<0.5。
    返回升级的信号列表。
    """
    watch_pool = state.get("watch_pool", {})
    if not watch_pool:
        return []

    stock_daily_dir = os.path.join(DATA_DIR, "stock_daily/")
    upgraded = []

    for ts_code, info in list(watch_pool.items()):
        csv_path = os.path.join(stock_daily_dir, ts_code + ".csv")
        if not os.path.exists(csv_path):
            continue

        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            df = df.sort_values('trade_date').reset_index(drop=True)
            if len(df) < 60:
                continue
            if len(df) > 60:
                df = df.tail(60).reset_index(drop=True)

            c = df['close'].values
            l = df['low'].values
            o = df['open'].values
            v = df['vol'].values
            amt_arr = df['amount'].values

            # 找当天
            day_locs = np.where(df['trade_date'].astype(str) == current_trade_day)[0]
            if len(day_locs) == 0:
                continue
            idx = day_locs[0]

            # 计算pos_20d
            ws = max(0, idx - 19)
            high_20d = float(np.max(c[ws:idx + 1]))
            low_20d = float(np.min(c[ws:idx + 1]))
            if high_20d > low_20d:
                pos_20d = (c[idx] - low_20d) / (high_20d - low_20d)
            else:
                pos_20d = 0.5

            # 检查其他条件是否仍然满足
            ma5 = pd.Series(c).rolling(5).mean().values
            ma10 = pd.Series(c).rolling(10).mean().values
            ma20 = pd.Series(c).rolling(20).mean().values
            bull = (ma5[idx] > ma10[idx]) and (ma10[idx] > ma20[idx])

            # 底分型
            is_bottom = (idx >= 2) and (l[idx-1] < l[idx-2]) and (l[idx-1] < l[idx])
            is_yang = c[idx] > o[idx]

            # MA10
            if ma10[idx] > 0:
                price_vs_ma10 = c[idx] / ma10[idx]
            else:
                price_vs_ma10 = 0

            zt_date = info.get('zt_date', '')
            zt_vol = info.get('zt_vol', 0)
            entry_price = float(c[idx])

            # pos降到0.5以下 → 升级！
            if pos_20d < POS_STRICT:
                upgraded.append({
                    'ts_code': ts_code,
                    'name': info.get('name', _get_stock_name(ts_code)),
                    'zt_date': zt_date,
                    'zt_vol': zt_vol,
                    'signal_date': current_trade_day,
                    'entry_price': entry_price,
                    'min_shrink_vol': info.get('min_shrink_vol', 0),
                    'min_shrink_ratio': info.get('min_shrink_ratio', 0),
                    'min_amount': info.get('min_amount', 0),
                    'price_vs_ma10': (price_vs_ma10 - 1) * 100,
                    'pos_20d': float(pos_20d),
                    'day_after_zt': info.get('day_after_zt', 0),
                    'upgraded_from_watch': True,
                })

                # 从观察池移除（已升级）
                del watch_pool[ts_code]

        except Exception as e:
            continue

    return upgraded


def _format_date(date_str: str) -> str:
    """YYYYMMDD → YYYY-MM-DD"""
    if len(date_str) == 8:
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    return date_str


def _format_message(tier1_signals: list, tier2_signals: list,
                    upgraded_signals: list, total_scanned: int,
                    current_trade_day: str) -> str:
    """格式化飞书消息"""

    lines = []
    lines.append("━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("🎯 小卓策略 · 收盘扫描")
    lines.append(f"📅 {_format_date(current_trade_day)}")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━")

    circle_nums = ["①", "②", "③", "④", "⑤", "⑥", "⑦", "⑧", "⑨", "⑩",
                   "⑪", "⑫", "⑬", "⑭", "⑮", "⑯", "⑰", "⑱", "⑲", "⑳"]

    # ── 观察池升级 ──
    if upgraded_signals:
        lines.append("")
        lines.append("🔥 观察池→正式升级")
        lines.append("────────────────────")
        for i, sig in enumerate(upgraded_signals):
            num = circle_nums[i] if i < len(circle_nums) else f"({i+1})"
            lines.append(f"{num} {sig['ts_code']} {sig['name']}")
            lines.append(f"  涨停: {_format_date(sig['zt_date'])} | 收{sig['entry_price']:.2f} | pos={sig['pos_20d']:.3f}")
            lines.append(f"  ⬆️ 从观察池升级！已满足正式条件")

    # ── Tier 1 正式信号 ──
    if tier1_signals:
        lines.append("")
        lines.append("🟢 正式池（pos < 0.5，次日开盘入场）")
        lines.append("────────────────────")
        for i, sig in enumerate(tier1_signals):
            num = circle_nums[i] if i < len(circle_nums) else f"({i+1})"
            zt_vol_wan = sig['zt_vol'] / 10000.0
            min_shrink_vol_wan = sig['min_shrink_vol'] / 10000.0
            lines.append(f"{num} {sig['ts_code']} {sig['name']}")
            lines.append(f"  涨停: {_format_date(sig['zt_date'])}（{sig['day_after_zt']}天前）")
            lines.append(f"  缩量: {zt_vol_wan:.1f}万手→{min_shrink_vol_wan:.1f}万手（{sig['min_shrink_ratio']:.1f}%）")
            lines.append(f"  MA10偏离: {sig['price_vs_ma10']:+.2f}% | pos={sig['pos_20d']:.3f}")
            lines.append(f"  收盘 {sig['entry_price']:.2f}（次日开盘入场）")
    else:
        lines.append("")
        lines.append("🟢 正式池（pos < 0.5，次日开盘入场）")
        lines.append("────────────────────")
        lines.append("  今日无正式信号")

    # ── Tier 2 观察池 ──
    if tier2_signals:
        lines.append("")
        lines.append("👀 观察池（0.5 ≤ pos < 0.8，持续监控）")
        lines.append("────────────────────")
        for i, sig in enumerate(tier2_signals):
            num = circle_nums[i] if i < len(circle_nums) else f"({i+1})"
            lines.append(f"{num} {sig['ts_code']} {sig['name']}")
            lines.append(f"  涨停: {_format_date(sig['zt_date'])}（{sig['day_after_zt']}天前）")
            lines.append(f"  pos={sig['pos_20d']:.3f} | MA10偏离: {sig['price_vs_ma10']:+.2f}%")
            lines.append(f"  收盘 {sig['entry_price']:.2f} | 持续监控中")
    else:
        lines.append("")
        lines.append("👀 观察池（0.5 ≤ pos < 0.8，持续监控）")
        lines.append("────────────────────")
        lines.append("  今日无新增观察")

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━")
    parts = [f"共扫描 {total_scanned}+ 只"]
    parts.append(f"正式 {len(tier1_signals)} 个")
    parts.append(f"观察 {len(tier2_signals)} 个")
    if upgraded_signals:
        parts.append(f"升级 {len(upgraded_signals)} 个")
    lines.append(" | ".join(parts))
    lines.append("止损-8% / 5%卖30% / +20%清仓 / 持15天")
    lines.append("时间阶梯: tp1后 1-5天-8% → 6-10天-3% → 11-15天保本")
    lines.append("入场: 次日开盘价（高开>7%/低开>5%放弃）")

    return "\n".join(lines)


def _send_feishu(message: str):
    """发送飞书消息"""
    msg_lines = message.strip().split("\n")
    title = msg_lines[0] if msg_lines else "小卓策略信号"
    content = "\n".join(msg_lines[1:]) if len(msg_lines) > 1 else title

    mention = f"@{FEISHU_TARGET}"
    content = f"{mention}\n{content}"

    msg = f"{title}|{content}"

    try:
        subprocess.run(
            ["bash", FEISHU_SCRIPT, msg, "card"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        print("[小卓扫描] 飞书消息已推送")
    except Exception as e:
        print(f"[小卓扫描] 飞书推送失败: {e}")


def run_xiaozhuo_scan(current_trade_day: str = None):
    """
    主入口。扫描 → 分层 → 观察池升级检测 → 推送。
    """
    # 1. 确定交易日
    if not current_trade_day:
        current_trade_day = _get_trade_day_from_calendar()
    print(f"[小卓扫描] 交易日: {current_trade_day}")

    # 2. 加载状态
    state = _load_state()
    notified = state.get("notified", {})

    # 3. 添加 filter 路径
    filter_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                              "analyze", "stock_filter")
    if filter_dir not in sys.path:
        sys.path.insert(0, filter_dir)

    from xiaozhuo_filter import scan_xiaozhuo_signals

    # 4. 用宽松阈值(0.8)扫描，获取所有候选
    print("[小卓扫描] 扫描观察池范围(pos<0.8)...")
    result = scan_xiaozhuo_signals(current_trade_day, pos_threshold=POS_WATCH)
    all_signals = result["signals"]
    total_scanned = result["total_scanned"]
    print(f"[小卓扫描] 宽松扫描: {len(all_signals)} 个候选")

    # 5. 分层: Tier1 (pos<0.5) 和 Tier2 (0.5<=pos<0.8)
    tier1 = [s for s in all_signals if s['pos_20d'] < POS_STRICT]
    tier2 = [s for s in all_signals if s['pos_20d'] >= POS_STRICT and s['pos_20d'] < POS_WATCH]
    print(f"[小卓扫描] Tier1(正式): {len(tier1)} | Tier2(观察): {len(tier2)}")

    # 6. Tier1 去重（已通知过的跳过）
    new_tier1 = []
    for sig in tier1:
        key = sig['ts_code']
        if key in notified and notified[key] == sig['zt_date']:
            continue
        new_tier1.append(sig)

    # 7. Tier2 去重（已在观察池的跳过，但保留更新pos）
    watch_pool = state.get("watch_pool", {})
    new_tier2 = []
    for sig in tier2:
        key = sig['ts_code']
        if key in watch_pool and watch_pool[key]['zt_date'] == sig['zt_date']:
            # 已在观察池，更新pos
            watch_pool[key]['pos_20d'] = sig['pos_20d']
            watch_pool[key]['entry_price'] = sig['entry_price']
            continue
        new_tier2.append(sig)

    # 8. 检查观察池升级
    print("[小卓扫描] 检查观察池升级...")
    upgraded = _check_watch_pool_upgrades(current_trade_day, state)
    watch_pool = state.get("watch_pool", {})  # 重新获取（_check可能已删除升级的）
    if upgraded:
        print(f"[小卓扫描] 🔥 {len(upgraded)} 只观察池股票升级!")
        # 升级的也算新的正式信号
        new_tier1.extend(upgraded)

    # 9. 新Tier2加入观察池
    for sig in new_tier2:
        watch_pool[sig['ts_code']] = {
            'zt_date': sig['zt_date'],
            'zt_vol': sig['zt_vol'],
            'name': sig['name'],
            'min_shrink_vol': sig['min_shrink_vol'],
            'min_shrink_ratio': sig['min_shrink_ratio'],
            'min_amount': sig['min_amount'],
            'day_after_zt': sig['day_after_zt'],
            'pos_20d': sig['pos_20d'],
            'entry_price': sig['entry_price'],
            'added_date': current_trade_day,
        }

    # 10. 清理过期观察池（超过15天的移除）
    from datetime import datetime, timedelta
    cutoff = (datetime.strptime(current_trade_day, '%Y%m%d') - timedelta(days=30)).strftime('%Y%m%d')
    expired = [k for k, v in watch_pool.items() if v.get('added_date', '') < cutoff]
    for k in expired:
        print(f"[小卓扫描] 观察池过期移除: {k}")
        del watch_pool[k]

    # 11. 判断是否需要推送
    has_content = new_tier1 or new_tier2 or upgraded

    if not has_content:
        # 即使没有新信号，也推送一个简报告知观察池状态
        if watch_pool:
            lines = ["━━━━━━━━━━━━━━━━━━━━━━"]
            lines.append("📋 小卓策略 · 每日简报")
            lines.append(f"📅 {_format_date(current_trade_day)}")
            lines.append("━━━━━━━━━━━━━━━━━━━━━━")
            lines.append(f"🟢 正式池: 0")
            lines.append(f"👀 观察池: {len(watch_pool)} 只监控中")
            lines.append("")
            for code, info in watch_pool.items():
                lines.append(f"· {code} {info.get('name','')} pos={info.get('pos_20d',0):.3f} 收{info.get('entry_price',0):.2f}")
            lines.append("")
            lines.append("━━━━━━━━━━━━━━━━━━━━━━")
            message = "\n".join(lines)
            print(f"[小卓扫描] 推送简报:\n{message}")
            _send_feishu(message)
        else:
            print("[小卓扫描] 无新信号，观察池为空，跳过推送")

        # 更新state
        state["last_scan_date"] = current_trade_day
        state["watch_pool"] = watch_pool
        _save_state(state)
        return

    # 12. 格式化并推送
    message = _format_message(new_tier1, new_tier2, upgraded, total_scanned, current_trade_day)
    print(f"[小卓扫描] 消息内容:\n{message}")
    _send_feishu(message)

    # 13. 更新state
    for sig in new_tier1:
        notified[sig['ts_code']] = sig['zt_date']
    state["last_scan_date"] = current_trade_day
    state["notified"] = notified
    state["watch_pool"] = watch_pool
    _save_state(state)
    print(f"[小卓扫描] 状态已保存 | 已通知 {len(notified)} 只 | 观察池 {len(watch_pool)} 只")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="小卓策略收盘后扫描器")
    parser.add_argument("--date", type=str, default=None,
                        help="指定交易日 YYYYMMDD，默认从交易日历读取")
    parser.add_argument("--force", action="store_true",
                        help="强制忽略state去重，全推")
    args = parser.parse_args()

    if args.force and os.path.exists(STATE_FILE):
        state = _load_state()
        state["notified"] = {}
        _save_state(state)
        print("[小卓扫描] --force 模式，已清空去重状态")

    run_xiaozhuo_scan(args.date)
