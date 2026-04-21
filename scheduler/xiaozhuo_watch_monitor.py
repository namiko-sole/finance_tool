#!/usr/bin/env python3
"""
小卓策略(v5.1)观察池盘中监控
读取观察池state → 用实时数据计算pos → pos降到<0.5立刻推送升级通知到小八交流群

出场规则(v5.1时间阶梯止损):
  止损-8% | 赚5%卖30% | 剩余等20%清仓 | 持15天
  tp1触发后: 1-5天止损-8% → 6-10天止损-3% → 11-15天保本
"""
import json, os, sys, subprocess
import numpy as np
import pandas as pd
from datetime import datetime

STATE_PATH = "/root/.openclaw/workspace/finance_tool/scheduler/xiaozhuo_scanner_state.json"
DATA_DIR = "/root/.openclaw/workspace/data/raw/stock_daily/"
REALTIME_PATH = "/root/.openclaw/workspace/finance_tool/scheduler/realtime_stock_zh_a_spot.csv"
FEISHU_SCRIPT = "/root/.openclaw/workspace/skills/feishu-webhook/scripts/send_message.sh"
POS_UPGRADE_THRESHOLD = 0.5


def load_state():
    if not os.path.exists(STATE_PATH):
        return None
    with open(STATE_PATH, 'r') as f:
        return json.load(f)


def save_state(state):
    with open(STATE_PATH, 'w') as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def get_realtime_price(ts_code):
    """从实时数据CSV获取最新价"""
    if not os.path.exists(REALTIME_PATH):
        return None
    
    # ts_code格式: 600488.SH → 需要转成CSV里的格式
    parts = ts_code.split('.')
    if len(parts) != 2:
        return None
    num, suffix = parts
    
    # CSV里的代码格式：sh600000, sz000001, bj920000
    suffix_map = {'SH': 'sh', 'SZ': 'sz', 'BJ': 'bj'}
    prefix = suffix_map.get(suffix.upper(), '')
    if not prefix:
        return None
    
    csv_code = f"{prefix}{num}"
    
    try:
        # 只读需要的行，避免加载全部
        df = pd.read_csv(REALTIME_PATH, encoding='utf-8-sig', dtype={'代码': str})
        row = df[df['代码'] == csv_code]
        if row.empty:
            return None
        price = float(row.iloc[0]['最新价'])
        chg_pct = float(row.iloc[0]['涨跌幅'])
        return {'price': price, 'chg_pct': chg_pct}
    except Exception as e:
        print(f"获取实时价格失败 {ts_code}: {e}")
        return None


def calc_realtime_pos(ts_code, realtime_price):
    """计算实时20日区间位置"""
    csv_path = os.path.join(DATA_DIR, f"{ts_code}.csv")
    if not os.path.exists(csv_path):
        return None
    
    try:
        df = pd.read_csv(csv_path, encoding='utf-8-sig').sort_values('trade_date')
        closes = df['close'].values
        
        # 取最近19个收盘价 + 今天实时价
        recent_19 = closes[-19:]
        all_prices = np.append(recent_19, realtime_price)
        
        high_20 = float(np.max(all_prices))
        low_20 = float(np.min(all_prices))
        
        if high_20 > low_20:
            pos = (realtime_price - low_20) / (high_20 - low_20)
        else:
            pos = 0.5
        
        return float(pos)
    except Exception as e:
        print(f"计算pos失败 {ts_code}: {e}")
        return None


def send_feishu(message):
    """推送飞书"""
    try:
        # 消息写入临时文件避免shell转义问题
        tmp_file = "/tmp/xiaozhuo_watch_alert.txt"
        with open(tmp_file, 'w') as f:
            f.write(message)
        
        result = subprocess.run(
            ['bash', FEISHU_SCRIPT, message, 'card'],
            capture_output=True, text=True, timeout=30
        )
        print(f"飞书推送: {result.stdout.strip()}")
        return True
    except Exception as e:
        print(f"飞书推送失败: {e}")
        return False


def main():
    now = datetime.now()
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 观察池盘中监控启动")

    # 1. 加载state
    state = load_state()
    if not state:
        print("无state文件，退出")
        return

    watch_pool = state.get('watch_pool', {})
    if not watch_pool:
        print("观察池为空，退出")
        return

    print(f"观察池共 {len(watch_pool)} 只股票")

    # 2. 逐只检查
    upgraded = []
    still_watching = {}

    for code, info in watch_pool.items():
        rt = get_realtime_price(code)
        if rt is None:
            print(f"  {code}: 无法获取实时价格，保留观察")
            still_watching[code] = info
            continue

        current_pos = calc_realtime_pos(code, rt['price'])
        if current_pos is None:
            print(f"  {code}: 无法计算pos，保留观察")
            still_watching[code] = info
            continue

        name = info.get('name', code)
        old_pos = info.get('pos_20d', '?')
        print(f"  {code} {name}: 实时价{rt['price']:.2f} pos {old_pos} → {current_pos:.3f} 涨跌{rt['chg_pct']:+.2f}%")

        if current_pos < POS_UPGRADE_THRESHOLD:
            # 🎉 升级！pos降到0.5以下
            upgraded.append({
                'code': code,
                'name': name,
                'price': rt['price'],
                'chg_pct': rt['chg_pct'],
                'old_pos': old_pos,
                'new_pos': current_pos,
                'zt_date': info.get('zt_date', ''),
                'entry_price': info.get('entry_price', 0),
            })
            print(f"  ✅ {code} {name} 升级！pos {old_pos} → {current_pos:.3f}")
        else:
            # 还在观察范围
            info['current_pos'] = current_pos
            info['current_price'] = rt['price']
            still_watching[code] = info

    # 3. 如果有升级，推送通知
    if upgraded:
        weekday = ['周一','周二','周三','周四','周五','周六','周日'][now.weekday()]
        msg_lines = [
            "━━━━━━━━━━━━━━━━━━━━━━",
            "🚨 小卓策略 · 观察池升级通知！",
            "━━━━━━━━━━━━━━━━━━━━━━",
            f"📅 {now.strftime('%Y年%m月%d日')}（{weekday}）{now.strftime('%H:%M')}",
            "",
            f"以下观察池股票pos已降到0.5以下，",
            f"升级为正式池！",
            "",
        ]

        for i, s in enumerate(upgraded, 1):
            ep = s['entry_price']
            sl_price = ep * 0.92
            tp1_price = ep * 1.05
            tp2_price = ep * 1.20
            msg_lines.extend([
                f"🟢 {s['code']} {s['name']}",
                f"  现价{s['price']:.2f} 涨跌{s['chg_pct']:+.2f}%",
                f"  pos {s['old_pos']} → {s['new_pos']:.3f} ✅",
                f"  涨停日{s['zt_date']}（次日开盘入场）",
                f"  💰 出场规则:",
                f"  🔴止损{sl_price:.2f}(-8%)",
                f"  🟢赚5%卖30%({tp1_price:.2f})",
                f"  🚀剩余等+20%({tp2_price:.2f})清仓",
                f"  ⏰最多持仓15天",
                f"  📊时间阶梯: tp1后 1-5天-8% → 6-10天-3% → 11-15天保本",
                "",
            ])

        msg_lines.append("━━━━━━━━━━━━━━━━━━━━━━")

        msg = "\n".join(msg_lines)
        send_feishu(msg)
        print(f"\n📤 已推送升级通知 ({len(upgraded)}只)")

    # 4. 更新state
    state['watch_pool'] = still_watching
    # 已升级的移入正式记录
    for s in upgraded:
        state.setdefault('upgraded_log', []).append({
            'code': s['code'],
            'name': s['name'],
            'upgraded_at': now.strftime('%Y%m%d %H%M'),
            'old_pos': str(s['old_pos']),
            'new_pos': round(s['new_pos'], 3),
        })
    save_state(state)
    print(f"State已更新: 观察池{len(still_watching)}只, 本次升级{len(upgraded)}只")


if __name__ == "__main__":
    main()
