#!/usr/bin/env python3
"""
缠论买点过滤器 - 飞书推送脚本

运行 chanlun_filter.py 并将结果以卡片形式推送到飞书群
"""

import subprocess
import sys
import os
import json
import argparse
from datetime import datetime

WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/9f8d3430-740c-4c39-a2e4-4ae06885109d"
SCRIPT_DIR = "/root/.openclaw/workspace/finance_tool/analyze/stock_filter"
OUTPUT_DIR = "/root/.openclaw/workspace/output"
PYTHON_BIN = "/root/.openclaw/workspace/venv/bin/python"


def is_trading_day():
    """检查今天是否为交易日"""
    import pandas as pd
    today = datetime.now().strftime('%Y%m%d')
    cal_path = "/root/.openclaw/workspace/data/raw/trade_calendar.csv"
    if not os.path.exists(cal_path):
        return True  # 找不到日历，默认执行
    cal = pd.read_csv(cal_path)
    today_row = cal[cal['cal_date'].astype(str) == today]
    if today_row.empty:
        return True
    return int(today_row.iloc[0]['is_open']) == 1


def build_card_message(title: str, body: str) -> dict:
    """构建飞书卡片消息"""
    return {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {
                    "content": title,
                    "tag": "plain_text"
                }
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": body
                    }
                }
            ]
        }
    }


def send_feishu(card_payload: dict) -> bool:
    """发送卡片消息到飞书"""
    import urllib.request
    import urllib.error

    data = json.dumps(card_payload).encode('utf-8')
    req = urllib.request.Request(
        WEBHOOK_URL,
        data=data,
        headers={'Content-Type': 'application/json'}
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read().decode('utf-8'))
            if result.get('code') == 0 or result.get('StatusCode') == 0:
                print("✅ 飞书推送成功")
                return True
            else:
                print(f"❌ 飞书推送失败: {result}")
                return False
    except Exception as e:
        print(f"❌ 飞书推送异常: {e}")
        return False


def run_filter_and_push(add_virtual_day: bool = False, days: int = 3):
    """运行缠论过滤器并推送结果"""
    now = datetime.now()
    time_str = now.strftime('%Y-%m-%d %H:%M')

    # 检查交易日
    if not is_trading_day():
        print(f"今日({now.strftime('%Y%m%d')})非交易日，跳过执行")
        return

    print(f"========== [{time_str}] 缠论买点过滤器 ==========")
    print(f"虚拟交易日模式: {'是' if add_virtual_day else '否'}")

    # 构建命令
    cmd = [PYTHON_BIN, os.path.join(SCRIPT_DIR, "chanlun_filter.py")]
    if add_virtual_day:
        cmd.append("--add-virtual-day")

    # 运行脚本
    print(f"执行: {' '.join(cmd)}")
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,
            cwd=SCRIPT_DIR
        )
        stdout = proc.stdout
        stderr = proc.stderr
        print(stdout[-2000:] if len(stdout) > 2000 else stdout)  # 打印最后2000字符
        if stderr:
            print("STDERR:", stderr[-1000:] if len(stderr) > 1000 else stderr)
    except subprocess.TimeoutExpired:
        print("❌ 脚本执行超时（10分钟）")
        return
    except Exception as e:
        print(f"❌ 执行异常: {e}")
        return

    # 解析输出获取结果
    # 查找结果CSV
    csv_path = os.path.join(OUTPUT_DIR, f"chanlun_bulao_buy_resonance_last_{days}_days.csv")
    stock_count = 0
    results = []

    if os.path.exists(csv_path):
        import pandas as pd
        try:
            df = pd.read_csv(csv_path)
            stock_count = len(df)
            results = df.to_dict('records')
        except Exception as e:
            print(f"读取CSV失败: {e}")

    # 构建推送消息
    title_time = "尾盘" if add_virtual_day else time_str.split(' ')[1]
    title = f"📊 缠论买点 | {title_time}"

    if stock_count == 0:
        body = f"**时间**: {time_str}\n**模式**: {'尾盘虚拟日' if add_virtual_day else '常规'}\n\n暂无共振买点个股"
    else:
        rows = []
        for r in results:
            code = r.get('ts_code', '')
            name = r.get('name', '')
            sig_date = r.get('signal_date', '')
            macd = r.get('macd_value', 0)
            ch_price = r.get('chanlun_price', 0)
            rows.append(
                f"• **{name}({code})** | 信号:{sig_date} | 缠论价:{ch_price:.2f} | MACD:{macd:.4f}"
            )
        body = (
            f"**时间**: {time_str}\n"
            f"**模式**: {'尾盘虚拟日' if add_virtual_day else '常规'}\n"
            f"**共振个股**: {stock_count} 只\n\n"
            + "\n".join(rows[:20])  # 最多显示20只
        )
        if stock_count > 20:
            body += f"\n\n_...还有 {stock_count - 20} 只，点上方链接查看完整列表_"

    card = build_card_message(title, body)
    send_feishu(card)
    print(f"推送完成，共 {stock_count} 只个股")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--add-virtual-day', action='store_true')
    args = parser.parse_args()

    days = 4 if args.add_virtual_day else 3
    run_filter_and_push(add_virtual_day=args.add_virtual_day, days=days)


if __name__ == "__main__":
    main()
