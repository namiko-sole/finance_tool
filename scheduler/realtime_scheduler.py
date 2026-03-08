#!/usr/bin/env python3
"""
A股实时行情爬虫 - 定时任务版
- 每天9:10启动，15:10自动退出
- 交易时段: 9:15-11:30, 13:00-15:00
- 每3分钟执行一次
- 数据存到 stock_daily/{ts_code}.csv
"""

import os
import sys
import time
import json
import pandas as pd
import akshare as ak
from datetime import datetime, time as dt_time

# 配置
DATA_DIR = "/root/.openclaw/workspace/data/raw"
STOCK_DAILY_DIR = os.path.join(DATA_DIR, "stock_daily")
TRADE_CALENDAR_PATH = os.path.join(DATA_DIR, "trade_calendar_info.json")
LOG_FILE = "/root/.openclaw/workspace/finance_tool/scheduler/realtime_scheduler.log"

# 交易时段
MORNING_START = dt_time(9, 15)
MORNING_END = dt_time(11, 30)
AFTERNOON_START = dt_time(13, 0)
AFTERNOON_END = dt_time(17, 0)

# 每日退出时间
DAILY_QUIT_TIME = dt_time(17, 10)

import random


def get_random_interval():
    """随机生成5-6分钟的间隔（秒）"""
    return random.randint(300, 360)  # 5-6分钟


def log(msg):
    """带时间戳的日志"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def is_trading_hours():
    """判断当前是否在交易时段"""
    now = datetime.now().time()
    
    # 上午时段: 9:15-11:30
    if MORNING_START <= now <= MORNING_END:
        return True
    # 下午时段: 13:00-15:00
    if AFTERNOON_START <= now <= AFTERNOON_END:
        return True
    return False


def should_exit():
    """判断是否应该退出（超过每日退出时间）"""
    now = datetime.now().time()
    return now >= DAILY_QUIT_TIME


def is_trading_day():
    """判断今天是否为交易日"""
    if not os.path.exists(TRADE_CALENDAR_PATH):
        log(f"交易日历文件不存在: {TRADE_CALENDAR_PATH}")
        return False
    
    with open(TRADE_CALENDAR_PATH, "r", encoding="utf-8") as f:
        calendar_info = json.load(f)
    
    current_trade_day = calendar_info.get("current_trade_day")
    today_str = datetime.now().strftime("%Y%m%d")
    
    if current_trade_day == today_str:
        return True
    return False


def convert_code(raw_code):
    """将AKShare代码转换为tushare格式: 000001/sh600000/bj920000 -> 000001.SZ/600000.SH/920000.BJ"""
    raw_code = str(raw_code).lower()  # 转小写，处理新格式如 bj920000

    # 处理交易所前缀格式: sh600000/sz000001/bj920000
    if len(raw_code) == 8 and raw_code[:2] in ("sh", "sz", "bj") and raw_code[2:].isdigit():
        return f"{raw_code[2:]}.{raw_code[:2].upper()}"

    # 处理新格式: bj920000 -> 920000.BJ
    if raw_code.startswith("bj"):
        num_part = raw_code[2:]  # 取字母后面的数字部分
        if len(num_part) == 6 and num_part.isdigit():
            return f"{num_part}.BJ"

    # 处理纯数字格式: 000001 -> 000001.SZ/SH
    if not raw_code or len(raw_code) != 6:
        return None

    if raw_code.startswith(("4", "8")):
        return f"{raw_code}.BJ"
    elif raw_code.startswith("6"):
        return f"{raw_code}.SH"
    else:
        return f"{raw_code}.SZ"


def fetch_and_save_realtime():
    """获取实时数据并保存"""
    log("开始获取实时行情数据...")
    
    # 重试机制
    max_retries = 3
    for attempt in range(max_retries):
        try:
            df = ak.stock_zh_a_spot()
            if df is not None and len(df) > 0:
                log(f"获取到 {len(df)} 只股票数据")
                # 临时保存到当前文件目录下，方便调试查看
                df.to_csv(os.path.join(os.path.dirname(__file__), "temp_stock_zh_a_spot.csv"), index=False, encoding="utf-8-sig")
                break
        except Exception as e:
            log(f"获取数据失败 (尝试 {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                import time
                time.sleep(5)  # 等待5秒后重试
            else:
                log("获取数据失败，已达最大重试次数")
                return False
    
    if df is None or len(df) == 0:
        log("获取数据为空")
        return False
    
    # 列名映射 (AKShare -> tushare格式)
    column_map = {
        "代码": "ts_code",
        "名称": "name",
        "最新价": "close",
        "涨跌额": "change",
        "涨跌幅": "pct_chg",
        "昨收": "pre_close",
        "今开": "open",
        "最高": "high",
        "最低": "low",
        "成交量": "vol",
        "成交额": "amount",
    }
    
    # 转换列名
    df = df.rename(columns=column_map)

    # 转换代码格式
    df["ts_code"] = df["ts_code"].apply(convert_code)
    before_drop = len(df)
    df = df.dropna(subset=["ts_code"])
    log(f"代码转换后保留 {len(df)} / {before_drop} 条")
    
    # 添加交易日期
    today_str = datetime.now().strftime("%Y%m%d")
    df["trade_date"] = today_str
    
    # 保留需要的列（与历史日线格式一致）
    columns = ["ts_code", "trade_date", "open", "high", "low", "close", 
               "pre_close", "change", "pct_chg", "vol", "amount"]
    
    # 检查列是否存在，不存在的用0填充
    for col in columns:
        if col not in df.columns:
            log(f"警告: 缺少列 {col}，用0填充")
            df[col] = 0
    
    df = df[columns]
    
    # 按股票保存
    saved_count = 0
    skipped_count = 0
    
    try:
        for _, row in df.iterrows():
            ts_code = row["ts_code"]
            csv_path = os.path.join(STOCK_DAILY_DIR, f"{ts_code}.csv")
            
            # 如果文件不存在，跳过
            if not os.path.exists(csv_path):
                skipped_count += 1
                continue
            
            try:
                # 读取现有数据
                existing_df = pd.read_csv(csv_path, encoding="utf-8-sig")
                
                # 检查是否已有今天的数据，有则更新，无则追加
                today_mask = existing_df["trade_date"].astype(str) == today_str
                
                if today_mask.any():
                    # 更新今天的记录
                    existing_df.loc[today_mask, columns[2:]] = row[columns[2:]].values
                else:
                    # 追加新记录
                    existing_df = pd.concat([existing_df, row.to_frame().T], ignore_index=True)

                # 统一按交易日倒序，确保最新数据在文件顶部
                existing_df["trade_date"] = pd.to_numeric(existing_df["trade_date"], errors="coerce")
                existing_df = existing_df.sort_values("trade_date", ascending=False, na_position="last")
                existing_df["trade_date"] = existing_df["trade_date"].astype("Int64").astype(str)
                
                # 保存
                existing_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
                saved_count += 1
                
            except Exception as e:
                log(f"保存 {ts_code} 失败: {e}")
    except Exception as e:
        log(f"处理数据时出错: {e}")
    
    log(f"实时数据更新完成: 保存 {saved_count} 只，跳过 {skipped_count} 只")
    return True


def main():
    """主循环"""
    log("=" * 50)
    log("实时行情爬虫启动")
    log("=" * 50)
    
    # 确保目录存在
    os.makedirs(STOCK_DAILY_DIR, exist_ok=True)
    
    while True:
        try:
            # 检查是否为交易日
            if not is_trading_day():
                log("今天不是交易日，退出")
                break

            # 检查是否超过退出时间
            if should_exit():
                now = datetime.now().strftime("%H:%M:%S")
                log(f"当前 {now} 已超过退出时间 {DAILY_QUIT_TIME}，退出")
                break
            
            # 检查是否在交易时段
            if not is_trading_hours():
                now = datetime.now().strftime("%H:%M:%S")
                log(f"当前 {now} 不在交易时段 (9:15-11:30, 13:00-15:00)，休眠等待...")
                time.sleep(60)  # 非交易时段每分钟检查一次
                continue
            
            # 获取并保存数据
            fetch_and_save_realtime()
            
        except KeyboardInterrupt:
            log("收到中断信号，退出")
            break
        except Exception as e:
            log(f"发生异常: {e}")
        
        # 休眠3-4分钟（随机）
        interval = get_random_interval()
        log(f"休眠 {interval} 秒...")
        time.sleep(interval)
    
    log("程序退出")
    sys.exit(0)


if __name__ == "__main__":
    main()
