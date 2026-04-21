#!/usr/bin/env python3
"""
新闻预处理脚本 v3 — 两阶段分析的数据准备层

原则：不丢信息，不截断
  - 跨源+跨频道内容去重（同一事件保留最完整版本）
  - 生成紧凑的全量摘要索引（每行~35字，供AI快速扫描全部新闻）
  - 完整内容保留在原文件，AI按序号grep深入

输出：
  - news_scan.txt: 全量摘要索引
    格式: "序号|HH:MM|频道|标题前30字"
  - stdout: 统计信息（供cronjob prompt引用）
"""

import json
import re
import os
from collections import Counter

NEWS_POOL = "/root/.openclaw/workspace/data/raw/news/new_news.json"
SCAN_FILE = "/root/.openclaw/workspace/data/raw/news/news_scan.txt"

# 频道优先级（去重时高优先级频道优先保留）
CHANNEL_RANK = {
    "加红": 100, "要闻": 90, "A股": 80, "宏观": 70,
    "公司": 60, "市场": 55, "行业": 50, "焦点": 45,
    "观点": 40, "快讯": 35, "美股": 30, "港美股": 30,
    "外汇": 25, "大宗": 25, "黄金": 20, "石油": 20,
    "提醒": 15, "基金": 10, "金融": 10, "看盘": 10,
    "全部": 5, "其他": 3, "7*24AI电报": 2,
    "7*24全球直播": 2, "7*24小时全球直播": 2, "7*24": 2,
}

SOURCE_RANK = {
    "新华社": 10, "央视": 10, "财联社": 9, "华尔街见闻": 8,
    "第一财经": 7, "同花顺": 5, "东方财富": 5, "金融界": 5,
    "新浪财经": 4, "云财经": 3, "雪球": 2,
}


def make_dedup_key(content: str) -> str:
    """生成去重key：去空白+去标点，取前200字符"""
    text = re.sub(r'\s+', '', content)
    text = re.sub(r'[，。、；：！？\u201c\u201d\u2018\u2019\uff08\uff09\u3010\u3011\u300a\u300b\[\]]', '', text)
    return text[:200]


def get_title(content: str, max_len: int = 30) -> str:
    """从内容提取紧凑标题"""
    # 优先取【】内的标题
    m = re.match(r'【(.+?)】', content)
    if m and len(m.group(1)) <= max_len:
        return m.group(1)
    # 取第一句话
    m = re.match(r'(.{8,30}?)[，。；]', content)
    if m:
        return m.group(1)
    return content[:max_len]


def main():
    if not os.path.exists(NEWS_POOL):
        print("新闻池不存在，无需处理")
        open(SCAN_FILE, "w").close()
        return

    with open(NEWS_POOL, "r", encoding="utf-8") as f:
        pool = json.load(f)

    news = pool.get("news", [])
    total_raw = len(news)

    if total_raw == 0:
        print("新闻池为空，无需处理")
        open(SCAN_FILE, "w").close()
        return

    # === Step 1: 内容级去重 ===
    # 同一条新闻被多个源/频道报道，只保留最完整+最高优先级的版本
    buckets = {}
    for item in news:
        key = make_dedup_key(item.get("content", ""))
        if not key:
            continue

        if key not in buckets:
            buckets[key] = item
        else:
            existing = buckets[key]
            # 综合评分：频道优先级 × 10 + 来源优先级 + 内容长度权重
            item_score = (CHANNEL_RANK.get(item.get("channel", ""), 0) * 10
                         + SOURCE_RANK.get(item.get("source", ""), 0)
                         + len(item.get("content", "")) * 0.001)
            exist_score = (CHANNEL_RANK.get(existing.get("channel", ""), 0) * 10
                          + SOURCE_RANK.get(existing.get("source", ""), 0)
                          + len(existing.get("content", "")) * 0.001)

            if item_score > exist_score:
                # 保留高优先级的元数据，但内容取更长的
                if len(item.get("content", "")) < len(existing.get("content", "")):
                    item["content"] = existing["content"]
                buckets[key] = item

    deduped = list(buckets.values())
    del buckets

    # === Step 2: 按时间排序（最新在前） ===
    deduped.sort(key=lambda x: x.get("time", ""), reverse=True)

    # === Step 3: 生成紧凑索引 ===
    scan_lines = []
    for i, item in enumerate(deduped):
        # 提取时间
        time_str = item.get("time_raw", "")
        if not time_str and "T" in item.get("time", ""):
            time_str = item["time"].split("T")[1][:5]

        channel = item.get("channel", "")
        title = get_title(item.get("content", ""))
        line = f"{i+1}|{time_str}|{channel}|{title}"
        scan_lines.append(line)

    with open(SCAN_FILE, "w", encoding="utf-8") as f:
        for line in scan_lines:
            f.write(line + "\n")

    # === 统计输出 ===
    total_chars = sum(len(l) for l in scan_lines)
    channel_dist = Counter(item.get("channel", "") for item in deduped)

    print(f"=== 新闻预处理完成 ===")
    print(f"原始: {total_raw} 条")
    print(f"去重后: {len(deduped)} 条 (合并 {total_raw - len(deduped)} 条)")
    print(f"索引: {len(scan_lines)} 行, {total_chars:,} 字符")
    print(f"频道分布(top8): {dict(channel_dist.most_common(8))}")
    print(f"索引文件: {SCAN_FILE}")
    print(f"完整内容: {NEWS_POOL}")


if __name__ == "__main__":
    main()
