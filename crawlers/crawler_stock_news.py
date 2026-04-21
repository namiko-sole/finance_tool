#!/usr/bin/env python3
"""
东方财富股吧个股资讯爬虫
覆盖: 资讯(tab=1) / 公告(tab=3) / 问董秘(type=1最新答复)
数据源: https://guba.eastmoney.com
"""

import requests
from bs4 import BeautifulSoup
import random
import time
import re
import hashlib
import warnings
from datetime import datetime, timedelta
from typing import List, Dict, Optional

warnings.filterwarnings("ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning)

# ============================================================
# User-Agent 池
# ============================================================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_URL = "https://guba.eastmoney.com"


def _get_headers() -> dict:
    """随机返回请求头"""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://guba.eastmoney.com/",
    }


def _fetch_page(url: str, retries: int = 3) -> Optional[str]:
    """
    通用页面获取，带重试和随机延迟
    返回 HTML 文本，失败返回 None
    """
    for attempt in range(retries):
        try:
            time.sleep(random.uniform(1.0, 3.0))
            resp = requests.get(
                url,
                headers=_get_headers(),
                timeout=15,
                verify=False,
            )
            resp.encoding = "utf-8"
            if resp.status_code == 200:
                return resp.text
            else:
                print(f"[WARNING] HTTP {resp.status_code} for {url} (attempt {attempt+1}/{retries})")
        except requests.RequestException as e:
            print(f"[WARNING] Request failed for {url}: {e} (attempt {attempt+1}/{retries})")
    return None


def _parse_list_time(date_str: str) -> str:
    """
    解析股吧列表页时间并转换为北京时间。
    输入: "MM-DD HH:mm" (服务端渲染时间，比北京时间少12小时)
    输出: "YYYY-MM-DD HH:mm" (北京时间)
    
    注意: 东方财富股吧列表页 raw HTML 中的时间不是北京时间，
    浏览器端 JS 会做 +12 小时修正。爬虫读的是 raw HTML，
    因此需要手动加 12 小时。
    """
    now = datetime.now()
    current_year = now.year

    # 解析 "MM-DD HH:mm"
    date_str = date_str.strip()
    try:
        parts = date_str.split()
        if len(parts) == 2:
            md_part, hm_part = parts
        else:
            md_part = date_str
            hm_part = "00:00"

        month, day = md_part.split("-")
        month, day = int(month), int(day)
        hour, minute = hm_part.split(":")
        hour, minute = int(hour), int(minute)
    except (ValueError, IndexError):
        return date_str

    # 判断年份: 如果 MM-DD <= 今天则当年，否则去年
    if (month, day) <= (now.month, now.day):
        year = current_year
    else:
        year = current_year - 1

    # +12 小时修正为北京时间
    dt = datetime(year, month, day, hour, minute) + timedelta(hours=12)

    return dt.strftime("%Y-%m-%d %H:%M")


def _safe_int(text: str) -> int:
    """安全地将文本转为整数"""
    try:
        # 去除可能的空格、逗号、万等单位
        text = text.strip().replace(",", "").replace("，", "")
        if "万" in text:
            return int(float(text.replace("万", "")) * 10000)
        return int(text)
    except (ValueError, TypeError):
        return 0


def _extract_id_from_url(url: str) -> str:
    """从 URL 中提取新闻 ID
    格式: /news,002202,1695465153.html → 1695465153
    """
    match = re.search(r"/news,\d+,(\d+)\.html", url)
    if match:
        return match.group(1)
    return ""


# ============================================================
# 资讯 (tab=1)
# ============================================================
def fetch_news(stock_code: str, max_pages: int = 1) -> List[Dict]:
    """
    抓取资讯列表
    stock_code: 6位纯数字如 "002202"
    max_pages: 最大页数（1-3）
    返回: [{"id", "title", "url", "author", "time", "reads", "comments"}, ...]
    """
    results = []
    seen_ids = set()

    for page in range(1, max_pages + 1):
        if page == 1:
            url = f"{BASE_URL}/list,{stock_code},1,f.html"
        else:
            url = f"{BASE_URL}/list,{stock_code},1,f_{page}.html"

        html = _fetch_page(url)
        if not html:
            print(f"[WARNING] Failed to fetch news page {page} for {stock_code}")
            continue

        soup = BeautifulSoup(html, "html.parser")
        table = soup.select_one("table.default_list")
        if not table:
            print(f"[WARNING] No news table found on page {page} for {stock_code}")
            continue

        rows = table.select("tr")
        for row in rows:
            try:
                # 跳过表头（没有 .read 的行）
                read_div = row.select_one("div.read")
                if not read_div:
                    continue

                reads = _safe_int(read_div.get_text(strip=True))
                comments = _safe_int(row.select_one("div.reply").get_text(strip=True))

                # 标题和链接
                title_a = row.select_one("div.title a")
                if not title_a:
                    continue
                title = title_a.get("title") or title_a.get_text(strip=True)
                href = title_a.get("href", "")
                news_id = _extract_id_from_url(href)
                if not news_id or news_id in seen_ids:
                    continue
                seen_ids.add(news_id)

                full_url = f"{BASE_URL}{href}" if href.startswith("/") else href

                # 作者
                author_div = row.select_one("div.author a")
                author = author_div.get_text(strip=True) if author_div else ""

                # 时间 "MM-DD HH:mm" (raw HTML 非北京时间，由 _parse_list_time 推断年份并+12h修正)
                time_div = row.select_one("div.update")
                raw_time = time_div.get_text(strip=True) if time_div else ""
                time_str = _parse_list_time(raw_time) if raw_time else ""

                results.append({
                    "id": news_id,
                    "title": title,
                    "url": full_url,
                    "author": author,
                    "time": time_str,
                    "reads": reads,
                    "comments": comments,
                })
            except Exception as e:
                print(f"[WARNING] Error parsing news row: {e}")
                continue

    return results


# ============================================================
# 公告 (tab=3)
# ============================================================
def fetch_announcements(stock_code: str, max_pages: int = 1) -> List[Dict]:
    """
    抓取公告列表
    stock_code: 6位纯数字如 "002202"
    max_pages: 最大页数（1-3）
    返回: [{"id", "title", "url", "type", "time", "reads", "comments"}, ...]
    """
    results = []
    seen_ids = set()

    for page in range(1, max_pages + 1):
        if page == 1:
            url = f"{BASE_URL}/list,{stock_code},3,f.html"
        else:
            url = f"{BASE_URL}/list,{stock_code},3,f_{page}.html"

        html = _fetch_page(url)
        if not html:
            print(f"[WARNING] Failed to fetch announcements page {page} for {stock_code}")
            continue

        soup = BeautifulSoup(html, "html.parser")
        table = soup.select_one("table.default_list")
        if not table:
            print(f"[WARNING] No announcement table found on page {page} for {stock_code}")
            continue

        rows = table.select("tr")
        for row in rows:
            try:
                # 跳过表头
                read_div = row.select_one("div.read")
                if not read_div:
                    continue

                reads = _safe_int(read_div.get_text(strip=True))
                comments = _safe_int(row.select_one("div.reply").get_text(strip=True))

                # 标题和链接
                title_a = row.select_one("div.title a")
                if not title_a:
                    continue
                title = title_a.get("title") or title_a.get_text(strip=True)
                href = title_a.get("href", "")
                news_id = _extract_id_from_url(href)
                if not news_id or news_id in seen_ids:
                    continue
                seen_ids.add(news_id)

                full_url = f"{BASE_URL}{href}" if href.startswith("/") else href

                # 公告类型
                type_div = row.select_one("div.notice_type")
                ann_type = type_div.get_text(strip=True) if type_div else ""

                # 时间 — 公告页 raw HTML 为 "MM-DD HH:mm" (比北京时间少12h，由 _parse_list_time 修正)
                time_div = row.select_one("div.update")
                raw_time = time_div.get_text(strip=True) if time_div else ""
                # 判断是否含年份
                if raw_time and re.match(r"\d{4}-\d{2}-\d{2}", raw_time):
                    time_str = raw_time
                elif raw_time:
                    time_str = _parse_list_time(raw_time)
                else:
                    time_str = ""

                results.append({
                    "id": news_id,
                    "title": title,
                    "url": full_url,
                    "type": ann_type,
                    "time": time_str,
                    "reads": reads,
                    "comments": comments,
                })
            except Exception as e:
                print(f"[WARNING] Error parsing announcement row: {e}")
                continue

    return results


# ============================================================
# 问董秘 (type=1最新答复)
# ============================================================
def fetch_qa(stock_code: str, max_pages: int = 1) -> List[Dict]:
    """
    抓取问董秘(type=1最新答复)
    使用新版 URL: /qa/search?type=1&code={code}
    页面结构: .qa_list_item 卡片式，含 .qa_question_text / .qa_answer_text / .qa_answer_date
    返回: [{"question", "answer", "time", "id"}, ...]
    注意: 问董秘仅支持单页HTML抓取（约15条），服务端无分页API，max_pages 参数无效
    """
    results = []
    seen_hashes = set()

    if max_pages > 1:
        print("[INFO] 问董秘仅支持单页抓取（约15条），忽略 max_pages 参数")

    for page in range(1, max_pages + 1):
        if page >= 2:
            break

        # 首页使用服务端渲染的 HTML
        url = f"{BASE_URL}/qa/search?type=1&code={stock_code}"
        html = _fetch_page(url)
        if not html:
            print(f"[WARNING] Failed to fetch QA page 1 for {stock_code}")
            continue

        soup = BeautifulSoup(html, "html.parser")
        items = soup.select(".qa_list_item")

        for item in items:
            try:
                # 问题
                question_div = item.select_one(".qa_question_text")
                question_text = question_div.get_text(strip=True) if question_div else ""
                # 去掉 "金风科技股友：" 前缀
                question_text = re.sub(r'^.+?股友[：:]\s*', '', question_text).strip() if question_text else ""

                # 回答
                answer_p = item.select_one(".qa_answer_text p")
                answer_text = answer_p.get_text(strip=True) if answer_p else ""
                # 去掉 "金风科技：" 前缀
                answer_text = re.sub(r'^.+?[：:]\s*', '', answer_text).strip() if answer_text else ""

                # 时间 — 在 .qa_answer_date 中，格式 "YYYY-MM-DD HH:MM:SS"
                date_div = item.select_one(".qa_answer_date")
                raw_time = date_div.get_text(strip=True) if date_div else ""
                # 去掉 "来自深交所互动易 " 前缀
                time_match = re.search(r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", raw_time)
                time_str = time_match.group(1) if time_match else raw_time

                # 用 question 内容 hash 去重
                content_hash = hashlib.md5(question_text.encode("utf-8")).hexdigest()
                if content_hash in seen_hashes:
                    continue
                seen_hashes.add(content_hash)

                # 提取新闻链接中的 ID
                link = item.select_one("a[href*='news']")
                news_id = ""
                if link:
                    news_id = _extract_id_from_url(link.get("href", ""))

                results.append({
                    "id": news_id,
                    "question": question_text,
                    "answer": answer_text,
                    "time": time_str,
                })
            except Exception as e:
                print(f"[WARNING] Error parsing QA item: {e}")
                continue

    return results


# ============================================================
# 统一入口
# ============================================================
def fetch_all(stock_code: str, max_pages: int = 1) -> Dict:
    """
    统一入口，返回 {"news": [...], "announcements": [...], "qa": [...]}
    """
    return {
        "news": fetch_news(stock_code, max_pages),
        "announcements": fetch_announcements(stock_code, max_pages),
        "qa": fetch_qa(stock_code, max_pages),
    }


# ============================================================
# CLI 入口
# ============================================================
if __name__ == "__main__":
    import json
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="东方财富股吧个股资讯爬虫")
    parser.add_argument("stock_code", help="股票代码，如 002202")
    parser.add_argument(
        "--type",
        choices=["news", "announcements", "qa", "all"],
        default="all",
        help="数据类型: news=资讯, announcements=公告, qa=问董秘, all=全部",
    )
    parser.add_argument("--pages", type=int, default=1, help="最大页数 (默认1)")
    args = parser.parse_args()

    code = args.stock_code.strip()
    if not re.match(r"^\d{6}$", code):
        print(f"[ERROR] 无效股票代码: {code}，请输入6位纯数字", file=sys.stderr)
        sys.exit(1)

    if args.type == "all":
        data = fetch_all(code, args.pages)
    elif args.type == "news":
        data = {"news": fetch_news(code, args.pages)}
    elif args.type == "announcements":
        data = {"announcements": fetch_announcements(code, args.pages)}
    elif args.type == "qa":
        data = {"qa": fetch_qa(code, args.pages)}
    else:
        data = {}

    print(json.dumps(data, ensure_ascii=False, indent=2))
