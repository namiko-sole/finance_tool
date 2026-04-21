#!/usr/bin/env python3
"""
巨潮资讯(cninfo.com.cn)招股说明书下载爬虫

从巨潮资讯网批量下载A股招股说明书PDF文件。
搜索策略:
  1. 优先通过 secid(orgId) 精确搜索该股票的招股公告
  2. 若无结果，回退到 searchkey(公司名+招股) 全文搜索

用法:
    # 下载所有A股招股说明书
    python crawler_cninfo_prospectus.py

    # 仅测试前5只股票（不下载）
    python crawler_cninfo_prospectus.py --dry-run --limit 5

    # 从指定股票代码开始继续
    python crawler_cninfo_prospectus.py --start-from 601888

    # 仅下载3只股票
    python crawler_cninfo_prospectus.py --limit 3
"""

import argparse
import csv
import json
import logging
import os
import random
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from curl_cffi import requests

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------
CNINFO_QUERY_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
CNINFO_PDF_BASE = "https://static.cninfo.com.cn/"
CNINFO_STOCK_URL = "https://www.cninfo.com.cn/new/data/szse_stock.json"

DATA_DIR = "/root/.openclaw/workspace/data/raw"
STOCK_LIST_PATH = os.path.join(DATA_DIR, "stock_list.csv")
OUTPUT_DIR = os.path.join(DATA_DIR, "stock_info", "prospectus")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

MIN_DELAY = 5
MAX_DELAY = 10
MIN_PDF_SIZE_KB = 500
MIN_PDF_SIZE_KB_RELAXED = 100
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 10
RATE_LIMIT_WAIT = 60
PROGRESS_LOG_INTERVAL = 20

# 标题排除模式（不是招股说明书）
EXCLUDE_PATTERNS = [
    "摘要",
    "附录",
    "补充",
    "H股",
    "确认意见",
    "责任保险",
    "责任险",
    "投保",
    "提示性公告",
    "致歉",
    "更正",
    "修订说明",
]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# cninfo 股票列表 (orgId 映射)
# ---------------------------------------------------------------------------

def fetch_cninfo_stock_map(session: requests.Session) -> Dict[str, str]:
    """
    从 cninfo 获取所有股票代码 -> orgId 的映射。

    返回: {"000001": "gssz0000001", "600000": "gssh0600000", ...}
    """
    try:
        resp = session.get(
            CNINFO_STOCK_URL,
            headers={
                "Referer": "https://www.cninfo.com.cn/new/disclosure",
                "Accept": "application/json, text/plain, */*",
            },
            impersonate="chrome",
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        stock_list = data.get("stockList", [])
        return {s["code"]: s["orgId"] for s in stock_list if s.get("code") and s.get("orgId")}
    except Exception as e:
        logger.error("获取 cninfo 股票列表失败: %s", e)
        return {}


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def load_stock_list(
    path: str = STOCK_LIST_PATH,
    skip_bse: bool = True,
) -> List[Dict[str, str]]:
    """从CSV加载股票列表，返回 [{ts_code, symbol, name}, ...]"""
    stocks: List[Dict[str, str]] = []
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts_code = row.get("ts_code", "")
            if skip_bse and ts_code.endswith(".BJ"):
                continue
            stocks.append({
                "ts_code": ts_code,
                "symbol": row.get("symbol", ""),
                "name": row.get("name", ""),
            })
    return stocks


def get_random_headers() -> Dict[str, str]:
    """生成带随机UA的请求头"""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": "https://www.cninfo.com.cn/new/disclosure",
        "Origin": "https://www.cninfo.com.cn",
    }


def get_save_filename(ts_code: str) -> str:
    """生成保存文件名: {ts_code}_招股说明书.pdf"""
    return f"{ts_code}_招股说明书.pdf"


def is_already_downloaded(output_dir: str, ts_code: str) -> bool:
    """检查是否已下载（文件存在且 > 1KB）"""
    filepath = os.path.join(output_dir, get_save_filename(ts_code))
    if not os.path.exists(filepath):
        return False
    return os.path.getsize(filepath) > 1024


# ---------------------------------------------------------------------------
# API 搜索
# ---------------------------------------------------------------------------

def _api_post(
    session: requests.Session,
    form_data: Dict[str, str],
    max_retries: int = MAX_RETRIES,
) -> Optional[Dict]:
    """发送 POST 请求到 cninfo 公告查询 API"""
    for attempt in range(max_retries):
        try:
            headers = get_random_headers()
            resp = session.post(
                CNINFO_QUERY_URL,
                data=form_data,
                headers=headers,
                impersonate="chrome",
                timeout=30,
            )

            if resp.status_code == 429:
                wait = RATE_LIMIT_WAIT * (attempt + 1)
                logger.warning("被限流(429)，等待 %ds 后重试 (第%d次)", wait, attempt + 1)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except Exception as e:
            if attempt < max_retries - 1:
                wait = RETRY_BACKOFF_BASE * (2 ** attempt) + random.uniform(0, 5)
                logger.warning("请求失败 (第%d次): %s, %.1fs 后重试", attempt + 1, e, wait)
                time.sleep(wait)
            else:
                logger.error("请求失败，已达最大重试次数: %s", e)
                return None

    return None


def search_by_secid(
    session: requests.Session,
    org_id: str,
    max_retries: int = MAX_RETRIES,
) -> List[Dict]:
    """通过 secid(orgId) 精确搜索该股票的招股公告"""
    form_data = {
        "secid": org_id,
        "searchkey": "招股",
        "sdate": "1990-01-01",
        "edate": datetime.now().strftime("%Y-%m-%d"),
        "isfulltext": "false",
        "sortName": "nothing",
        "sortType": "asc",
        "pageNum": "1",
        "pageSize": "30",
    }

    data = _api_post(session, form_data, max_retries)
    if data is None:
        return []

    announcements = data.get("announcements") or []
    total = data.get("totalAnnouncement", 0)
    logger.debug("secid搜索返回 %d 条公告 (总计 %d)", len(announcements), total)
    return announcements


def search_by_name(
    session: requests.Session,
    name: str,
    max_retries: int = MAX_RETRIES,
) -> List[Dict]:
    """通过公司名称全文搜索招股说明书（回退方案）"""
    form_data = {
        "searchkey": f"{name} 招股说明书",
        "sdate": "1990-01-01",
        "edate": datetime.now().strftime("%Y-%m-%d"),
        "isfulltext": "false",
        "sortName": "nothing",
        "sortType": "asc",
        "pageNum": "1",
        "pageSize": "30",
    }

    data = _api_post(session, form_data, max_retries)
    if data is None:
        return []

    announcements = data.get("announcements") or []
    total = data.get("totalAnnouncement", 0)
    logger.debug("名称搜索 '%s' 返回 %d 条公告 (总计 %d)", name, len(announcements), total)
    return announcements


# ---------------------------------------------------------------------------
# 结果过滤
# ---------------------------------------------------------------------------

def _is_excluded_title(title: str) -> bool:
    """检查标题是否应被排除"""
    return any(pat in title for pat in EXCLUDE_PATTERNS)


def _rank_score(ann: Dict) -> Tuple[int, int]:
    """为公告计算排名分数 (越低越好)"""
    title = ann.get("announcementTitle", "")
    size = ann.get("adjunctSize", 0)

    if title == "招股说明书":
        return (0, -size)
    if "首次公开发行" in title and "招股说明" in title:
        return (1, -size)
    if "招股说明" in title:
        return (2, -size)
    # 招股意向书等
    return (3, -size)


def _filter_candidates(
    announcements: List[Dict],
    symbol: str,
    min_size: int,
) -> List[Dict]:
    """从公告列表中筛选候选招股说明书"""
    # 先尝试按 secCode 过滤
    matched = [
        ann for ann in announcements
        if ann.get("secCode", "").strip() == symbol
    ]
    # 若 secCode 过滤后为空且回退搜索结果非空，使用全量
    if not matched:
        matched = announcements

    candidates = []
    for ann in matched:
        title = ann.get("announcementTitle", "")
        if _is_excluded_title(title):
            continue
        if "招股" not in title:
            continue
        # 检查文件类型: 只接受 PDF
        url = ann.get("adjunctUrl", "")
        if not url.upper().endswith(".PDF"):
            continue
        if ann.get("adjunctSize", 0) >= min_size:
            candidates.append(ann)

    return candidates


def filter_prospectus(
    announcements: List[Dict],
    symbol: str,
) -> Optional[Dict]:
    """
    从搜索结果中筛选最佳招股说明书。

    策略:
    1. 排除非招股说明书标题 (摘要/附录/H股等)
    2. 标题必须包含"招股"
    3. 只接受 PDF 文件 (排除 HTML 等老格式)
    4. 文件大小过滤 (>500KB, 放宽到 >100KB)
    5. 排名: 精确匹配 > 首次公开发行 > 其他招股 > 招股意向书
    6. 同级别取最大文件
    """
    # 第一轮: 严格大小过滤
    candidates = _filter_candidates(announcements, symbol, MIN_PDF_SIZE_KB)

    # 第二轮: 放宽大小过滤
    if not candidates:
        candidates = _filter_candidates(announcements, symbol, MIN_PDF_SIZE_KB_RELAXED)

    if not candidates:
        return None

    candidates.sort(key=_rank_score)
    return candidates[0]


# ---------------------------------------------------------------------------
# PDF 下载
# ---------------------------------------------------------------------------

def download_pdf(
    session: requests.Session,
    adjunct_url: str,
    save_path: str,
    max_retries: int = MAX_RETRIES,
) -> bool:
    """下载PDF文件，验证内容有效性"""
    url = CNINFO_PDF_BASE + adjunct_url

    for attempt in range(max_retries):
        try:
            headers = get_random_headers()
            resp = session.get(
                url,
                headers=headers,
                impersonate="chrome",
                timeout=60,
            )

            if resp.status_code == 429:
                wait = RATE_LIMIT_WAIT * (attempt + 1)
                logger.warning("下载被限流(429)，等待 %ds", wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()

            content = resp.content
            if len(content) < 1024:
                logger.warning("下载文件过小 (%d bytes)，可能无效", len(content))
                continue

            # 验证 PDF 魔术字节
            if not content[:5].startswith(b"%PDF-"):
                logger.warning("下载内容不是有效PDF (前5字节: %s)", content[:10])
                continue

            with open(save_path, "wb") as f:
                f.write(content)

            logger.info("已下载: %s (%d KB)", os.path.basename(save_path), len(content) // 1024)
            return True

        except Exception as e:
            if attempt < max_retries - 1:
                wait = RETRY_BACKOFF_BASE * (2 ** attempt) + random.uniform(0, 5)
                logger.warning(
                    "下载失败 (第%d次): %s, %.1fs 后重试", attempt + 1, e, wait
                )
                time.sleep(wait)
            else:
                logger.error("下载失败，已达最大重试次数: %s", e)

    return False


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="巨潮资讯招股说明书批量下载")
    parser.add_argument(
        "--skip-bse", action="store_true", default=True,
        help="跳过北交所股票 (默认开启)",
    )
    parser.add_argument(
        "--no-skip-bse", action="store_false", dest="skip_bse",
        help="不跳过北交所股票",
    )
    parser.add_argument(
        "--skip-existing", action="store_true", default=True,
        help="跳过已下载的股票 (默认开启)",
    )
    parser.add_argument(
        "--no-skip-existing", action="store_false", dest="skip_existing",
        help="重新下载已存在的文件",
    )
    parser.add_argument("--delay-min", type=int, default=MIN_DELAY, help="最小请求间隔(秒)")
    parser.add_argument("--delay-max", type=int, default=MAX_DELAY, help="最大请求间隔(秒)")
    parser.add_argument("--max-retries", type=int, default=MAX_RETRIES, help="最大重试次数")
    parser.add_argument("--start-from", type=str, default=None, help="从指定股票代码开始")
    parser.add_argument("--limit", type=int, default=None, help="仅处理前N只股票")
    parser.add_argument("--output-dir", type=str, default=OUTPUT_DIR, help="输出目录")
    parser.add_argument("--stock-list", type=str, default=STOCK_LIST_PATH, help="股票列表CSV路径")
    parser.add_argument("--dry-run", action="store_true", help="仅搜索不下载")
    parser.add_argument(
        "--log-level", type=str, default="INFO",
        choices=["DEBUG", "INFO", "WARNING"],
        help="日志级别",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # 日志配置
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 初始化会话
    session = requests.Session()

    # 获取 cninfo 股票代码 -> orgId 映射
    logger.info("正在获取 cninfo 股票列表...")
    cninfo_stock_map = fetch_cninfo_stock_map(session)
    logger.info("获取到 %d 个股票的 orgId 映射", len(cninfo_stock_map))

    # 加载本地股票列表
    stocks = load_stock_list(path=args.stock_list, skip_bse=args.skip_bse)
    logger.info("共加载 %d 只股票", len(stocks))

    # --start-from 过滤
    if args.start_from:
        start_idx = next(
            (i for i, s in enumerate(stocks) if s["symbol"] == args.start_from),
            0,
        )
        stocks = stocks[start_idx:]
        logger.info("从 %s 开始，剩余 %d 只股票", args.start_from, len(stocks))

    # --limit 过滤
    if args.limit:
        stocks = stocks[: args.limit]
        logger.info("限制处理 %d 只股票", args.limit)

    # 创建输出目录
    os.makedirs(args.output_dir, exist_ok=True)

    # 计数器
    total = len(stocks)
    skipped = 0
    downloaded = 0
    not_found = 0
    errors: List[Dict[str, str]] = []
    start_time = time.time()

    logger.info("开始处理 %d 只股票...", total)

    for i, stock in enumerate(stocks, 1):
        ts_code = stock["ts_code"]
        symbol = stock["symbol"]
        name = stock["name"]

        # 跳过已下载
        if args.skip_existing and is_already_downloaded(args.output_dir, ts_code):
            skipped += 1
            if i % PROGRESS_LOG_INTERVAL == 0:
                logger.info("[%d/%d] 跳过 %s %s (已下载)", i, total, symbol, name)
            continue

        # 搜索招股说明书: 策略1 - secid(orgId) 精确搜索
        org_id = cninfo_stock_map.get(symbol)
        announcements: List[Dict] = []
        search_method = "none"

        if org_id:
            announcements = search_by_secid(session, org_id, args.max_retries)
            search_method = "secid"

        # 策略2 - 若 secid 搜索无结果，回退到名称搜索
        if not announcements:
            # 先延迟再发第二次请求
            if org_id:
                time.sleep(random.uniform(args.delay_min, args.delay_max))
            announcements = search_by_name(session, name, args.max_retries)
            search_method = "name"

        best = filter_prospectus(announcements, symbol)

        if best is None:
            not_found += 1
            logger.info(
                "[%d/%d] %s %s - 未找到招股说明书 (搜索方式: %s, 结果数: %d)",
                i, total, symbol, name, search_method, len(announcements),
            )
            errors.append({
                "ts_code": ts_code,
                "symbol": symbol,
                "name": name,
                "error": "not_found",
                "search_method": search_method,
            })
            time.sleep(random.uniform(args.delay_min, args.delay_max))
            continue

        title = best.get("announcementTitle", "")
        adjunct_url = best.get("adjunctUrl", "")
        size_kb = best.get("adjunctSize", 0)

        if args.dry_run:
            logger.info(
                "[%d/%d] %s %s - 找到: %s (%dKB) [%s] [dry-run]",
                i, total, symbol, name, title, size_kb, search_method,
            )
            downloaded += 1
            time.sleep(random.uniform(args.delay_min, args.delay_max))
            continue

        # 下载 PDF
        save_path = os.path.join(args.output_dir, get_save_filename(ts_code))
        success = download_pdf(session, adjunct_url, save_path, args.max_retries)

        if success:
            downloaded += 1
        else:
            errors.append({
                "ts_code": ts_code,
                "symbol": symbol,
                "name": name,
                "error": "download_failed",
                "url": CNINFO_PDF_BASE + adjunct_url,
                "title": title,
            })

        # 请求间延迟
        time.sleep(random.uniform(args.delay_min, args.delay_max))

        # 定期进度日志
        if i % PROGRESS_LOG_INTERVAL == 0:
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            remaining = (total - i) / rate if rate > 0 else 0
            logger.info(
                "进度: %d/%d (已下载:%d 跳过:%d 未找到:%d) "
                "已用时:%.0fs 预计剩余:%.0fs",
                i, total, downloaded, skipped, not_found,
                elapsed, remaining,
            )

    # 最终汇总
    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info("处理完成!")
    logger.info("  总计: %d", total)
    logger.info("  已下载: %d", downloaded)
    logger.info("  跳过(已存在): %d", skipped)
    logger.info("  未找到: %d", not_found)
    logger.info("  下载失败: %d", len(errors) - not_found)
    logger.info("  总用时: %.0fs (%.1f 分钟)", elapsed, elapsed / 60)

    # 保存失败列表
    if errors:
        failure_file = os.path.join(
            args.output_dir,
            f"prospectus_failures_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        )
        with open(failure_file, "w", encoding="utf-8") as f:
            json.dump(errors, f, ensure_ascii=False, indent=2)
        logger.info("失败列表已保存: %s", failure_file)


if __name__ == "__main__":
    main()
