#!/usr/bin/env python3
"""
Tushare 资讯聚合爬虫（优化版 v2）
覆盖 10 个财经快讯平台，共约 8000 条快讯

优化内容：
- 每个数据源独立保存到 data/raw/tushare/{source}.json
- 生成汇总文件 data/raw/tushare/summary.json
- 更精确的Cookie判断（避免误判）
- 使用requests库替代curl（更稳定）
- 添加重试机制（提高成功率）
"""

import requests
import re
import json
import time
import random
import os
import sys
from datetime import datetime
from typing import List, Dict, Optional

# 添加当前目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入配置
# from config import DATA_TUSHARE, TUSHARE_SUMMARY, TUSHARE_COOKIE, REQUEST_DELAY

# Cookie (从环境变量读取，如果未设置则使用默认值)
COOKIE = os.environ.get("TUSHARE_COOKIE", "uid=2|1:0|10:1771858073|3:uid|8:OTQ0MDI5|e4fde30a4396dff5bd6763e03fe672ca1b4d057f0da5dd1fb8e18dea74908aff; username=2|1:0|10:1771858073|8:username|8:bmFtaWtv|2689a69455e3c0607a336ecdbaec2a2059f373c322654de89cc30517250f3b41")

# 请求Headers（模拟真实浏览器）
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Cache-Control': 'max-age=0',
    'Referer': 'https://tushare.pro/',
    'Cookie': COOKIE
}

# 随机User-Agent池（备用）
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]

# 支持的数据源
SOURCES = {
    'xq': '雪球',
    'yicai': '第一财经',
    'fenghuang': '凤凰网',
    '10jqka': '同花顺',
    'jinrongjie': '金融界',
    'sina': '新浪财经',
    'yuncaijing': '云财经',
    'cls': '财联社',
    'eastmoney': '东方财富',
    'wallstreetcn': '华尔街见闻'
}

# 默认抓取的源（所有 10 个数据源）
DEFAULT_SOURCES = [
    'cls',           # 财联社
    'wallstreetcn',  # 华尔街见闻
    'eastmoney',     # 东方财富
    'sina',          # 新浪财经
    '10jqka',        # 同花顺
    'yicai',         # 第一财经
    'fenghuang',     # 凤凰网
    'jinrongjie',     # 金融界
    'yuncaijing',    # 云财经
    'xq'             # 雪球
]

# 输出目录（使用固定路径）
OUTPUT_DIR = '/root/.openclaw/workspace/data/raw/news'
SUMMARY_FILE = '/root/.openclaw/workspace/data/raw/news/summary.json'



def extract_channel_from_html(html: str, news_pos: int) -> Optional[str]:
    """从HTML中提取新闻条目所属的频道
    
    HTML结构示例：
    <div id="news_港美股" class="news_data">
        <div class="news_item">...</div>
    </div>
    
    Args:
        html: 完整的HTML内容
        news_pos: 新闻条目在HTML中的位置
    
    Returns:
        频道名称，如果无法提取则返回 None
    """
    # 方式1: 向上查找包含该新闻的 news_data 容器的 id
    # 查找新闻位置之前所有的 <div id="news_xxx" class="news_data"> 标记
    before_news = html[:news_pos]
    
    # 匹配 <div id="news_xxx" class="news_data"> 或 <div class="news_data" id="news_xxx">
    # 这个id是频道的容器标识
    news_data_pattern = r'<div[^>]*id=["\']news_([^"\']+)["\'][^>]*class=["\'][^"\']*news_data[^"\']*["\']'
    news_data_matches = list(re.finditer(news_data_pattern, before_news))
    
    if news_data_matches:
        # 返回最后一个（最近的）news_data 容器的频道
        return news_data_matches[-1].group(1)
    
    # 也尝试反向顺序的属性：class在前，id在后
    news_data_pattern2 = r'<div[^>]*class=["\'][^"\']*news_data[^"\']*["\'][^>]*id=["\']news_([^"\']+)["\']'
    news_data_matches2 = list(re.finditer(news_data_pattern2, before_news))
    
    if news_data_matches2:
        return news_data_matches2[-1].group(1)
    
    # 方式2: 查找 chan_xxx 格式的频道标记（新浪财经等）
    search_range = html[max(0, news_pos - 500):news_pos + 500]
    chan_id_pattern = r'id=["\']chan_([^"\']+)["\']'
    chan_id_match = re.search(chan_id_pattern, search_range)
    if chan_id_match:
        return chan_id_match.group(1)
    
    # 方式3: 查找 class="channel_name" 的 span 标签内容
    channel_pattern = r'<span class="channel_name[^"]*"[^>]*>([^<]+)</span>'
    channel_matches = list(re.finditer(channel_pattern, before_news))
    
    if channel_matches:
        # 返回最后一个（最近的）频道
        return channel_matches[-1].group(1).strip()
    
    return None


def get_source_news(source: str, retry: int = 2, existing_news: Optional[List[Dict]] = None) -> List[Dict]:
    """获取单个数据源的快讯（返回所有数据，带重试和增量更新支持）
    
    Args:
        source: 数据源标识
        retry: 重试次数，默认2次
        existing_news: 已有新闻列表，用于增量更新和去重。基于 time 和 content 字段判断重复
    
    Returns:
        新闻列表，如果传入了 existing_news 则只返回新增的数据
    """
    url = f'https://tushare.pro/news/{source}'

    for attempt in range(retry + 1):
        try:
            # 随机延迟 3-5 秒，避免频繁请求
            if attempt > 0:
                delay = random.uniform(3, 5)
                time.sleep(delay)

            # 随机选择User-Agent
            headers = HEADERS.copy()
            headers['User-Agent'] = random.choice(USER_AGENTS)

            # 发送请求
            response = requests.get(url, headers=headers, timeout=30)
            html = response.text

            # 🔍 新的Cookie检查逻辑（更精确）
            times = re.findall(r'class="news_datetime"[^>]*>([^<]+)', html)
            contents = re.findall(r'class="news_content"[^>]*>([^<]+)', html)

            # 检查是否有真实的新闻数据
            if not times or not contents:
                # 没有新闻数据，检查是否是登录页面
                if len(html) < 5000 and ('请登录后访问' in html or '需要登录才能查看' in html):
                    return [{'error': 'Cookie失效，需要重新登录'}]
                else:
                    return [{'error': '未能获取数据'}]

            # 🔍 提取 news_day 标签（日期分隔符）
            # 格式: <div class="news_day news_item"><img src="/static/frontend/images/clock.png" />2月8日</div>
            news_day_pattern = r'<div class="news_day[^>]*>.*?(\d{1,2})月(\d{1,2})日.*?</div>'
            news_day_matches = list(re.finditer(news_day_pattern, html))
            
            # 构建 news_day 位置列表，用于确定每个新闻条目的日期
            # 格式: [(position_in_html, month, day), ...]
            news_day_positions = []
            for match in news_day_matches:
                pos = match.start()
                month = int(match.group(1))
                day = int(match.group(2))
                news_day_positions.append((pos, month, day))
            
            # 解析数据
            # 构建已有新闻的集合（用于去重）
            # 使用 (time_raw, content) 作为key，确保与后续去重检查一致
            existing_keys = set()
            if existing_news:
                for news in existing_news:
                    key = (news.get('time_raw', ''), news.get('content', ''))
                    existing_keys.add(key)
            
            # 获取新闻条目在HTML中的位置
            # 使用 finditer 获取位置信息
            time_pattern = r'<div class="news_datetime"[^>]*>([^<]+)</div>'
            time_matches = list(re.finditer(time_pattern, html))
            
            # 遍历所有数据（不再限制数量）
            news_list = []
            current_date = datetime.now()
            
            for i, time_match in enumerate(time_matches):
                time_str = time_match.group(1).strip()
                content_str = contents[i].strip()
                
                # 如果传入了 existing_news，则进行去重检查
                if existing_keys:
                    key = (time_str, content_str)
                    if key in existing_keys:
                        continue  # 跳过已存在的新闻
                
                # 🔍 提取频道信息
                current_pos = time_match.start()
                channel = extract_channel_from_html(html, current_pos)
                
                # 🔍 确定新闻日期（优先级：news_day标签 > 当前日期）
                news_date = current_date.date()  # 默认使用当前日期
                
                # 使用 news_day 标签中的日期
                current_pos = time_match.start()
                relevant_day = None
                for pos, month, day in reversed(news_day_positions):
                    if pos < current_pos:
                        relevant_day = (month, day)
                        break
                
                if relevant_day:
                    # 使用 news_day 标签中的日期
                    month, day = relevant_day
                    year = current_date.year
                    # 如果月份大于当前月份，可能是去年的
                    if month > current_date.month:
                        year -= 1
                    try:
                        news_date = datetime(year, month, day).date()
                    except ValueError:
                        # 日期无效（如2月30日），使用当前日期
                        news_date = current_date.date()
                
                # 解析时间字符串（只包含时间部分，如 "14:43"）
                time_only_match = re.match(r'^(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?$', time_str)
                if time_only_match:
                    hour = int(time_only_match.group(1))
                    minute = int(time_only_match.group(2))
                    second = int(time_only_match.group(3)) if time_only_match.group(3) else 0
                    time_obj = datetime.combine(news_date, datetime.min.time()).replace(hour=hour, minute=minute, second=second)
                else:
                    # 无法解析时间，使用当前时间
                    time_obj = current_date
                
                time_info = {
                    'time_raw': time_str,
                    'time': time_obj.isoformat(),
                    'date': time_obj.strftime('%Y-%m-%d'),
                    # 'time_only': time_obj.strftime('%H:%M:%S')
                }
                
                news_list.append({
                    'time': time_info['time'],           # ISO格式完整日期时间
                    'time_raw': time_info['time_raw'],   # 原始时间字符串
                    'date': time_info['date'],           # 日期部分
                    # 'time_only': time_info['time_only'], # 时间部分
                    'content': content_str,
                    'source': SOURCES.get(source, source),
                    # 'source_key': source,
                    'channel': channel                   # 所属频道
                })

            return news_list

        except requests.Timeout:
            if attempt < retry:
                print(f"  ⏱️ 超时，重试 {attempt + 1}/{retry}...")
                continue
            return [{'error': f'请求超时（{retry}次重试后）'}]

        except Exception as e:
            if attempt < retry:
                print(f"  ❌ 错误，重试 {attempt + 1}/{retry}...: {e}")
                continue
            return [{'error': str(e)}]

    return [{'error': '未知错误'}]


def load_existing_news() -> List[Dict]:
    """从按日期分组的文件中读取已有的新闻数据
    
    读取格式: data/raw/news/news_*.json
    
    Returns:
        已有新闻列表，如果文件不存在或读取失败则返回空列表
    """
    all_news = []
    
    if os.path.exists(OUTPUT_DIR) and os.path.isdir(OUTPUT_DIR):
        try:
            for filename in os.listdir(OUTPUT_DIR):
                if filename.startswith('news_') and filename.endswith('.json'):
                    # 跳过汇总文件和特殊文件
                    if filename.startswith('_') or filename == 'new_news.json':
                        continue
                    file_path = os.path.join(OUTPUT_DIR, filename)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            news = data.get('news', [])
                            all_news.extend(news)
                    except Exception as e:
                        print(f"  ⚠️ 读取日期文件 {filename} 失败: {e}")
            
            if all_news:
                # 按时间排序（最新的在前）
                all_news.sort(key=lambda x: x.get('time', ''), reverse=True)
                return all_news
        except Exception as e:
            print(f"  ⚠️ 读取日期目录失败: {e}")
    
    return []


def get_all_sources_news(sources: List[str] = None, incremental: bool = False, save: bool = False) -> Dict:
    """获取多个数据源的快讯（支持增量更新和自动保存）
    
    Args:
        sources: 数据源列表，默认为所有数据源
        incremental: 是否增量更新模式，默认 False
            - False: 返回所有数据
            - True: 只返回新增的数据
        save: 是否自动保存数据，默认 False
            - False: 只返回数据，不保存
            - True: 自动保存数据到指定目录（按日期统一保存）
    
    Returns:
        包含各数据源新闻和统计信息的字典
    """
    if sources is None:
        sources = list(SOURCES.keys())

    # 增量模式：加载所有已有数据（用于去重）
    existing_news = None
    if incremental:
        existing_news = load_existing_news()

    results = {}
    total = 0
    total_new = 0
    all_new_news = []  # 收集所有新增新闻

    for source in sources:
        name = SOURCES.get(source, source)
        
        news = get_source_news(source, existing_news=existing_news)

        # 检查是否有错误（news 为空或包含错误信息）
        has_error = news and len(news) > 0 and 'error' in news[0]
        
        if not has_error:
            # 成功获取数据（可能为空列表，表示没有新增数据）
            new_count = len(news)
            total += new_count
            total_new += new_count
            
            results[source] = {
                'name': name,
                'count': new_count,
                'news': news
            }
            
            # 增量模式：添加 new_count 字段
            if incremental:
                results[source]['new_count'] = new_count
            
            # 收集新增新闻
            all_new_news.extend(news)
        else:
            # 获取失败
            results[source] = {
                'name': name,
                'count': 0,
                'error': news[0].get('error', '未知错误')
            }
            # 增量模式：添加 new_count 字段（失败时为0）
            if incremental:
                results[source]['new_count'] = 0

    return_dict = {
        'sources': results,
        'total_news': total,
        'working_sources': sum(1 for v in results.values() if v.get('count', 0) > 0)
    }
    
    # 增量模式：添加 total_new_news 和 incremental 标识
    if incremental:
        return_dict['total_new_news'] = total_new
        return_dict['incremental'] = True
    
    # 自动保存功能
    if save:
        # 保存所有新增新闻（按日期统一保存）
        if all_new_news:
            if incremental and existing_news:
                # 增量模式：合并已有新闻和新增新闻后保存
                combined_news = existing_news + all_new_news
                save_news_by_date(combined_news)
                print(f"    💾 原有 {len(existing_news)} 条，新增 {total_new} 条，总计 {len(combined_news)} 条已保存（按日期统一保存）")
            else:
                # 非增量模式或没有已有数据，直接保存
                save_news_by_date(all_new_news)
                print(f"    💾 {total_new} 条已保存（按日期统一保存）")
        
        # 保存汇总信息
        summary_data = {
            'crawl_time': datetime.now().isoformat(),
            'total_sources': len(sources),
            'working_sources': return_dict['working_sources'],
            'total_news': total,
            'sources': results
        }
        
        # 增量模式：添加 total_new_news 和 incremental 标识
        if incremental:
            summary_data['total_new_news'] = total_new
            summary_data['incremental'] = True
        
        save_summary(summary_data)
        print(f"    📊 汇总信息已保存到 {SUMMARY_FILE}")
        
        # 保存新增新闻到 new_news.json（用于快速查看最新新闻）
        # 没有新增新闻，则保存空json
        if incremental and total_new > 0:
            save_new_news(all_new_news)
            print(f"    📰 新增新闻已保存到 {os.path.join(OUTPUT_DIR, 'new_news.json')}")
        else:
            save_new_news([])
            print(f"    📰 没有新增新闻，已保存空文件到 {os.path.join(OUTPUT_DIR, 'new_news.json')}")
    
    return return_dict


def save_news_by_date(all_news: List[Dict]):
    """按日期统一保存所有新闻
    
    保存格式: data/raw/news/news_{YYYY-MM-DD}.json
    每个文件包含所有数据源在该日期的新闻
    
    Args:
        all_news: 所有新闻的列表
    """
    # 按日期分组
    date_groups = {}
    for news in all_news:
        date = news.get('date', datetime.now().strftime('%Y-%m-%d'))
        if date not in date_groups:
            date_groups[date] = []
        date_groups[date].append(news)
    
    # 为每个日期创建文件
    for date, news_list in date_groups.items():
        # 统计各数据源的新闻数量
        source_counts = {}
        for news in news_list:
            source = news.get('source', '未知')
            source_counts[source] = source_counts.get(source, 0) + 1
        
        output = {
            'date': date,
            'crawl_time': datetime.now().isoformat(),
            'news_count': len(news_list),
            'sources': source_counts,
            'news': sorted(news_list, key=lambda x: x.get('time', ''), reverse=True)
        }
        
        output_path = os.path.join(OUTPUT_DIR, f'news_{date}.json')
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)


def save_new_news(all_new_news: List[Dict]):
    """保存所有新增新闻到统一文件
    
    Args:
        all_new_news: 所有新增新闻的列表
    """
    if not all_new_news:
        return
    
    # 按时间排序（最新的在前）
    sorted_news = sorted(all_new_news, key=lambda x: x.get('time', ''), reverse=True)
    
    output = {
        'crawl_time': datetime.now().isoformat(),
        'news_count': len(sorted_news),
        'news': sorted_news
    }
    
    output_path = os.path.join(OUTPUT_DIR, 'new_news.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)


def save_summary(all_data: Dict):
    """保存汇总信息"""
    with open(SUMMARY_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)


def main():
    """主函数"""
    print("=" * 70)
    print(f"📰 Tushare 资讯聚合爬虫 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    # print("⏱️  反爬策略: 每次请求间隔 3-5 秒")
    print("📁 数据保存位置: /root/.openclaw/workspace/data/raw/news/")
    print("🔄 增量更新模式：只保存新增数据")
    print("📂 保存模式：按日期统一保存（news_{YYYY-MM-DD}.json）")
    print()

    # 创建输出目录
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 获取并保存数据（增量模式）
    print(f"🔄 获取 {len(DEFAULT_SOURCES)} 个数据源的快讯...")
    print()
    
    result = get_all_sources_news(sources=DEFAULT_SOURCES, incremental=True, save=True)
    
    # 显示汇总信息
    total_news = result.get('total_news', 0)
    total_new = result.get('total_new_news', 0)
    working = result.get('working_sources', 0)
    
    print(f"✅ 正常工作: {working}/{len(DEFAULT_SOURCES)} 个数据源")
    print(f"📰 总快讯数: {total_news} 条")
    if total_new > 0:
        print(f"🆕 本次新增: {total_new} 条")
    
    print()
    print("=" * 70)
    print(f"✅ 数据获取完成！")
    print(f"📁 数据保存位置: {OUTPUT_DIR}/")
    print(f"📊 汇总文件: {SUMMARY_FILE}")
    print()


if __name__ == "__main__":
    main()
