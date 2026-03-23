#!/usr/bin/env python3
"""
深交所互动易 (irm.cninfo.com.cn) 爬虫 - 已回复数据
使用 Playwright 渲染页面并提取数据
功能：爬取 https://irm.cninfo.com.cn/views/interactiveAnswer 中的"已回复"数据
反爬措施：
1. 随机User-Agent轮换
2. 请求间隔延时
3. 模拟真实浏览器行为
4. 慢速等待页面渲染
"""

import asyncio
import json
import os
import random
import re
import time
from datetime import datetime
from typing import List, Dict, Optional

from playwright.async_api import async_playwright, Page, Browser

# 输出目录
OUTPUT_DIR = '/root/.openclaw/workspace/data/raw/irm_qa'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 随机User-Agent池
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]


async def create_browser_context() -> tuple:
    """创建浏览器上下文"""
    playwright = await async_playwright().start()

    browser = await playwright.chromium.launch(
        headless=True,
        args=['--disable-blink-features=AutomationControlled']
    )

    context = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        viewport={'width': 1920, 'height': 1080},
        locale='zh-CN',
        timezone_id='Asia/Shanghai'
    )

    await context.set_extra_http_headers({
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    })

    return playwright, browser, context


def clean_qa_item(item: Dict) -> Optional[Dict]:
    """清理和验证单条问答数据"""
    # 过滤太短的问题或回答
    if len(item.get("question", "")) < 5:
        return None
    if len(item.get("answer", "")) < 2:
        return None

    # 清理数据
    question = item.get("question", "")
    answer = item.get("answer", "")

    # 移除多余空白
    question = re.sub(r'\s+', ' ', question).strip()
    answer = re.sub(r'\s+', ' ', answer).strip()

    # 移除"·"等干扰字符
    question = re.sub(r'^[\s·]+', '', question)
    answer = re.sub(r'[\s·]+$', '', answer)

    # 提取股票代码和公司名
    stock_match = re.search(r'\[(\d{6})\]([^\u00A5\uFF00·\n\d]+)', question)
    if stock_match:
        item["stock_code"] = stock_match.group(1)
        item["company"] = stock_match.group(2).strip()

    # 提取时间
    time_match = re.search(r'[·\u00B7](\d+(?:小时|分钟)?前|\d{4}-\d{2}-\d{2})', question)
    if time_match:
        item["time"] = time_match.group(1)

    # 提取来源
    source_match = re.search(r'来源\s*(App|网站|公众号)', question + answer)
    if source_match:
        item["source"] = source_match.group(1)

    item["question"] = question
    item["answer"] = answer

    return item


async def navigate_and_get_data(page: Page) -> Dict:
    """导航到页面并获取数据"""
    base_url = "https://irm.cninfo.com.cn/views/interactiveAnswer"

    print("正在导航到互动易页面...")

    await page.goto(base_url, wait_until='networkidle', timeout=60000)
    await asyncio.sleep(3)

    title = await page.title()
    print(f"页面标题: {title}")

    # 查找并点击"已回复"标签
    answered_tab = await page.query_selector('text=已回复')
    if answered_tab:
        try:
            await answered_tab.click()
            print("点击了已回复标签")
            await asyncio.sleep(3)
        except Exception as e:
            print(f"点击已回复标签失败: {e}")

    content = await page.content()
    print(f"页面内容长度: {len(content)} 字符")

    data = await extract_qa_data(page)

    return {
        "page_title": title,
        "data": data,
        "html_length": len(content)
    }


async def extract_qa_data(page: Page) -> List[Dict]:
    """从页面提取问答数据"""
    qa_list = []

    try:
        js_data = await page.evaluate('''
            () => {
                const data = [];
                const elements = document.querySelectorAll('div, li');

                for (const el of elements) {
                    const text = el.innerText || '';

                    if (text.match(/\\[\\d{6}\\]/) &&
                        (text.includes('？') || text.includes('?')) &&
                        text.includes('：')) {

                        if (text.length > 30 && text.length < 3000) {
                            data.push(text);
                        }
                    }
                }
                return data;
            }
        ''')

        print(f"通过JS找到 {len(js_data)} 个问答元素")

        for text in js_data:
            parsed = parse_single_qa(text)
            if parsed:
                qa_list.append(parsed)

    except Exception as e:
        print(f"JS提取失败: {e}")

    # 清理和去重
    cleaned_qa = []
    seen = set()

    for item in qa_list:
        cleaned = clean_qa_item(item)
        if cleaned:
            # 基于问题内容去重
            key = cleaned.get("question", "")[:50]
            if key and key not in seen:
                seen.add(key)
                cleaned_qa.append(cleaned)

    return cleaned_qa


def parse_single_qa(text: str) -> Optional[Dict]:
    """解析单条问答文本"""
    result = {
        "company": "",
        "stock_code": "",
        "time": "",
        "question": "",
        "answer": "",
        "source": ""
    }

    if not text or not text.strip():
        return None

    # 清理干扰文字
    text = re.sub(r'\.\.\.显示全部', '', text)
    text = re.sub(r'点赞\s*\d*', '', text)
    text = re.sub(r'irm\d+', '', text)

    # 提取股票代码
    stock_match = re.search(r'\[(\d{6})\]([^\u00A5\uFF00·\n]+?)(?=[·\u00B7]|$)', text)
    if stock_match:
        result["stock_code"] = stock_match.group(1)
        result["company"] = stock_match.group(2).strip()

    # 提取时间
    time_match = re.search(r'[·\u00B7](\d+(?:小时|分钟)?前|\d{4}-\d{2}-\d{2})', text)
    if time_match:
        result["time"] = time_match.group(1)

    # 提取来源
    source_match = re.search(r'来源\s*(App|网站|公众号)', text)
    if source_match:
        result["source"] = source_match.group(1)

    # 清理并分离问答
    clean_text = re.sub(r'^\[[^\]]+\][^\u00B7·]*[·\u00B7]\d+[^\n]*', '', text)
    clean_text = re.sub(r'来源\s*(App|网站|公众号)', '', clean_text)
    clean_text = clean_text.strip()

    # 查找回答分隔点 - 最后一个"xxx："的模式
    answer_pattern = r'([^：\n]{2,20})：(.*)'
    answer_matches = list(re.finditer(answer_pattern, clean_text))

    if answer_matches:
        last_match = answer_matches[-1]
        answer_part = last_match.group(2).strip()
        question_part = clean_text[:last_match.start()].strip()

        result["answer"] = answer_part
        result["question"] = question_part
    else:
        # 备用方法：查找问号位置
        question_mark_pos = clean_text.rfind('？')
        if question_mark_pos == -1:
            question_mark_pos = clean_text.rfind('?')

        if question_mark_pos > 0:
            result["question"] = clean_text[:question_mark_pos+1].strip()
            result["answer"] = clean_text[question_mark_pos+1:].strip()
        else:
            result["question"] = clean_text

    return result if (result.get("question") or result.get("answer")) else None


async def main():
    """主函数"""
    print("=" * 70)
    print(f"深交所互动易 爬虫 (Playwright版) | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()

    result = None

    # 使用Playwright渲染页面
    print("使用Playwright渲染页面...")

    try:
        playwright, browser, context = await create_browser_context()
        page = await context.new_page()

        result = await navigate_and_get_data(page)

        await browser.close()
        await playwright.stop()

        print("  页面渲染完成")

    except Exception as e:
        print(f"  Playwright错误: {e}")
        result = {"error": str(e)}

    # 保存结果
    print("\n保存数据...")

    save_data = {
        "crawl_time": datetime.now().isoformat(),
        "source": "irm.cninfo.com.cn",
        "page": "interactiveAnswer",
        "type": "已回复",
    }

    if "data" in result:
        save_data["qa_count"] = len(result.get("data", []))
        save_data["qa_list"] = result.get("data", [])
    else:
        save_data.update(result)

    filename = f"irm_qa_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(save_data, f, ensure_ascii=False, indent=2)

    print(f"  数据已保存到: {filepath}")

    # 打印摘要
    print()
    print("=" * 70)
    print("摘要:")
    if "qa_count" in save_data:
        print(f"  获取到 {save_data['qa_count']} 条已回复问答")
        if save_data.get("qa_list"):
            print("\n  前3条示例:")
            for i, qa in enumerate(save_data["qa_list"][:3]):
                print(f"\n  [{i+1}] {qa.get('company', '')}[{qa.get('stock_code', '')}] {qa.get('time', '')}")
                q = qa.get('question', '')[:50]
                a = qa.get('answer', '')[:50]
                print(f"      问题: {q}{'...' if len(qa.get('question','')) > 50 else ''}")
                print(f"      回答: {a}{'...' if len(qa.get('answer','')) > 50 else ''}")
    elif "error" in result:
        print(f"  错误: {result['error']}")
    print()
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
