"""
TGB 跟帖图片批量下载器
=======================
从淘股吧指定用户的跟帖中，提取并下载所有含图片跟帖的高清原图。

用法:
    python3 tgb_reply_image_scraper.py

配置:
    修改下方 CONFIG 区域的参数即可自定义目标用户、时间范围等。

输出:
    图片保存到 data/raw/tgb_images/ 目录，文件名格式：
    {日期}_{时间}_{跟帖ID}_{序号}.png

技术要点:
    - 跟帖列表API: /user/blog/moreReplyMod?userID={}&pageNo={}
    - 每条跟帖独立页面: /a/{slug}/{reply_id}#{reply_id}
    - 图片在HTML中: <div id="reply{rid}"> 内的 src2 属性 (_max.png 为原图)
    - 不需要 Playwright，纯 requests 即可

作者: 小八
创建: 2026-04-19
"""

import requests
import re
import html
import os
import time
import glob
import json
from collections import Counter
from datetime import datetime

# ======================================================================
# CONFIG - 修改这里的参数
# ======================================================================
CONFIG = {
    # 目标用户
    "user_id": "308803",           # 淘股吧数字ID（URL中的userID）
    "user_name": "股海贼王",        # 显示名，仅用于日志
    "self_account": "11929265",    # 你自己的账号ID（cookies中的tgbuser）

    # Cookies - 从浏览器开发者工具复制，注意有效期
    "cookies": "_c_WBKFRo=wfH3Pp2WH4Mf00kVGDl0QgOBSVKJbv9XYelXuqNN; acw_tc=7929ee2c17766075119875097e0aaa816c1203cde8fec7adc368e0ffe4fc04; JSESSIONID=NGRmMDEzY2ItMTkwZC00NWI2LWIwOTAtZTM3NjU4YzViZmQ5; gdp_user_id=gioenc-571cd5b1%2Ce039%2C5dd9%2Cac33%2C083b419dg487; JSESSIONID=ZTkzYjNkM2YtNjliZi00ZjVjLThjY2MtOTVmNTdiYTI2NTBh; agree=enter; tgbuser=11929265; tgbpwd=c3910733cece2b75659c67488288398a507a60ac7a1620a85cc0804f1e4e45c3n04rgnsslznb5th; loginStatus=qrcode; 893eedf422617c96_gdp_gio_id=gioenc-00838374; 893eedf422617c96_gdp_session_id=58d9b410-bfe5-4064-9307-1119a08b13bf; 893eedf422617c96_gdp_cs1=gioenc-00838394; creatorStatus11929265=true; 893eedf422617c96_gdp_sequence_ids=%7B%22globalKey%22%3A19%2C%22VISIT%22%3A4%2C%22PAGE%22%3A8%2C%22CUSTOM%22%3A9%7D; 893eedf422617c96_gdp_session_id_58d9b410-bfe5-4064-9307-1119a08b13bf=true",

    # 过滤
    "year": "2026",                # 只抓这个年份之后的跟帖
    "blog_slug": None,             # None=自动从列表页获取第一个博客slug; 或指定如 "1ykAFNDujlw"

    # 输出（基础目录，脚本会自动在下面按用户名建子目录）
    "output_base": "/root/.openclaw/workspace/data/raw/tgb_images",

    # 网络控制
    "delay_list_page": 0.3,        # 列表页请求间隔(秒)
    "delay_reply_page": 0.3,       # 详情页请求间隔(秒)
    "delay_download": 0.1,         # 图片下载间隔(秒)
    "timeout": 15,                 # 请求超时(秒)

    # 行为
    "clean_existing": True,        # True=开始前清空输出目录已有文件
    "save_metadata": True,         # 是否保存元数据JSON
}

# ======================================================================
# 核心逻辑
# ======================================================================

def parse_cookies(cookie_str):
    """解析cookie字符串为字典"""
    cookies = {}
    for item in cookie_str.split('; '):
        if '=' in item:
            key, val = item.split('=', 1)
            cookies[key.strip()] = val.strip()
    return cookies


def get_headers():
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        'Referer': 'https://tgb.cn/',
    }


def collect_image_posts(session, cfg):
    """Step 1: 从跟帖列表页收集含图片的跟帖"""
    print(f"📋 Step 1: 扫描 {cfg['user_name']} (ID:{cfg['user_id']}) 的跟帖列表...")

    all_posts = []
    pg = 1
    blog_slug = cfg.get("blog_slug")

    while True:
        url = f"https://tgb.cn/user/blog/moreReplyMod?userID={cfg['user_id']}&pageNo={pg}"
        resp = session.get(url, timeout=cfg['timeout'])

        # 提取跟帖链接、日期、内容
        reply_ids = re.findall(r"href='a/([^/]+)/(\d+)#\d+'", resp.text)
        dates_raw = re.findall(r'class="blogReply-date"[^>]*>(.*?)</span>', resp.text, re.DOTALL)
        dates = [re.sub(r'<[^>]+>', '', html.unescape(d)).strip() for d in dates_raw]
        tops = re.findall(r'class="blogReply-top"[^>]*>(.*?)</div>', resp.text, re.DOTALL)

        if not dates:
            break

        # 自动获取blog_slug（取第一个跟帖的slug）
        if blog_slug is None and reply_ids:
            blog_slug = reply_ids[0][0]
            print(f"  🔗 自动检测到 blog slug: {blog_slug}")

        for i in range(min(len(dates), len(tops), len(reply_ids))):
            content = html.unescape(re.sub(r'<[^>]+>', '', tops[i])).strip()
            date = dates[i]
            slug, reply_id = reply_ids[i]

            if date < cfg['year']:
                continue

            if '［图片］' in content:
                all_posts.append({
                    'date': date,
                    'content': content,
                    'slug': slug,
                    'reply_id': reply_id,
                    'img_count': content.count('［图片］'),
                })

        print(f"  Page {pg}: {dates[0]} ~ {dates[-1]} | 累计图片帖: {len(all_posts)}")

        if dates[-1] < cfg['year']:
            break
        pg += 1
        time.sleep(cfg['delay_list_page'])

    print(f"\n  ✅ 共找到 {len(all_posts)} 条含图片跟帖")
    return all_posts, blog_slug


def extract_images_from_reply(session, post, blog_slug, cfg):
    """Step 2: 从单条跟帖页面提取高清图片URL"""
    rid = post['reply_id']
    reply_url = f"https://tgb.cn/a/{blog_slug}/{rid}#{rid}"

    resp = session.get(reply_url, timeout=cfg['timeout'])
    page = resp.text

    # 定位 <div id="reply{rid}"> 内容区域
    reply_marker = f'id="reply{rid}"'
    pos = page.find(reply_marker)

    if pos == -1:
        # 备选：通过 pcDetail 链接定位
        alt_marker = f"pcDetail({rid}, 'R')"
        alt_pos = page.find(alt_marker)
        if alt_pos > 0:
            pos = page.rfind('comment-data-text', 0, alt_pos)

    if pos == -1:
        return [], f"reply div not found in page ({len(page)} chars)"

    # 截取到下一个评论区域
    next_reply = page.find('comment-data-text', pos + 100)
    if next_reply == -1:
        next_reply = pos + 10000
    chunk = page[pos:next_reply]

    # 提取 src2 中的高清图片URL（_max.png 为原图）
    img_urls = re.findall(
        r"data-type='contentImage'[^>]*src2=\"(https?://image\.tgb\.cn/[^\"]+)\"", chunk
    )
    if not img_urls:
        img_urls = re.findall(
            r"src2=\"(https?://image\.tgb\.cn/[^\"]+)\"[^>]*data-type='contentImage'", chunk
        )

    # 兜底：chunk中所有非头像的src2图片
    if not img_urls:
        img_urls = [u for u in re.findall(r'src2="(https?://image\.tgb\.cn/[^"]+)"', chunk)
                    if '_60wh.' not in u]

    return img_urls, None


def download_images(session, downloads, cfg):
    """Step 3: 批量下载图片"""
    print(f"\n📥 Step 3: 下载 {len(downloads)} 张图片...")

    success = 0
    failed = 0

    for i, dl in enumerate(downloads):
        try:
            img_resp = session.get(dl['url'], timeout=30)
            if img_resp.status_code == 200 and len(img_resp.content) > 500:
                with open(dl['filepath'], 'wb') as f:
                    f.write(img_resp.content)
                success += 1
                if (i + 1) % 20 == 0:
                    print(f"  进度: {i+1}/{len(downloads)} ({success} ok, {failed} fail)")
            else:
                failed += 1
                print(f"  ❌ HTTP {img_resp.status_code} size={len(img_resp.content)}: {dl['url'][:80]}")
        except Exception as e:
            failed += 1
            print(f"  ❌ {e}")
        time.sleep(cfg['delay_download'])

    return success, failed


def run():
    """主流程"""
    cfg = CONFIG
    start_time = time.time()

    # 初始化 — 按用户名建子目录: tgb_images/股海贼王/
    output_dir = os.path.join(cfg['output_base'], cfg['user_name'])
    os.makedirs(output_dir, exist_ok=True)

    if cfg['clean_existing']:
        for f in glob.glob(os.path.join(output_dir, "*")):
            os.remove(f)
        print(f"🗑️ 已清空输出目录: {output_dir}")

    cookies = parse_cookies(cfg['cookies'])
    session = requests.Session()
    session.cookies.update(cookies)
    session.headers.update(get_headers())

    # Step 1: 收集含图片的跟帖
    all_posts, blog_slug = collect_image_posts(session, cfg)
    if not all_posts:
        print("❌ 没有找到含图片的跟帖")
        return

    # Step 2: 提取图片URL
    print(f"\n🔍 Step 2: 从 {len(all_posts)} 条跟帖页面提取图片...")
    all_downloads = []
    errors = []

    for idx, post in enumerate(all_posts):
        img_urls, err = extract_images_from_reply(session, post, blog_slug, cfg)

        if err:
            errors.append(f"{post['reply_id']}: {err}")
            print(f"  [{idx+1}/{len(all_posts)}] ❌ {post['date']} rid={post['reply_id']} - {err}")
            continue

        if img_urls:
            for j, img_url in enumerate(img_urls):
                safe_date = post['date'].replace(':', '').replace(' ', '_')
                if '_max.' in img_url:
                    ext = '.' + img_url.split('_max.')[-1].split('?')[0]
                elif '_760w.' in img_url:
                    ext = '.' + img_url.split('_760w.')[-1].split('?')[0]
                else:
                    ext = '.png'
                filename = f"{safe_date}_{post['reply_id']}_{j+1}{ext}"
                filepath = os.path.join(output_dir, filename)
                all_downloads.append({
                    'url': img_url,
                    'filepath': filepath,
                    'date': post['date'],
                    'reply_id': post['reply_id'],
                })
            print(f"  [{idx+1}/{len(all_posts)}] ✅ {post['date']} rid={post['reply_id']} -> {len(img_urls)} imgs")
        else:
            errors.append(f"{post['reply_id']}: 0 images found")
            print(f"  [{idx+1}/{len(all_posts)}] ⚠️ {post['date']} rid={post['reply_id']} -> 0 imgs")

        time.sleep(cfg['delay_reply_page'])

    # Step 3: 下载
    success, failed = download_images(session, all_downloads, cfg)

    # 统计
    elapsed = time.time() - start_time
    files = sorted(os.listdir(output_dir))
    total_size = sum(os.path.getsize(os.path.join(output_dir, f)) for f in files) / (1024 * 1024)

    # 按日期统计
    date_counts = Counter()
    for f in files:
        date_counts[f[:10]] += 1

    # 汇总
    print(f"\n{'=' * 60}")
    print(f"🎯 完成!")
    print(f"  用户: {cfg['user_name']} (ID:{cfg['user_id']})")
    print(f"  时间范围: {cfg['year']} 至今")
    print(f"  含图跟帖: {len(all_posts)} 条")
    print(f"  图片总数: {len(all_downloads)} 张")
    print(f"  下载成功: {success} | 失败: {failed}")
    print(f"  总文件数: {len(files)}")
    print(f"  总大小: {total_size:.1f} MB")
    print(f"  耗时: {elapsed:.0f}s")
    print(f"  输出: {output_dir}")

    if errors:
        print(f"\n  ⚠️ 错误: {len(errors)}")
        for e in errors[:10]:
            print(f"    {e}")

    print(f"\n  📅 按日期分布:")
    for d in sorted(date_counts.keys()):
        print(f"    {d}: {date_counts[d]} 张")

    # 保存元数据
    if cfg['save_metadata']:
        metadata = {
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "target_user": cfg['user_name'],
            "target_user_id": cfg['user_id'],
            "year_filter": cfg['year'],
            "blog_slug": blog_slug,
            "total_posts": len(all_posts),
            "total_images": len(all_downloads),
            "downloaded": success,
            "failed": failed,
            "total_size_mb": round(total_size, 1),
            "elapsed_seconds": round(elapsed),
            "date_distribution": dict(sorted(date_counts.items())),
            "files": files,
            "errors": errors,
        }
        meta_path = os.path.join(output_dir, "_metadata.json")
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        print(f"\n  💾 元数据已保存: {meta_path}")


if __name__ == "__main__":
    run()
