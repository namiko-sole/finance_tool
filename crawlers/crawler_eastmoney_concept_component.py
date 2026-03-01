import json
import re
import time
from typing import List, Dict, Any

# 使用 curl_cffi 的 requests 接口（用法与标准库 requests 类似）
from curl_cffi import requests

# 常量：东财成分股接口（JSONP）
BASE_URL = "https://push2.eastmoney.com/api/qt/clist/get"

def fetch_stocks_by_bk(
    bk_code: str,
    page_size: int = 500,   # pz：每页条数，通常拉大一点一次取完
    impersonate: str = "chrome",
    timeout: int = 10,
) -> List[Dict[str, Any]]:
    """
    获取东方财富某个概念板块（BK 编号）的成分股列表

    Args:
        bk_code: 概念板块编号，如 "BK0655"
        page_size: 单页返回数量（pz 参数）
        impersonate: curl_cffi 伪装浏览器类型，默认 "chrome"
        timeout: 请求超时（秒）

    Returns:
        包含成分股信息的字典列表，每个字典至少包含：
            - "code": 股票代码（如 "600519"）
            - "name": 股票名称
            以及接口返回的其它字段（价格、涨跌幅等），全部扁平化在字典中。
    """
    # 构造查询参数
    # 关键参数：
    #   fs=b:BK{bk_code}  ← 指定概念板块
    #   pz=page_size      ← 每页条数
    #   pn=1              ← 当前页码（概念股通常一页能拉完）
    #   fid=f62           ← 常用于成分股排序依据（跟官网一致）
    #   fields            ← 需要返回的字段列表（可根据需要删减）
    #   cb 与 _           ← JSONP 回调名和时间戳，可随机生成，这里简化处理
    params = {
        "fid": "f62",
        "po": "1",
        "pz": str(page_size),
        "pn": "1",
        "np": "1",
        "fltt": "2",
        "invt": "2",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "fs": f"b:BK{bk_code}",
        # 常用字段：代码、名称、最新价、涨跌幅、涨跌额、成交量、成交额、
        # 振幅、换手率、市盈率、量比、最高、最低、今开、昨收、市净率等
        "fields": (
            "f12,f14,f2,f3,f4,f5,f6,f7,f8,f9,f10,f15,f16,f17,f18,f22,"
            "f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205,f124,f1,f13"
        ),
    }

    # 简单的随机 JSONP 回调和时间戳，避免缓存
    cb = f"jQuery{int(time.time() * 1000)}_{int(time.time() * 1000)}"
    params["cb"] = cb
    params["_"] = str(int(time.time() * 1000))

    # 请求头（通常 curl_cffi 会在 impersonate 时自动补上 User-Agent，
    # 但你也可以手动设置，尤其是 Referer，降低被拒概率）
    headers = {
        "Referer": "https://quote.eastmoney.com/center/gridlist.html",
    }

    try:
        resp = requests.get(
            BASE_URL,
            params=params,
            headers=headers,
            impersonate=impersonate,
            timeout=timeout,
        )
        resp.raise_for_status()
        text = resp.text
    except Exception as e:
        print(f"[fetch_stocks_by_bk] 请求异常: {e}")
        return []

    # 去掉 JSONP 包裹，拿到纯 JSON
    # 返回示例：jQuery123_456({...});
    pattern = rf"{re.escape(cb)}\((.+?)\);?"
    m = re.search(pattern, text, flags=re.DOTALL)
    if not m:
        print("[fetch_stocks_by_bk] 未找到 JSONP 数据包，可能接口或响应格式已变化")
        # 调试时可把响应打印出来看看
        # print(text)
        return []

    try:
        data = json.loads(m.group(1))
    except Exception as e:
        print(f"[fetch_stocks_by_bk] JSON 解析失败: {e}")
        return []

    # 成分股在 data["data"]["diff"] 里
    diff = data.get("data", {}).get("diff", [])
    if not diff:
        print(f"[fetch_stocks_by_bk] 概念 BK{bk_code} 未返回成分股数据（可能为空或 bk_code 不存在）")
        return []

    # 将每条记录扁平化，并至少映射出 code/name
    stocks = []
    for item in diff:
        # 这里把 f12 映射为 code，f14 映射为 name，其它字段直接保留
        record = {
            "code": item.get("f12"),
            "name": item.get("f14"),
        }
        # 把其它 f 开头的字段也平铺进去，方便后续使用
        for k, v in item.items():
            if k not in ("f12", "f14"):
                record[k] = v
        stocks.append(record)

    return stocks


def demo_single():
    """演示：获取单个概念的成分股（BK0655）"""
    bk = "BK0655"  # 你在原链接中看到的概念编号
    stocks = fetch_stocks_by_bk(bk)
    print(f"概念 BK{bk} 成分股数量: {len(stocks)}")
    for s in stocks[:10]:  # 只打印前 10 条
        print(s)


def demo_multiple():
    """演示：支持输入多个不同概念，批量获取成分股"""
    # 这里只是示例：你可以改成从命令行参数、配置文件或数据库中读取
    concepts = {
        "BK0655": "示例概念A",
        "BK0883": "数字货币",      # 可从东财网页 URL 或接口 list 中获取
        "BK0917": "半导体概念",
    }

    for bk, name in concepts.items():
        print(f"\n正在获取概念：{name} (BK{bk})")
        stocks = fetch_stocks_by_bk(bk)
        print(f"  共 {len(stocks)} 只成分股，前 5 只：")
        for s in stocks[:5]:
            print(f"    {s.get('code')} {s.get('name')}")


if __name__ == "__main__":
    # 运行单个概念示例
    demo_single()

    # 若要批量不同概念，可以取消下面这行的注释：
    # demo_multiple()
