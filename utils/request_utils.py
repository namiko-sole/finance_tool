#!/usr/bin/env python3
"""
请求工具模块（反爬友好）
"""

import time
import random
import threading
from typing import Callable, Any, Optional


class RequestManager:
    """请求管理器"""

    def __init__(self):
        """初始化请求管理器"""
        self.min_interval = {
            'tushare': 0.5,
            'eastmoney': 2.0,
            'tencent': 1.0,
            'akshare': 1.5,
            'default': 1.0
        }

        self.last_request_time = {}
        self.max_retries = 3
        self.retry_delays = [1, 2, 5]

        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15',
        ]

    def _wait_if_needed(self, source: str = 'default') -> None:
        """根据请求频率等待"""
        interval = self.min_interval.get(source, self.min_interval['default'])
        last_time = self.last_request_time.get(source)

        if last_time:
            elapsed = time.time() - last_time
            if elapsed < interval:
                wait_time = interval - elapsed
                jitter = wait_time * 0.2 * (random.random() * 2 - 1)
                time.sleep(max(0, wait_time + jitter))

        self.last_request_time[source] = time.time()

    def get_headers(self, source: str = 'default') -> dict:
        """获取请求头"""
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive'
        }

    def request_with_retry(self, func: Callable, source: str = 'default',
                          *args, **kwargs) -> Optional[Any]:
        """带重试的请求"""
        self._wait_if_needed(source)

        for attempt in range(self.max_retries):
            try:
                result = func(*args, **kwargs)
                if result is not None:
                    return result
                else:
                    if attempt < self.max_retries - 1:
                        delay = self.retry_delays[min(attempt, len(self.retry_delays) - 1)]
                        time.sleep(delay)
            except Exception as e:
                if attempt < self.max_retries - 1:
                    delay = self.retry_delays[min(attempt, len(self.retry_delays) - 1)]
                    time.sleep(delay)
                else:
                    return None

        return None


# 全局实例
request_manager = RequestManager()
