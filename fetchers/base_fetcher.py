"""
基础 Fetcher 模块

提供数据获取基类和重试机制。
"""

import time
import random
import logging
import ssl
from typing import Callable, Any, Optional
from urllib3.exceptions import SSLError

logger = logging.getLogger(__name__)


class BaseFetcher:
    """
    数据获取基类，提供重试机制
    """

    def __init__(self, storage=None, retry_count: Optional[int] = None, config: Optional[object] = None) -> None:
        """
        初始化 Fetcher

        Args:
            storage: Parquet 存储器
            retry_count: 失败重试次数（优先级高于 config）
            config: 配置对象（需要 retry_count 属性）
        """
        self.storage = storage
        self.retry_count = retry_count if retry_count is not None else (config.retry_count if config else 3)

    def _is_network_error(self, exception: Exception) -> bool:
        """
        判断是否为网络错误

        Args:
            exception: 异常对象

        Returns:
            是否为网络错误
        """
        network_errors = (
            ConnectionError,
            TimeoutError,
            SSLError,
            ssl.SSLError,
            OSError
        )
        return isinstance(exception, network_errors)

    def fetch_with_retry(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """
        带重试机制的数据获取

        Args:
            func: 数据获取函数
            *args, **kwargs: 传递给 func 的参数

        Returns:
            func 的返回值，重试失败返回 None
        """
        for attempt in range(self.retry_count):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == self.retry_count - 1:
                    logger.error(f"Failed after {self.retry_count} retries: {e}")
                    return None

                # 区分网络错误和其他错误
                if self._is_network_error(e):
                    # 指数退避 + 随机抖动
                    base_delay = 2 ** attempt  # 1, 2, 4, 8, 16...
                    jitter = random.uniform(0, 1) * base_delay * 0.5
                    delay = base_delay + jitter
                    logger.warning(f"Network error (attempt {attempt + 1}/{self.retry_count}): {e}, retrying in {delay:.2f}s...")
                else:
                    # 非网络错误使用固定延迟
                    delay = 1
                    logger.warning(f"Error (attempt {attempt + 1}/{self.retry_count}): {e}, retrying in {delay}s...")

                time.sleep(delay)
        return None
