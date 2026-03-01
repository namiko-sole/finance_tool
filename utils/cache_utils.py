#!/usr/bin/env python3
"""
缓存工具模块
"""

import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional
import hashlib
import threading

# 缓存目录
CACHE_DIR = Path(__file__).parent.parent.parent / 'data' / 'cache'
CACHE_DIR.mkdir(parents=True, exist_ok=True)


class CacheManager:
    """缓存管理器"""

    def __init__(self):
        """初始化缓存管理器"""
        self.memory_cache = {}
        self.cache_lock = threading.Lock()

        # 缓存过期时间（秒）
        self.cache_ttl = {
            'realtime': 60,
            'kline_daily': 3600,
            'kline_mins': 300,
            'moneyflow': 600,
            'financial': 86400,
            'holders': 86400,
            'rating': 86400,
            'toplist': 1800,
            'default': 1800
        }

    def _get_cache_key(self, category: str, params: Dict) -> str:
        """生成缓存键"""
        params_str = json.dumps(params, sort_keys=True)
        return f"{category}_{hashlib.md5(params_str.encode()).hexdigest()}"

    def _get_cache_file(self, cache_key: str) -> Path:
        """获取缓存文件路径"""
        return CACHE_DIR / f"{cache_key}.json"

    def get(self, category: str, params: Dict, force_refresh: bool = False) -> Optional[Any]:
        """获取缓存数据"""
        cache_key = self._get_cache_key(category, params)

        # 1. 检查内存缓存
        if not force_refresh and cache_key in self.memory_cache:
            cached = self.memory_cache[cache_key]
            if not self._is_expired(cached):
                return cached['data']
            else:
                del self.memory_cache[cache_key]

        # 2. 检查文件缓存
        cache_file = self._get_cache_file(cache_key)
        if not force_refresh and cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cached = json.load(f)
                if not self._is_expired(cached):
                    self.memory_cache[cache_key] = cached
                    return cached['data']
                else:
                    cache_file.unlink()
            except:
                pass

        return None

    def set(self, category: str, params: Dict, data: Any) -> None:
        """设置缓存数据"""
        cache_key = self._get_cache_key(category, params)
        ttl = self.cache_ttl.get(category, self.cache_ttl['default'])

        cached = {
            'data': data,
            'cached_at': datetime.now().isoformat(),
            'expires_at': (datetime.now() + timedelta(seconds=ttl)).isoformat(),
            'ttl': ttl
        }

        # 保存到内存
        with self.cache_lock:
            self.memory_cache[cache_key] = cached

        # 保存到文件
        cache_file = self._get_cache_file(cache_key)
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cached, f, ensure_ascii=False, indent=2)
        except:
            pass

    def _is_expired(self, cached: Dict) -> bool:
        """检查缓存是否过期"""
        expires_at = cached.get('expires_at')
        if not expires_at:
            return True
        try:
            return datetime.now() > datetime.fromisoformat(expires_at)
        except:
            return True

    def clear(self, category: Optional[str] = None) -> None:
        """清除缓存"""
        if category is None:
            with self.cache_lock:
                self.memory_cache.clear()
            for cache_file in CACHE_DIR.glob('*.json'):
                cache_file.unlink()
        else:
            keys_to_delete = []
            for key in self.memory_cache:
                if key.startswith(category):
                    keys_to_delete.append(key)
            with self.cache_lock:
                for key in keys_to_delete:
                    del self.memory_cache[key]
            for cache_file in CACHE_DIR.glob(f'{category}_*.json'):
                cache_file.unlink()

    def get_cache_stats(self) -> Dict:
        """获取缓存统计信息"""
        cache_files = list(CACHE_DIR.glob('*.json'))
        total_size = sum(f.stat().st_size for f in cache_files)

        return {
            'memory_cache_count': len(self.memory_cache),
            'file_cache_count': len(cache_files),
            'total_size_mb': round(total_size / 1024 / 1024, 2),
            'cache_dir': str(CACHE_DIR)
        }


# 全局实例
cache_manager = CacheManager()
