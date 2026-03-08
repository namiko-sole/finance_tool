"""
统一数据获取器

数据获取优先级：
1. 本地数据 /root/.openclaw/workspace/data/raw
2. tushare
3. akshare
4. 其他

支持：
- 股票日K线数据
- 股票列表
- 交易日历
"""

import os
import pandas as pd
import logging
from pathlib import Path
from typing import Optional
from datetime import datetime

# 本地数据目录
LOCAL_DATA_DIR = Path("/root/.openclaw/workspace/data/raw")

logger = logging.getLogger(__name__)


class DataFetcher:
    """
    统一数据获取器，按优先级自动选择数据源
    """

    def __init__(self):
        """初始化数据获取器"""
        self.local_dir = LOCAL_DATA_DIR
        self.tushare_fetcher = None
        self.akshare_available = False

        # 初始化 tushare（如果可用）
        self._init_tushare()

        # 检查 akshare（如果可用）
        self._check_akshare()

    def _init_tushare(self):
        """初始化 tushare"""
        try:
            import tushare as ts
            token = os.getenv('TUSHARE_TOKEN', 'da8b0417e689bd72a3e5b3312fb6094f9405b06b667f7033eebecf83')
            if token:
                self.tushare_fetcher = ts.pro_api(token)
                logger.info("Tushare 初始化成功")
            else:
                logger.warning("TUSHARE_TOKEN 未设置")
        except ImportError:
            logger.warning("Tushare 未安装")
        except Exception as e:
            logger.warning(f"Tushare 初始化失败: {e}")

    def _check_akshare(self):
        """检查 akshare 是否可用"""
        try:
            import akshare
            self.akshare_available = True
            logger.info("AKShare 可用")
        except ImportError:
            self.akshare_available = False
            logger.warning("AKShare 未安装")

    # ==================== 股票日K线 ====================

    def get_stock_daily(self, symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取股票日K线数据

        优先级：本地 > tushare > akshare

        Args:
            symbol: 股票代码（如 '000001.SZ', 'sh600519'）
            start_date: 开始日期（格式：YYYYMMDD 或 YYYY-MM-DD）
            end_date: 结束日期（格式：YYYYMMDD 或 YYYY-MM-DD）

        Returns:
            DataFrame，包含 date, open, high, low, close, volume 列
        """
        # 1. 尝试本地数据
        df = self._get_from_local(symbol, start_date, end_date)
        if df is not None and not df.empty:
            logger.info(f"[本地] 获取 {symbol} 日K线 {len(df)} 条")
            return df

        # 2. 尝试 tushare
        if self.tushare_fetcher:
            df = self._get_from_tushare(symbol, start_date, end_date)
            if df is not None and not df.empty:
                logger.info(f"[Tushare] 获取 {symbol} 日K线 {len(df)} 条")
                return df

        # 3. 尝试 akshare
        if self.akshare_available:
            df = self._get_from_akshare(symbol, start_date, end_date)
            if df is not None and not df.empty:
                logger.info(f"[AKShare] 获取 {symbol} 日K线 {len(df)} 条")
                return df

        logger.error(f"无法获取 {symbol} 的日K线数据")
        return pd.DataFrame()

    def _get_from_local(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """从本地获取数据"""
        try:
            # 转换股票代码格式
            ts_code = self._normalize_symbol(symbol)

            # 查找本地文件
            local_file = self.local_dir / "stock_daily" / f"{ts_code}.csv"

            if not local_file.exists():
                return None

            df = pd.read_csv(local_file)

            if df.empty:
                return None

            # 转换日期格式
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
                df = df.sort_values('trade_date')

                # 过滤日期范围
                if start_date:
                    start = pd.to_datetime(start_date.replace('-', ''))
                    df = df[df['trade_date'] >= start]
                if end_date:
                    end = pd.to_datetime(end_date.replace('-', ''))
                    df = df[df['trade_date'] <= end]

                # 转换列名
                df = df.rename(columns={
                    'trade_date': 'date',
                    'vol': 'volume'
                })

                # 选择需要的列
                cols = ['date', 'open', 'high', 'low', 'close', 'volume']
                df = df[[c for c in cols if c in df.columns]]

                # 转换日期为字符串
                df['date'] = df['date'].dt.strftime('%Y-%m-%d')

                return df

        except Exception as e:
            logger.debug(f"本地数据获取失败: {e}")

        return None

    def _get_from_tushare(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """从 tushare 获取数据"""
        try:
            import tushare as ts

            # 转换日期格式
            if start_date:
                start_date = start_date.replace('-', '')
            if end_date:
                end_date = end_date.replace('-', '')

            if not start_date:
                start_date = '19900101'
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')

            # 标准化代码
            ts_code = self._normalize_to_tscode(symbol)

            # 使用 tushare 获取数据
            df = self.tushare_fetcher.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)

            if df is not None and not df.empty:
                df = df.rename(columns={
                    'trade_date': 'date',
                    'vol': 'volume'
                })
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                df = df.sort_values('date')

                cols = ['date', 'open', 'high', 'low', 'close', 'volume']
                df = df[[c for c in cols if c in df.columns]]

                return df

        except Exception as e:
            logger.debug(f"Tushare 数据获取失败: {e}")

        return None

    def _get_from_akshare(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """从 akshare 获取数据"""
        try:
            import akshare as ak

            # 转换日期格式
            if start_date:
                start_date = start_date.replace('-', '')
            if end_date:
                end_date = end_date.replace('-', '')

            if not start_date:
                start_date = (datetime.now() - pd.Timedelta(days=365)).strftime('%Y%m%d')
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')

            # 提取股票代码
            code = symbol[2:] if len(symbol) > 6 else symbol

            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=""
            )

            if df is not None and not df.empty:
                df = df.rename(columns={
                    '日期': 'date',
                    '开盘': 'open',
                    '收盘': 'close',
                    '最高': 'high',
                    '最低': 'low',
                    '成交量': 'volume'
                })
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                df = df.sort_values('date')

                cols = ['date', 'open', 'high', 'low', 'close', 'volume']
                df = df[[c for c in cols if c in df.columns]]

                return df

        except Exception as e:
            logger.debug(f"AKShare 数据获取失败: {e}")

        return None

    def _normalize_symbol(self, symbol: str) -> str:
        """标准化股票代码为 tushare 格式"""
        symbol = symbol.upper().strip()

        # 如果已经是标准格式，直接返回
        if '.' in symbol:
            return symbol

        # 添加市场后缀
        if symbol.startswith('6'):
            return f"{symbol}.SH"
        elif symbol.startswith(('0', '3')):
            return f"{symbol}.SZ"
        elif symbol.startswith('8') or symbol.startswith('4'):
            return f"{symbol}.BJ"

        return symbol

    def _normalize_to_tscode(self, symbol: str) -> str:
        """标准化为 tushare 代码格式"""
        return self._normalize_symbol(symbol)

    # ==================== 股票列表 ====================

    def get_stock_list(self) -> pd.DataFrame:
        """
        获取股票列表

        优先级：本地 > tushare
        """
        # 1. 尝试本地数据
        local_file = self.local_dir / "stock_list.csv"
        if local_file.exists():
            try:
                df = pd.read_csv(local_file)
                logger.info(f"[本地] 获取股票列表 {len(df)} 只")
                return df
            except Exception as e:
                logger.debug(f"本地股票列表获取失败: {e}")

        # 2. 尝试 tushare
        if self.tushare_fetcher:
            try:
                df = self.tushare_fetcher.stock_basic(list_status='L')
                if df is not None and not df.empty:
                    logger.info(f"[Tushare] 获取股票列表 {len(df)} 只")
                    return df
            except Exception as e:
                logger.debug(f"Tushare 股票列表获取失败: {e}")

        return pd.DataFrame()

    # ==================== 交易日历 ====================

    def get_trade_calendar(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取交易日历

        优先级：本地 > tushare
        """
        # 1. 尝试本地数据
        local_file = self.local_dir / "trade_calendar.csv"
        if local_file.exists():
            try:
                df = pd.read_csv(local_file)
                if start_date:
                    df = df[df['cal_date'] >= start_date]
                if end_date:
                    df = df[df['cal_date'] <= end_date]
                logger.info(f"[本地] 获取交易日历 {len(df)} 条")
                return df
            except Exception as e:
                logger.debug(f"本地交易日历获取失败: {e}")

        # 2. 尝试 tushare
        if self.tushare_fetcher:
            try:
                if start_date:
                    start_date = start_date.replace('-', '')
                if end_date:
                    end_date = end_date.replace('-', '')

                df = self.tushare_fetcher.trade_cal(
                    exchange='SSE',
                    start_date=start_date,
                    end_date=end_date
                )
                if df is not None and not df.empty:
                    logger.info(f"[Tushare] 获取交易日历 {len(df)} 条")
                    return df
            except Exception as e:
                logger.debug(f"Tushare 交易日历获取失败: {e}")

        return pd.DataFrame()


def get_kline_data(symbol: str, days: int = 120) -> pd.DataFrame:
    """
    便捷函数：获取K线数据

    Args:
        symbol: 股票代码
        days: 获取天数

    Returns:
        DataFrame
    """
    from datetime import datetime, timedelta

    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')

    fetcher = DataFetcher()
    return fetcher.get_stock_daily(symbol, start_date, end_date)


if __name__ == "__main__":
    # 测试数据获取
    logging.basicConfig(level=logging.INFO)

    print("=" * 60)
    print("测试统一数据获取器")
    print("=" * 60)

    fetcher = DataFetcher()

    # 测试1: 获取股票日K线
    print("\n[1] 测试获取股票日K线...")
    df = fetcher.get_stock_daily("000001.SZ", start_date="20250101", end_date="20250301")
    if not df.empty:
        print(f"✅ 获取成功: {len(df)} 条")
        print(df.head())
    else:
        print("❌ 获取失败")

    # 测试2: 获取股票列表
    print("\n[2] 测试获取股票列表...")
    df = fetcher.get_stock_list()
    if not df.empty:
        print(f"✅ 获取成功: {len(df)} 只")
    else:
        print("❌ 获取失败")

    # 测试3: 获取交易日历
    print("\n[3] 测试获取交易日历...")
    df = fetcher.get_trade_calendar(start_date="20250101", end_date="20251231")
    if not df.empty:
        print(f"✅ 获取成功: {len(df)} 条")
    else:
        print("❌ 获取失败")
