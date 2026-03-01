"""
每日市场数据获取器模块（龙虎榜、资金流向等）
"""

import os
import sys
import akshare as ak
import pandas as pd
import logging
import requests
from typing import Optional
from datetime import datetime
import argparse
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fetchers.base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)


class MarketDataFetcher(BaseFetcher):
    """
    每日市场数据获取器（龙虎榜、资金流向等）
    """

    # 同花顺 API 配置
    _10JQKA_URL = "http://dq.10jqka.com.cn/fuyao/pc_hevo_ztjj/focus_limit_up/v1/change-table"
    _10JQKA_HEADERS = {
        "Host": "dq.10jqka.com.cn",
        "Referer": "PC_Hevo_FocusLimitUp_ChangeGridWidget",
        "User-Agent": "hevo",
        "Cookie": "user=MDptb19mYWlnY3Uzd3I6Ok5vbmU6NTAwOjc3ODc2MDc5Mzo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxMDEsNDA7MiwxLDQwOzMsMSw0MDs1LDEsNDA7OCwwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMSw0MDsxMDIsMSw0MDoxNjo6Ojc2ODc2MDc5MzoxNzY4OTA5MTc3Ojo6MTczOTkzMTE4MDoyNjc4NDAwOjA6MWFiNDNlODhiMTVjYzc0ZjZiN2I0NzEzMjg3MTZlN2UxOjox; userid=768760793; u_name=mo_faigcu3wr; sess_tk=eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiIsImtpZCI6InNlc3NfdGtfMSIsImJ0eSI6InNlc3NfdGsifQ.eyJqdGkiOiJlMWU3MTY4NzMyNzFiNGI3ZjY3NGNjMTU4YmU4NDNhYjEiLCJpYXQiOjE3Njg5MDkxNzcsImV4cCI6MTc3MTU4NzU3Nywic3ViIjoiNzY4NzYwNzkzIiwiaXNzIjoidXBhc3MuMTBqcWthLmNvbS5jbiIsImF1ZCI6IjIwMjMwODA0OTA3NTEyOTIiLCJhY3QiOiJvZmMiLCJjdWhzIjoiNDYwMzUwYzNlYjY0NzgyZDQzNzYyZDZjZTk2ODNjMmFmOTEzYjE5NjU3OWZhYTZlZTgxN2MyYTE2NDMxYmJjYSJ9.7YPEdtHGO4IxnNad2wlahM5KIligvkG_8EKWPMTHb6UDyDHho1mXxwJcaRlgKJJA-sXOzRvEWF4X7K1DhroQhQ; cuc=bd02a73600a3474c9b8a6ead2c445e8d; escapename=mo_faigcu3wr; ticket=1d347ba847c029d53f4b39956c69df81; user_status=0"
    }

    def _fetch_10jqka_data(
        self,
        date: str,
        strategy: str,
        field: str,
        limit: str = "21",
        with_pagination: bool = True,
        data_type_name: str = "data"
    ) -> pd.DataFrame:
        """
        通用方法：从同花顺 API 获取数据

        Args:
            date: 日期（格式：YYYYMMDD）
            strategy: 策略参数（如 limit_up_pool, limit_down_pool）
            field: 字段参数
            limit: 每页数量
            with_pagination: 是否需要分页
            data_type_name: 数据类型名称（用于日志）

        Returns:
            包含数据的 DataFrame
        """
        try:
            # 构建请求参数
            params = {
                "strategy": strategy,
                "filter": "HS,GEM2STAR",
                "date": date,
                "field": field,
                "orderField": "199112",
                "orderType": "0",
                "limit": limit,
                "page": "1"
            }

            # 发送第一页请求
            response = requests.get(
                self._10JQKA_URL,
                params=params,
                headers=self._10JQKA_HEADERS,
                timeout=30
            )
            response.raise_for_status()

            # 解析JSON响应
            result = response.json()

            # 检查响应状态
            if result.get("status_code") != 0:
                logger.error(f"API returned error for {date}: {result.get('status_msg', 'Unknown error')}")
                return pd.DataFrame()

            # 获取总页数和数据
            page_info = result.get("data", {}).get("page", {})
            total_pages = page_info.get("total", 1)
            all_data = result.get("data", {}).get("data", [])

            # 如果需要分页，获取剩余页的数据
            if with_pagination and total_pages > 1:
                for page in range(2, total_pages + 1):
                    params["page"] = str(page)
                    response = requests.get(
                        self._10JQKA_URL,
                        params=params,
                        headers=self._10JQKA_HEADERS,
                        timeout=30
                    )
                    response.raise_for_status()
                    result = response.json()

                    if result.get("status_code") == 0:
                        page_data = result.get("data", {}).get("data", [])
                        all_data.extend(page_data)
                    else:
                        logger.warning(f"Failed to fetch page {page} for {date}: {result.get('status_msg', 'Unknown error')}")

            # 转换为DataFrame
            if not all_data:
                logger.warning(f"No {data_type_name} data found for {date}")
                return pd.DataFrame()

            df = pd.DataFrame(all_data)
            logger.info(f"Found {len(df)} {data_type_name} records for {date}")
            return df

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch {data_type_name} data for {date}: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Failed to fetch {data_type_name} data for {date}: {e}")
            return pd.DataFrame()

    def fetch_limit_up(self, date: str) -> pd.DataFrame:
        """
        获取涨停板数据

        Args:
            date: 日期（格式：YYYYMMDD）

        Returns:
            包含涨停板数据的 DataFrame
        """
        return self._fetch_10jqka_data(
            date=date,
            strategy="limit_up_pool",
            field="5,55,199112,10,9001,330323,330329,3475914,1968584,19,330324,330325,9002,133971,133970,3541450,9003,48",
            limit="21",
            with_pagination=True,
            data_type_name="limit up"
        )

    def fetch_limit_down(self, date: str) -> pd.DataFrame:
        """
        获取跌停板数据

        Args:
            date: 日期（格式：YYYYMMDD）

        Returns:
            包含跌停板数据的 DataFrame
        """
        return self._fetch_10jqka_data(
            date=date,
            strategy="limit_down_pool",
            field="330333,330334",
            limit="24",
            with_pagination=True,
            data_type_name="limit down"
        )

    def fetch_board_broken(self, date: str) -> pd.DataFrame:
        """
        获取炸板数据

        使用同花顺API接口获取炸板股票数据。
        炸板股票是指当天涨停后打开的股票。

        Args:
            date: 日期（格式：YYYYMMDD）

        Returns:
            包含炸板数据的 DataFrame
        """
        return self._fetch_10jqka_data(
            date=date,
            strategy="open_limit_up_pool",
            field="9002,9003",
            limit="24",
            with_pagination=True,
            data_type_name="board broken"
        )

    def fetch_consecutive_limit_up(self, date: str) -> pd.DataFrame:
        """
        获取连板数据

        Args:
            date: 日期（格式：YYYYMMDD）

        Returns:
            包含连板数据的 DataFrame
        """
        try:
            # 构建请求URL和参数
            params = {
                "strategy": "connect_limit_pool",
                "filter": "HS,GEM2STAR",
                "date": date,
                "field": "continue_num,199121",
                "orderField": "continue_num",
                "orderType": "1"
            }

            # 构建请求头（使用不同的 Referer）
            headers = self._10JQKA_HEADERS.copy()
            headers["Referer"] = "PC_Hevo_FocusLimitUp_EvenPlateLadderGridWidget"

            # 发送请求
            response = requests.get(self._10JQKA_URL, params=params, headers=headers, timeout=30)
            response.raise_for_status()

            # 解析JSON响应
            result = response.json()

            # 检查响应状态
            if result.get("status_code") != 0:
                logger.error(f"API returned error for {date}: {result.get('status_msg', 'Unknown error')}")
                return pd.DataFrame()

            # 提取数据并转换为DataFrame
            data_list = result.get("data", {}).get("data", [])
            if not data_list:
                logger.warning(f"No consecutive limit up data found for {date}")
                return pd.DataFrame()

            df = pd.DataFrame(data_list)
            logger.info(f"Found {len(df)} consecutive limit up records for {date}")
            return df

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch consecutive limit up data for {date}: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Failed to fetch consecutive limit up data for {date}: {e}")
            return pd.DataFrame()


def _print_preview(df: pd.DataFrame, name: str, limit: int) -> None:
    if df.empty:
        logger.info("%s: no data", name)
        return
    logger.info("%s: %d rows", name, len(df))
    print(df.head(limit).to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="Test THS limit up/down data fetcher")
    parser.add_argument("--date", help="Date in YYYYMMDD format, default today")
    parser.add_argument(
        "--task",
        choices=["limit_up", "limit_down", "board_broken", "consecutive", "all"],
        default="all",
        help="Which dataset to fetch",
    )
    parser.add_argument("--preview", type=int, default=5, help="Preview rows to print")
    args = parser.parse_args()

    date = args.date or datetime.now().strftime("%Y%m%d")
    fetcher = MarketDataFetcher()

    if args.task in ("limit_up", "all"):
        df = fetcher.fetch_limit_up(date)
        _print_preview(df, "limit_up", args.preview)
    if args.task in ("limit_down", "all"):
        df = fetcher.fetch_limit_down(date)
        _print_preview(df, "limit_down", args.preview)
    if args.task in ("board_broken", "all"):
        df = fetcher.fetch_board_broken(date)
        _print_preview(df, "board_broken", args.preview)
    if args.task in ("consecutive", "all"):
        df = fetcher.fetch_consecutive_limit_up(date)
        _print_preview(df, "consecutive", args.preview)


if __name__ == "__main__":
    main()
