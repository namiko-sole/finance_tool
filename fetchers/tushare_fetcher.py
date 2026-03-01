"""
Tushare 数据获取器

使用 Tushare API 获取各类市场数据，包括：
- 交易日历
- 股票列表
- 日线数据
- 申万2021版行业分类和成分股
- 实时行情数据（实时行情、实时分笔成交、实时涨跌幅排名列表）
"""

import tushare as ts
import pandas as pd
import logging
import os
from typing import Optional

# 处理相对导入和绝对导入
try:
    from .base_fetcher import BaseFetcher
except ImportError:
    from base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)


class TushareFetcher(BaseFetcher):
    """
    Tushare 数据获取器
    """

    def __init__(self, storage, retry_count: Optional[int] = None, config: Optional[object] = None) -> None:
        """
        初始化 Fetcher

        Args:
            storage: Parquet 存储器
            retry_count: 失败重试次数（优先级高于 config）
            config: 配置对象
        """
        super().__init__(storage, retry_count, config)
        # 从环境变量获取 Tushare Token，不再使用硬编码默认值
        self.ts_token = os.getenv('TUSHARE_TOKEN', 'da8b0417e689bd72a3e5b3312fb6094f9405b06b667f7033eebecf83')
        if not self.ts_token:
            logger.error("TUSHARE_TOKEN 环境变量未设置，Tushare API 将无法使用")
            self.pro = None
        else:
            self.pro = ts.pro_api(self.ts_token)
            logger.info("Tushare API 初始化成功")

    # ==================== 交易日历相关接口 ====================

    def _fetch_trade_calendar_internal(self) -> pd.DataFrame:
        """
        内部方法：获取交易日历（用于重试）
        """
        df = self.pro.trade_cal(exchange='SSE')
        if not df.empty:
            # 转换日期格式
            df['cal_date'] = pd.to_datetime(df['cal_date']).dt.date
        return df

    def fetch_trade_calendar(self) -> pd.DataFrame:
        """
        获取交易日历

        Returns:
            包含交易日历的 DataFrame，包含以下字段：
            - exchange: 交易所 SSE上交所 SZSE深交所
            - cal_date: 日历日期
            - is_open: 是否开市（1=开市，0=休市）
            - pretrade_date: 前一交易日
        """
        if not self.pro:
            logger.error("Tushare API 未初始化，请设置 TUSHARE_TOKEN 环境变量")
            return pd.DataFrame()

        result = self.fetch_with_retry(self._fetch_trade_calendar_internal)
        if result is not None and not result.empty:
            logger.info(f"成功获取交易日历，共 {len(result)} 条记录")
            return result
        return pd.DataFrame()

    # ==================== 股票列表相关接口 ====================

    def _fetch_stock_list_internal(self) -> pd.DataFrame:
        """
        内部方法：获取 A 股列表数据（用于重试）
        """
        return self.pro.stock_basic(list_status='L', fields=["ts_code","symbol","name","area","industry","cnspell","act_name","act_ent_type","enname","fullname","list_date"])

    def fetch_stock_list(self) -> pd.DataFrame:
        """
        获取 A 股列表数据

        Returns:
            包含股票列表的 DataFrame，包含以下字段：
            - ts_code: 股票代码
            - symbol: 股票简称
            - name: 股票名称
            - area: 地域
            - industry: 行业
            - cnspell: 拼音缩写
            - act_name: 实控人名称
            - act_ent_type: 实控人企业性质
            - enname: 英文名称
            - fullname: 股票全称
            - list_date: 上市日期
        """
        if not self.pro:
            logger.error("Tushare API 未初始化，请设置 TUSHARE_TOKEN 环境变量")
            return pd.DataFrame()

        result = self.fetch_with_retry(self._fetch_stock_list_internal)
        if result is not None and not result.empty:
            logger.info(f"成功获取股票列表，共 {len(result)} 只股票")
            return result
        return pd.DataFrame()

    # ==================== 日线数据相关接口 ====================

    def _fetch_daily_history_internal(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        内部方法：获取个股日线数据（用于重试）
        """
        df = self.pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if not df.empty:
            # 重命名列以保持一致性
            df = df.rename(columns={
                'ts_code': 'symbol',
                'trade_date': 'date'
            })
            # 转换日期格式
            df['date'] = pd.to_datetime(df['date']).dt.date
        return df

    def fetch_daily_history(self, ts_code: str, start_date: str = '19900101', end_date: str = None) -> pd.DataFrame:
        """
        获取个股日线数据

        Args:
            ts_code: 股票代码（如 '000001.SZ'）
            start_date: 开始日期（格式：YYYYMMDD）
            end_date: 结束日期（格式：YYYYMMDD），默认为当前日期

        Returns:
            包含日线数据的 DataFrame，包含以下字段：
            - ts_code: 股票代码
            - trade_date: 交易日期
            - open: 开盘价
            - high: 最高价
            - low: 最低价
            - close: 收盘价
            - pre_close: 昨收价
            - change: 涨跌额
            - pct_chg: 涨跌幅
            - vol: 成交量（手）
            - amount: 成交额（千元）
        """
        if not self.pro:
            logger.error("Tushare API 未初始化，请设置 TUSHARE_TOKEN 环境变量")
            return pd.DataFrame()

        if end_date is None:
            from datetime import datetime
            end_date = datetime.now().strftime('%Y%m%d')

        result = self.fetch_with_retry(self._fetch_daily_history_internal, ts_code, start_date, end_date)
        if result is not None and not result.empty:
            logger.debug(f"成功获取 {ts_code} 日线数据，共 {len(result)} 条记录")
            return result
        return pd.DataFrame()

    # ==================== 申万2021版行业分类相关接口 ====================

    def _fetch_sw2021_classification_internal(self, level: str) -> pd.DataFrame:
        """
        内部方法：获取申万2021版行业分类数据（用于重试）
        """
        df = self.pro.index_classify(src='SW2021', level=level)
        if not df.empty:
            # 根据级别过滤
            if level in ['L1', 'L2', 'L3']:
                df = df[df['level'] == level].copy()
        return df

    def fetch_sw2021_classification(self, level: int = 0) -> pd.DataFrame:
        """
        获取申万2021版行业分类数据

        Args:
            level: 行业级别（0=全部行业，1=一级行业，2=二级行业，3=三级行业）

        Returns:
            包含行业分类的 DataFrame，包含以下字段：
            - index_code: 指数代码
            - industry_name: 行业名称
            - level: 行业级别
            - industry_code: 行业代码
            - parent_code: 父级行业代码
        """
        if not self.pro:
            logger.error("Tushare API 未初始化，请设置 TUSHARE_TOKEN 环境变量")
            return pd.DataFrame()

        if level not in [0, 1, 2, 3]:
            logger.warning(f"无效的行业级别 {level}，仅支持 0、1、2、3。将返回空DataFrame。")
            return pd.DataFrame()

        # 转换级别参数
        level_param = None if level == 0 else f"L{level}"

        result = self.fetch_with_retry(self._fetch_sw2021_classification_internal, level_param)
        if result is not None and not result.empty:
            logger.info(f"成功获取申万2021版 {level if level else '全部'} 行业分类，共 {len(result)} 条记录")
            return result
        return pd.DataFrame()

    def _fetch_sw2021_components_by_code_internal(self, l1_code: str, l2_code: str, l3_code: str) -> pd.DataFrame:
        """
        内部方法：获取申万2021版行业成分股数据（用于重试）
        """
        return self.pro.index_member_all(l1_code=l1_code, l2_code=l2_code, l3_code=l3_code)

    def fetch_sw2021_components_by_code(self, l1_code: str=None, l2_code: str=None, l3_code: str=None) -> pd.DataFrame:
        """
        获取申万2021版行业成分股数据

        Returns:
            包含成分股的 DataFrame，包含以下字段：
            - l1_code: L1代码
            - l1_name: L1名称
            - l2_code: L2代码
            - l2_name: L2名称
            - l3_code: L3代码
            - l3_name: L3名称
            - ts_code: 成分股票代码
            - name: 成分股票名称
            - in_date: 纳入日期
            - out_date: 剔除日期
            - is_new: 是否新纳入
        """
        if not self.pro:
            logger.error("Tushare API 未初始化，请设置 TUSHARE_TOKEN 环境变量")
            return pd.DataFrame()

        result = self.fetch_with_retry(self._fetch_sw2021_components_by_code_internal, l1_code, l2_code, l3_code)
        if result is not None and not result.empty:
            logger.info(f"成功获取申万2021版{l1_code or l2_code or l3_code}成分股，共 {len(result)} 只")
            return result
        return pd.DataFrame()


    # ==================== 港股相关接口 ====================
    
    def _fetch_hk_stock_list_internal(self) -> pd.DataFrame:
        """
        内部方法：获取港股列表数据（用于重试）
        """
        return self.pro.hk_basic(**{
            "ts_code": "",
            "list_status": "L",
            "limit": "",
            "offset": ""
        }, fields=[
            "ts_code",
            "name",
            "fullname",
            "enname",
            "cnspell",
            "market",
            "list_status",
            "list_date",
            "delist_date",
            "trade_unit",
            "isin",
            "curr_type"
        ])
    
    def fetch_hk_stock_list(self) -> pd.DataFrame:
        """
        获取港股列表数据
        
        Returns:
            包含港股列表的 DataFrame，包含以下字段：
            - ts_code: 港股代码（格式：股票代码.HK）
            - name: 股票名称
            - fullname: 股票全称
            - enname: 英文名称
            - cnspell: 拼音缩写
            - market: 市场（港股）
            - list_status: 上市状态
            - list_date: 上市日期
            - delist_date: 退市日期
            - trade_unit: 交易单位
            - isin: ISIN代码
            - curr_type: 货币类型
        """
        if not self.pro:
            logger.error("Tushare API 未初始化，请设置 TUSHARE_TOKEN 环境变量")
            return pd.DataFrame()
        
        result = self.fetch_with_retry(self._fetch_hk_stock_list_internal)
        if result is not None and not result.empty:
            logger.info(f"成功获取港股列表，共 {len(result)} 只股票")
            return result
        return pd.DataFrame()
    
    def _fetch_us_stock_list_internal(self) -> pd.DataFrame:
        """
        内部方法：获取美股列表数据（用于重试）
        """
        df = self.pro.us_basic(**{
            "ts_code": "",
            "classify": "",
            "list_status": "L",
            "offset": "",
            "limit": ""
        }, fields=[
            "ts_code",
            "name",
            "enname",
            "classify",
            "delist_date",
            "list_date"
        ])
        # 添加市场后缀到 ts_code
        if not df.empty:
            df['ts_code'] = df['ts_code'] + '.US'
        return df
    
    def fetch_us_stock_list(self) -> pd.DataFrame:
        """
        获取美股列表数据
        
        Returns:
            包含美股列表的 DataFrame，包含以下字段：
            - ts_code: 美股代码（格式：股票代码.US）
            - name: 股票名称
            - enname: 英文名称
            - classify: 分类
            - delist_date: 退市日期
            - list_date: 上市日期
        """
        if not self.pro:
            logger.error("Tushare API 未初始化，请设置 TUSHARE_TOKEN 环境变量")
            return pd.DataFrame()
        
        result = self.fetch_with_retry(self._fetch_us_stock_list_internal)
        if result is not None and not result.empty:
            logger.info(f"成功获取美股列表，共 {len(result)} 只股票")
            return result
        return pd.DataFrame()

 
    # ==================== 实时行情相关接口 ====================
    
    def _fetch_realtime_quote_internal(self, ts_code: str, src: str) -> pd.DataFrame:
        """
        内部方法：获取实时行情数据（用于重试）
        """
        # 设置 tushare token（实时行情接口需要）
        if self.ts_token:
            ts.set_token(self.ts_token)
        return ts.realtime_quote(ts_code=ts_code, src=src)
    
    def fetch_realtime_quote(self, ts_code: str, src: str = 'sina') -> pd.DataFrame:
        """
        获取实时行情数据
        
        注意：此接口使用 tushare 的旧版实时行情接口，需要设置 token。
        
        Args:
            ts_code: 股票代码（如 '000001.SZ'），可选
                - sina 数据源：支持多个股票代码，逗号分隔，最多50只（如 '600000.SH,000001.SZ'）
                - dc 数据源：仅支持单个股票代码
            src: 数据源（'sina' 新浪，'dc' 东方财富，默认 'sina'）
        
        Returns:
            包含实时行情的 DataFrame，包含以下字段：
            - name: 股票名称
            - code: 股票代码（sina数据源）
            - ts_code: 股票代码（部分数据源）
            - date: 交易日期
            - time: 交易时间
            - open: 开盘价
            - pre_close: 昨收价
            - price: 当前价格
            - high: 今日最高价
            - low: 今日最低价
            - bid: 买一价
            - ask: 卖一价
            - volume: 成交量（sina为股，dc为手）
            - amount: 成交额（元）
            - b1_v: 买一量
            - b1_p: 买一价
            - b2_v: 买二量
            - b2_p: 买二价
            - b3_v: 买三量
            - b3_p: 买三价
            - b4_v: 买四量
            - b4_p: 买四价
            - b5_v: 买五量
            - b5_p: 买五价
            - a1_v: 卖一量
            - a1_p: 卖一价
            - a2_v: 卖二量
            - a2_p: 卖二价
            - a3_v: 卖三量
            - a3_p: 卖三价
            - a4_v: 卖四量
            - a4_p: 卖四价
            - a5_v: 卖五量
            - a5_p: 卖五价
        """
        if not self.ts_token:
            logger.error("Tushare Token 未设置，请设置 TUSHARE_TOKEN 环境变量")
            return pd.DataFrame()
        
        result = self.fetch_with_retry(self._fetch_realtime_quote_internal, ts_code, src)
        if result is not None and not result.empty:
            logger.info(f"成功获取实时行情数据，共 {len(result)} 条记录")
            return result
        return pd.DataFrame()
    
    def _fetch_realtime_tick_internal(self, ts_code: str, src: str) -> pd.DataFrame:
        """
        内部方法：获取实时分笔成交数据（用于重试）
        """
        # 设置 tushare token（实时行情接口需要）
        if self.ts_token:
            ts.set_token(self.ts_token)
        return ts.realtime_tick(ts_code=ts_code, src=src)
    
    def fetch_realtime_tick(self, ts_code: str, src: str = 'sina') -> pd.DataFrame:
        """
        获取实时分笔成交数据（逐笔交易）
        
        注意：此接口使用 tushare 的旧版实时行情接口，需要设置 token。
        
        Args:
            ts_code: 股票代码（如 '000001.SZ'），必填
                - 仅支持单个股票代码
            src: 数据源（'sina' 新浪，'dc' 东方财富，默认 'sina'）
        
        Returns:
            包含实时分时数据的 DataFrame，包含以下字段：
            - time: 交易时间
            - price: 当前价格
            - change: 涨跌额
            - volume: 成交量（手）
            - amount: 成交额（元）
            - type: 交易类型（'买入'、'卖出'、'中性'）
        """
        if not self.ts_token:
            logger.error("Tushare Token 未设置，请设置 TUSHARE_TOKEN 环境变量")
            return pd.DataFrame()
        
        result = self.fetch_with_retry(self._fetch_realtime_tick_internal, ts_code, src)
        if result is not None and not result.empty:
            logger.info(f"成功获取 {ts_code} 实时成交数据，共 {len(result)} 条记录")
            return result
        return pd.DataFrame()
    
    def _fetch_realtime_list_internal(self, src: str) -> pd.DataFrame:
        """
        内部方法：获取实时涨跌幅排名列表（用于重试）
        """
        # 设置 tushare token（实时行情接口需要）
        if self.ts_token:
            ts.set_token(self.ts_token)
        return ts.realtime_list(src=src)
    
    def fetch_realtime_list(self, src: str = 'dc') -> pd.DataFrame:
        """
        获取实时涨跌幅排名列表（全市场实时数据）
        
        注意：此接口使用 tushare 的旧版实时行情接口，需要设置 token。
        
        Args:
            src: 数据源（'sina' 新浪，'dc' 东方财富，默认 'dc'）
        
        Returns:
            包含实时行情列表的 DataFrame，包含以下字段：
            - ts_code: 股票代码
            - name: 股票名称
            - price: 当前价格
            - pct_change: 涨跌幅
            - change: 涨跌额
            - volume: 成交量（sina为股，dc为手）
            - amount: 成交额（元）
            - swing: 振幅（仅 dc）
            - low: 今日最低价
            - high: 今日最高价
            - open: 今日开盘价
            - close: 今日收盘价
            - vol_ratio: 量比（仅 dc）
            - turnover_rate: 换手率（仅 dc）
            - pe: 市盈率（仅 dc）
            - pb: 市净率（仅 dc）
            - total_mv: 总市值（元，仅 dc）
            - float_mv: 流通市值（元，仅 dc）
            - rise: 涨速（仅 dc）
            - 5min: 5分钟涨跌（仅 dc）
            - 60day: 60日涨跌（仅 dc）
            - 1year: 1年涨跌（仅 dc）
            - buy: 买价（仅 sina）
            - sale: 卖价（仅 sina）
            - time: 当前时间（仅 sina）
        """
        if not self.ts_token:
            logger.error("Tushare Token 未设置，请设置 TUSHARE_TOKEN 环境变量")
            return pd.DataFrame()
        
        result = self.fetch_with_retry(self._fetch_realtime_list_internal, src)
        if result is not None and not result.empty:
            logger.info(f"成功获取实时涨跌幅排名列表，共 {len(result)} 条记录")
            return result
        return pd.DataFrame()
  

    # ==================== 同花顺概念板块相关接口 ====================

    def _fetch_ths_concept_list_internal(self) -> pd.DataFrame:
        """
        内部方法：获取同花顺概念板块列表（用于重试）
        """
        return self.pro.ths_index()

    def fetch_ths_concept_list(self) -> pd.DataFrame:
        """
        获取同花顺概念板块列表

        Returns:
            包含同花顺概念板块列表的 DataFrame，包含以下字段：
            - ts_code: 概念板块代码
            - name: 概念板块名称
            - type: 概念类型
            - market: 市场
        """
        if not self.pro:
            logger.error("Tushare API 未初始化，请设置 TUSHARE_TOKEN 环境变量")
            return pd.DataFrame()

        result = self.fetch_with_retry(self._fetch_ths_concept_list_internal)
        if result is not None and not result.empty:
            logger.info(f"成功获取同花顺概念板块列表，共 {len(result)} 条记录")
            return result
        return pd.DataFrame()

    def _fetch_ths_concept_daily_internal(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        内部方法：获取同花顺概念板块日线行情（用于重试）
        """
        return self.pro.concept_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)

    def fetch_ths_concept_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取同花顺概念板块日线行情

        Args:
            ts_code: 概念板块代码（如 '885588.TI'）
            start_date: 开始日期（格式：YYYYMMDD）
            end_date: 结束日期（格式：YYYYMMDD）

        Returns:
            包含概念板块日线行情的 DataFrame，包含以下字段：
            - ts_code: 概念板块代码
            - trade_date: 交易日期
            - close: 收盘点位
            - turnover: 成交额
            - turnover_rate: 换手率
            - change_pct: 涨跌幅
            - amount: 成交额（千元）
            - volume: 成交量（手）
        """
        if not self.pro:
            logger.error("Tushare API 未初始化，请设置 TUSHARE_TOKEN 环境变量")
            return pd.DataFrame()

        result = self.fetch_with_retry(self._fetch_ths_concept_daily_internal, ts_code, start_date, end_date)
        if result is not None and not result.empty:
            logger.info(f"成功获取 {ts_code} 概念板块日线行情，共 {len(result)} 条记录")
            return result
        return pd.DataFrame()

    def _fetch_ths_concept_members_internal(self, ts_code: str) -> pd.DataFrame:
        """
        内部方法：获取同花顺概念板块成分股（用于重试）
        """
        return self.pro.concept_member(ts_code=ts_code)

    def fetch_ths_concept_members(self, ts_code: str) -> pd.DataFrame:
        """
        获取同花顺概念板块成分股

        Args:
            ts_code: 概念板块代码（如 '885588.TI'）

        Returns:
            包含概念板块成分股的 DataFrame，包含以下字段：
            - ts_code: 概念板块代码
            - con_code: 成分股代码
            - con_name: 成分股名称
            - in_date: 纳入日期
            - out_date: 剔除日期
            - is_new: 是否新纳入
        """
        if not self.pro:
            logger.error("Tushare API 未初始化，请设置 TUSHARE_TOKEN 环境变量")
            return pd.DataFrame()

        result = self.fetch_with_retry(self._fetch_ths_concept_members_internal, ts_code)
        if result is not None and not result.empty:
            logger.info(f"成功获取 {ts_code} 概念板块成分股，共 {len(result)} 只")
            return result
        return pd.DataFrame()


if __name__ == "__main__":
    """
    测试 Tushare Fetcher 所有接口
    """
    import sys
    from datetime import datetime
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 创建 Mock Storage（用于测试）
    class MockStorage:
        def save(self, df, path):
            pass
    
    # 创建 Fetcher 实例
    fetcher = TushareFetcher(MockStorage())
    
    print("=" * 60)
    print("测试 Tushare Fetcher 所有接口")
    print("=" * 60)
    
    # # 测试1: 获取交易日历
    # print("\n[1] 测试获取交易日历...")
    # try:
    #     df_calendar = fetcher.fetch_trade_calendar()
    #     if not df_calendar.empty:
    #         print(f"✅ 交易日历获取成功，共 {len(df_calendar)} 条记录")
    #         print(f"   前3条记录：")
    #         print(df_calendar.head(3).to_string(index=False))
    #     else:
    #         print("⚠️  交易日历为空")
    # except Exception as e:
    #     print(f"❌ 交易日历获取失败: {e}")
    
    # # 测试2: 获取股票列表
    # print("\n[2] 测试获取股票列表...")
    # try:
    #     df_stock_list = fetcher.fetch_stock_list()
    #     if not df_stock_list.empty:
    #         print(f"✅ 股票列表获取成功，共 {len(df_stock_list)} 只股票")
    #         print(f"   前3条记录：")
    #         print(df_stock_list.head(3).to_string(index=False))
    #     else:
    #         print("⚠️  股票列表为空")
    # except Exception as e:
    #     print(f"❌ 股票列表获取失败: {e}")
    
    # # 测试3: 获取日线数据
    # print("\n[3] 测试获取日线数据（平安银行 000001.SZ）...")
    # try:
    #     df_daily = fetcher.fetch_daily_history(
    #         ts_code='000001.SZ',
    #         start_date='20240101',
    #         end_date='20240131'
    #     )
    #     if not df_daily.empty:
    #         print(f"✅ 日线数据获取成功，共 {len(df_daily)} 条记录")
    #         print(f"   前3条记录：")
    #         print(df_daily.head(3).to_string(index=False))
    #     else:
    #         print("⚠️  日线数据为空")
    # except Exception as e:
    #     print(f"❌ 日线数据获取失败: {e}")
    
    # # 测试4: 获取申万2021版行业分类
    # print("\n[4] 测试获取申万2021版行业分类...")
    # try:
    #     df_sw2021 = fetcher.fetch_sw2021_classification(level=0)
    #     if not df_sw2021.empty:
    #         print(f"✅ 申万2021版行业分类获取成功，共 {len(df_sw2021)} 条记录")
    #         print(f"   前3条记录：")
    #         print(df_sw2021.head(3).to_string(index=False))
    #     else:
    #         print("⚠️  申万2021版行业分类为空")
    # except Exception as e:
    #     print(f"❌ 申万2021版行业分类获取失败: {e}")
    
    # # 测试5: 获取申万2021版行业成分股
    # print("\n[5] 测试获取申万2021版行业成分股(遍历所有L3代码)...")
    # try:
    #     # 从申万2021版行业分类获取所有L3代码
    #     l1_codes = fetcher.fetch_sw2021_classification(level=1)['index_code'].tolist()

    #     df_components = pd.DataFrame()
    #     for l1_code in l1_codes:
    #         df_l3_components = fetcher.fetch_sw2021_components_by_code(l1_code=l1_code)
    #         df_components = pd.concat([df_components, df_l3_components], ignore_index=True)

    #     if not df_components.empty:
    #         print(f"✅ 申万2021版行业成分股获取成功，共 {len(df_components)} 只成分股")
    #         # 获取前3个不同的行业代码
    #         unique_indices = df_components['l1_code'].unique()[:3]
    #         for index_code in unique_indices:
    #             df_index_components = df_components[df_components['l1_code'] == index_code]
    #             print(f"\n行业代码: {index_code}，成分股数量: {len(df_index_components)}")
    #             print(df_index_components.head(3).to_string(index=False))
    #     else:
    #         print(f"⚠️  申万2021版行业成分股为空")
    # except Exception as e:
    #     print(f"❌ 申万2021版行业成分股获取失败: {e}")
    
    # # 测试6: 获取实时行情数据（单个股票）
    # print("\n[6] 测试获取实时行情数据（平安银行 000001.SZ）...")
    # try:
    #     df_realtime_quote = fetcher.fetch_realtime_quote(ts_code='000001.SZ', src='sina')
    #     if not df_realtime_quote.empty:
    #         print(f"✅ 实时行情数据获取成功，共 {len(df_realtime_quote)} 条记录")
    #         print(f"   记录详情：")
    #         print(df_realtime_quote.to_string(index=False))
    #     else:
    #         print("⚠️  实时行情数据为空")
    # except Exception as e:
    #     print(f"❌ 实时行情数据获取失败: {e}")
    
    # # 测试7: 获取实时成交数据
    # print("\n[7] 测试获取实时分笔成交数据（平安银行 000001.SZ）...")
    # try:
    #     df_realtime_tick = fetcher.fetch_realtime_tick(ts_code='000001.SZ', src='dc')
    #     if not df_realtime_tick.empty:
    #         print(f"✅ 实时成交数据获取成功，共 {len(df_realtime_tick)} 条记录")
    #         print(f"   前5条记录：")
    #         print(df_realtime_tick.head(5).to_string(index=False))
    #     else:
    #         print("⚠️  实时成交数据为空")
    # except Exception as e:
    #     print(f"❌ 实时成交数据获取失败: {e}")
    
    # # 测试8: 获取实时涨跌幅排名列表（仅获取前10条用于测试）
    # print("\n[8] 测试获取实时涨跌幅排名列表（前10条）...")
    # try:
    #     df_realtime_list = fetcher.fetch_realtime_list(src='sina')
    #     if not df_realtime_list.empty:
    #         print(f"✅ 实时涨跌幅排名列表获取成功，共 {len(df_realtime_list)} 条记录")
    #         print(f"   前10条记录：")
    #         print(df_realtime_list.head(10).to_string(index=False))
    #     else:
    #         print("⚠️  实时涨跌幅排名列表为空")
    # except Exception as e:
    #     print(f"❌ 实时涨跌幅排名列表获取失败: {e}")
    
    # 测试9: 获取同花顺概念板块列表
    print("\n[9] 测试获取同花顺概念板块列表...")
    try:
        df_ths_concept_list = fetcher.fetch_ths_concept_list()
        if not df_ths_concept_list.empty:
            print(f"✅ 同花顺概念板块列表获取成功，共 {len(df_ths_concept_list)} 条记录")
            print(f"   前5条记录：")
            print(df_ths_concept_list.head(5).to_string(index=False))
        else:
            print("⚠️  同花顺概念板块列表为空")
    except Exception as e:
        print(f"❌ 同花顺概念板块列表获取失败: {e}")
    
    # 测试10: 获取同花顺概念板块日线行情
    print("\n[10] 测试获取同花顺概念板块日线行情...")
    try:
        # 先获取一个概念板块代码用于测试
        df_concepts = fetcher.fetch_ths_concept_list()
        if not df_concepts.empty:
            test_concept_code = df_concepts.iloc[0]['ts_code']
            df_ths_concept_daily = fetcher.fetch_ths_concept_daily(
                ts_code=test_concept_code,
                start_date='20240101',
                end_date='20240131'
            )
            if not df_ths_concept_daily.empty:
                print(f"✅ 同花顺概念板块日线行情获取成功，共 {len(df_ths_concept_daily)} 条记录")
                print(f"   概念板块代码: {test_concept_code}")
                print(f"   前5条记录：")
                print(df_ths_concept_daily.head(5).to_string(index=False))
            else:
                print(f"⚠️  同花顺概念板块日线行情为空")
        else:
            print("⚠️  无法获取概念板块代码，跳过测试")
    except Exception as e:
        print(f"❌ 同花顺概念板块日线行情获取失败: {e}")
    
    # 测试11: 获取同花顺概念板块成分股
    print("\n[11] 测试获取同花顺概念板块成分股...")
    try:
        # 先获取一个概念板块代码用于测试
        df_concepts = fetcher.fetch_ths_concept_list()
        if not df_concepts.empty:
            test_concept_code = df_concepts.iloc[0]['ts_code']
            df_ths_concept_members = fetcher.fetch_ths_concept_members(ts_code=test_concept_code)
            if not df_ths_concept_members.empty:
                print(f"✅ 同花顺概念板块成分股获取成功，共 {len(df_ths_concept_members)} 只")
                print(f"   概念板块代码: {test_concept_code}")
                print(f"   前5条记录：")
                print(df_ths_concept_members.head(5).to_string(index=False))
            else:
                print(f"⚠️  同花顺概念板块成分股为空")
        else:
            print("⚠️  无法获取概念板块代码，跳过测试")
    except Exception as e:
        print(f"❌ 同花顺概念板块成分股获取失败: {e}")
    
    print("\n" + "=" * 60)
    print("测试完成！")
    print("=" * 60)
