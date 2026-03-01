import os
import json
import pandas as pd
import tushare as ts


#设置你的token，登录tushare在个人用户中心里拷贝
ts.set_token('da8b0417e689bd72a3e5b3312fb6094f9405b06b667f7033eebecf83')

def get_realtime_quote(ts_code: str, src: str = 'dc') -> pd.DataFrame:
    """
    获取实时行情数据
    :param ts_code: 股票代码，格式为 '600000.SH' 或 '000001.SZ'
    :param src: 数据源，默认为 'sina'，可选 'dc'（东财）
    :return: 包含实时行情数据的DataFrame
    """
    if src == 'sina':
        return ts.realtime_quote(ts_code=ts_code, src='sina')
    elif src == 'dc':
        return ts.realtime_quote(ts_code=ts_code, src='dc')
    else:
        raise ValueError(f"不支持的数据源: {src}")

def get_realtime_daily(ts_code: str, src: str = 'dc') -> pd.DataFrame:
    """
    获取实时日线数据
    :param ts_code: 股票代码，格式为 '600000.SH' 或 '000001.SZ'
    :param src: 数据源，默认为 'sina'，可选 'dc'（东财）
    :return: 包含实时日线数据的DataFrame
    """
    # 获取当前交易日
    trade_calendar_info_path = "/root/.openclaw/workspace/data/raw/trade_calendar_info.json"
    if not os.path.exists(trade_calendar_info_path):
        raise FileNotFoundError(f"交易日历信息文件不存在: {trade_calendar_info_path}")
    with open(trade_calendar_info_path, 'r', encoding='utf-8') as f:
        trade_calendar_info = json.load(f)
    current_trade_day = trade_calendar_info.get("current_trade_day")
    if not current_trade_day:
        raise ValueError("交易日历信息中缺少 current_trade_day 字段")
    current_trade_day = str(current_trade_day)
    
    # 只返回最近2个月的日线数据，避免数据量过大
    start_date = pd.to_datetime(current_trade_day) - pd.DateOffset(months=2)
    start_date_str = start_date.strftime('%Y%m%d')
    
    history_data_path = f"/root/.openclaw/workspace/data/raw/stock_daily/{ts_code}.csv"
    history_data_df = pd.read_csv(history_data_path, dtype={'trade_date': str})
    history_data_df['trade_date'] = history_data_df['trade_date'].astype(str)
    lastest_history_date = history_data_df['trade_date'].max()
    if lastest_history_date >= current_trade_day:
        print(f"{ts_code} 的历史数据已包含当前交易日 {current_trade_day}，无需获取实时日线数据")
        return history_data_df[history_data_df['trade_date'] >= start_date_str]
    

    if src == 'dc':
        realtime_quote_df = get_realtime_quote(ts_code, src)
        if realtime_quote_df.empty:
            raise ValueError(f"未能获取到实时行情数据: {ts_code}")
        remap_df = pd.DataFrame({
            'ts_code': realtime_quote_df['TS_CODE'],
            'trade_date': realtime_quote_df['DATE'],
            'open': realtime_quote_df['OPEN'],
            'high': realtime_quote_df['HIGH'],
            'low': realtime_quote_df['LOW'],
            'close': realtime_quote_df['PRICE'],
            'pre_close': realtime_quote_df['PRE_CLOSE'],
            'vol': realtime_quote_df['VOLUME'],
            'amount': realtime_quote_df['AMOUNT'],
            })
        # change和pct_chg无法从东财实时行情数据中获取，因此*需要根据当前价和昨日收盘价计算
        remap_df['change'] = remap_df['close'] - remap_df['pre_close']
        remap_df['pct_chg'] = remap_df['change'] / remap_df['pre_close'] * 100
    else:
        raise ValueError(f"不支持的数据源: {src}")
    
    # 合并历史数据和实时数据
    merged_df = pd.concat([history_data_df, remap_df], ignore_index=True)
    merged_df['trade_date'] = merged_df['trade_date'].astype(str)
    merged_df = merged_df[merged_df['trade_date'] >= start_date_str]
    merged_df = merged_df.sort_values(by='trade_date', ascending=False).reset_index(drop=True)
    return merged_df

if __name__ == "__main__":
    ts_code = '600000.SH'
    realtime_daily_df = get_realtime_daily(ts_code)
    print(realtime_daily_df)

    # realtime_quote_df = get_realtime_quote(ts_code)
    # print(realtime_quote_df)