# -*- coding: utf-8 -*-
"""
Pure BULAO (不落浪) golden/dead cross backtest strategy for rqalpha.

Buy:  BULAO golden cross (X1 crosses above X2)
Sell: BULAO dead cross  (X1 crosses below X2)

No Chan Lun involved. Simple momentum-following strategy.

Usage:
    python run_backtest.py -f strategy_bulao.py -s 2024-06-01 -e 2026-04-10
"""

import sys
import os
import logging
import numpy as np
import pandas as pd

# Add indicator module paths
sys.path.insert(0, '/root/.openclaw/workspace/finance_tool/analyze/metric_tdx')

from MyTT import EMA, MA, REF
from backset import BACKSET

# Provide a module-level logger for use outside rqalpha context
_logger = logging.getLogger("strategy_bulao")
logging.basicConfig(level=logging.INFO, format="%(name)s - %(message)s")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Board prefixes
_MAIN_BOARD_PREFIXES = (
    "000", "001", "002", "003",  # SZ main board
    "600", "601", "603", "605",  # SH main board
)
_GEM_PREFIX = "30"          # 创业板 (Growth Enterprise Market)
_STAR_PREFIX = "68"         # 科创板 (STAR Market)

# Limit-up thresholds (percentage, accounting for rounding)
_LIMIT_UP_MAIN = 9.9        # 主板 10% 涨停 (>=9.9 to handle rounding)
_LIMIT_UP_GEM_STAR = 19.9   # 创业板/科创板 20% 涨停 (>=19.9)

# File paths
_DAILY_DIR = "/root/.openclaw/workspace/data/raw/stock_daily"
_STOCK_LIST_CSV = "/root/.openclaw/workspace/data/raw/stock_list.csv"
_TRADE_CALENDAR_CSV = "/root/.openclaw/workspace/data/raw/trade_calendar.csv"


# ---------------------------------------------------------------------------
# Stock pool helpers
# ---------------------------------------------------------------------------

def _load_main_board_stocks_basic():
    """Load all main-board non-ST stocks (basic filter, no personality sorting).

    Returns list of rqalpha order_book_id strings.
    """
    df = pd.read_csv(_STOCK_LIST_CSV, dtype=str, encoding="utf-8-sig")

    codes = df["ts_code"].str.split(".", expand=True)[0]
    names = df.get("name", pd.Series([""] * len(df)))
    is_main = codes.apply(
        lambda c: isinstance(c, str) and c.startswith(_MAIN_BOARD_PREFIXES)
    )
    is_not_st = ~names.str.upper().str.contains("ST", na=False)
    df = df[is_main & is_not_st].copy()

    stocks = []
    for ts_code in df["ts_code"]:
        code, exchange = ts_code.split(".")
        rq_exchange = "XSHE" if exchange == "SZ" else "XSHG"
        stocks.append(f"{code}.{rq_exchange}")
    return stocks


def _rqalpha_to_tscode(order_book_id):
    """Convert rqalpha order_book_id to tushare ts_code.
    e.g. '002471.XSHE' -> '002471.SZ', '600373.XSHG' -> '600373.SH'
    """
    code, exchange = order_book_id.split(".")
    ts_exchange = "SZ" if exchange == "XSHE" else "SH"
    return f"{code}.{ts_exchange}"


def _tscode_to_rqalpha(ts_code):
    """Convert tushare ts_code to rqalpha order_book_id.
    e.g. '002471.SZ' -> '002471.XSHE', '600373.SH' -> '600373.XSHG'
    """
    code, exchange = ts_code.split(".")
    rq_exchange = "XSHE" if exchange == "SZ" else "XSHG"
    return f"{code}.{rq_exchange}"


def _get_limit_up_threshold(code):
    """Get limit-up threshold based on stock code prefix."""
    if code.startswith(_GEM_PREFIX) or code.startswith(_STAR_PREFIX):
        return _LIMIT_UP_GEM_STAR
    return _LIMIT_UP_MAIN


# ---------------------------------------------------------------------------
# Precompute monthly stock pools (dynamic rolling, no look-ahead bias)
# ---------------------------------------------------------------------------

def _load_trade_calendar():
    """Load trade calendar, return sorted list of trading dates (YYYYMMDD strings)."""
    df = pd.read_csv(_TRADE_CALENDAR_CSV, dtype=str, encoding="utf-8-sig")
    df = df[df["is_open"] == "1"]
    dates = sorted(df["cal_date"].tolist())
    return dates


def _precompute_monthly_pools(start_date, end_date, all_stocks_rqalpha):
    """Precompute monthly stock pools based on rolling 252-day limit-up counts.

    Args:
        start_date: backtest start date, e.g. '2024-06-03' or '20240603'
        end_date: backtest end date, e.g. '2026-04-09' or '20260409'
        all_stocks_rqalpha: list of rqalpha order_book_id strings

    Returns:
        dict: {YYYYMM -> [order_book_id_list]}  top 500 stocks by limit-up count
    """
    # Normalize dates to YYYYMMDD format
    start_yyyymmdd = start_date.replace("-", "")
    end_yyyymmdd = end_date.replace("-", "")

    # Load trade calendar (sorted ascending)
    trade_dates = _load_trade_calendar()

    # Build lookup: date -> index for fast binary search
    td_list = trade_dates  # already sorted ascending

    # Map ts_code -> csv cache (lazy load)
    csv_cache = {}

    def load_stock_csv(ts_code):
        if ts_code not in csv_cache:
            fpath = os.path.join(_DAILY_DIR, f"{ts_code}.csv")
            if os.path.isfile(fpath):
                try:
                    df = pd.read_csv(fpath, dtype=str, encoding="utf-8-sig")
                    # CSV is reverse-ordered (newest first); sort ascending
                    df = df.sort_values("trade_date").reset_index(drop=True)
                    df["pct_chg"] = pd.to_numeric(df["pct_chg"], errors="coerce")
                    df["trade_date"] = df["trade_date"].astype(str)
                    csv_cache[ts_code] = df
                except Exception:
                    csv_cache[ts_code] = None
            else:
                csv_cache[ts_code] = None
        return csv_cache[ts_code]

    # Determine all months in backtest range
    # We need pools for the first month AND subsequent months
    start_dt = pd.Timestamp(start_yyyymmdd)
    end_dt = pd.Timestamp(end_yyyymmdd)
    months = pd.date_range(start=start_dt, end=end_dt, freq="MS")  # Month Start

    # Ensure the first backtest month is included (MS skips the start month
    # if start_dt is not the 1st of the month)
    start_ym = start_yyyymmdd[:6]
    if months.empty or months[0].strftime("%Y%m") != start_ym:
        months = pd.DatetimeIndex([pd.Timestamp(start_ym + "01")]).append(months)

    monthly_pools = {}

    # Build a mapping: YYYYMM -> first trading day of that month
    first_td_per_month = {}
    for td in td_list:
        ym = td[:6]
        if ym not in first_td_per_month:
            first_td_per_month[ym] = td

    _logger.info(f"Precomputing monthly pools for {len(months)} months, "
                 f"{len(all_stocks_rqalpha)} candidate stocks...")

    for month_start in months:
        ym = month_start.strftime("%Y%m")

        # Find the first trading day of this month (or after month_start)
        first_td = first_td_per_month.get(ym)
        if first_td is None:
            # No trading days in this month (unlikely but handle)
            continue

        # Find the index of first_td in the sorted trade_dates list
        try:
            cutoff_idx = td_list.index(first_td)
        except ValueError:
            # Use binary search as fallback
            import bisect
            cutoff_idx = bisect.bisect_left(td_list, first_td)

        # Look back 252 trading days (strictly BEFORE cutoff)
        lookback_start_idx = max(0, cutoff_idx - 252)
        lookback_start_date = td_list[lookback_start_idx]
        # The data window is [lookback_start_date, td_list[cutoff_idx - 1]]
        # i.e., up to the trading day just before this month begins
        if cutoff_idx > 0:
            lookback_end_date = td_list[cutoff_idx - 1]
        else:
            continue  # No historical data available

        # Count limit-ups for each stock in the lookback window
        stock_scores = []
        for rq_id in all_stocks_rqalpha:
            ts_code = _rqalpha_to_tscode(rq_id)
            df = load_stock_csv(ts_code)
            if df is None or df.empty:
                continue

            # Filter to lookback window
            mask = (df["trade_date"] >= lookback_start_date) & \
                   (df["trade_date"] <= lookback_end_date)
            window = df.loc[mask]

            if window.empty:
                continue

            # Determine threshold from code prefix
            code = ts_code.split(".")[0]
            threshold = _get_limit_up_threshold(code)

            # Count limit-up days
            limit_up_count = int((window["pct_chg"] >= threshold).sum())

            if limit_up_count > 0:
                stock_scores.append((rq_id, limit_up_count))

        # Sort by limit_up_count descending, take top 500
        stock_scores.sort(key=lambda x: x[1], reverse=True)
        top500 = [s[0] for s in stock_scores[:500]]

        monthly_pools[ym] = top500
        _logger.info(f"  Month {ym}: pool={len(top500)} stocks, "
                     f"cutoff={first_td}, "
                     f"lookback=[{lookback_start_date}..{lookback_end_date}]")

    _logger.info(f"Precomputed {len(monthly_pools)} monthly pools.")
    return monthly_pools


# ---------------------------------------------------------------------------
# BULAO indicator computation
# ---------------------------------------------------------------------------

def compute_bulao(C, H, L):
    """
    Compute BULAO golden cross and dead cross from C, H, L numpy arrays.

    Returns:
        (golden_cross, dead_cross): tuple of boolean numpy arrays
    """
    WY1001 = (2 * C + H + L) / 4
    WY1002 = EMA(WY1001, 3)
    WY1003 = EMA(WY1002, 3)
    WY1004 = EMA(WY1003, 3)
    XYS0 = (WY1004 - REF(WY1004, 1)) / REF(WY1004, 1) * 100
    X1 = MA(XYS0, 1)
    X2 = MA(XYS0, 2)
    golden_cross = (X1 > X2) & (REF(X1, 1) <= REF(X2, 1))
    dead_cross = (X1 < X2) & (REF(X1, 1) >= REF(X2, 1))
    return golden_cross, dead_cross


# ---------------------------------------------------------------------------
# Strategy configuration
# ---------------------------------------------------------------------------

LOOKBACK = 80            # BULAO needs ~20 bars warm-up, 80 is comfortable
MAX_POSITIONS = 5        # max concurrent positions
MAX_HOLD_DAYS = 10       # safety exit: max holding period

# Backtest date range (used for precomputing monthly pools)
# These can be overridden via environment variables RQ_START / RQ_END
# or set by run_backtest.py before loading the strategy
BACKTEST_START = os.environ.get("RQ_START", "20240603")
BACKTEST_END = os.environ.get("RQ_END", "20260409")


# ---------------------------------------------------------------------------
# rqalpha strategy
# ---------------------------------------------------------------------------

def init(context):
    context.lookback = LOOKBACK
    context.max_positions = MAX_POSITIONS
    context.entry_bar = {}
    context.bar_count = 0

    # Load all main-board non-ST stocks (basic filter)
    all_stocks = _load_main_board_stocks_basic()
    logger.info(f"Basic stock pool: {len(all_stocks)} main-board non-ST stocks")

    # Precompute monthly pools (dynamic rolling, no look-ahead bias)
    context.monthly_pools = _precompute_monthly_pools(
        BACKTEST_START, BACKTEST_END, all_stocks
    )

    context.current_pool = []
    context.current_month = None


def _is_limit_up(stock):
    """Check if a stock is at limit-up today using history_bars.

    Uses 2-day close comparison to compute pct change.
    Returns True if the stock is at limit-up price.
    """
    code = stock.split(".")[0]
    threshold = _get_limit_up_threshold(code)

    try:
        # Try to get 2 bars of close to compute pct change
        bars = history_bars(stock, 2, "1d", ["close"])
    except Exception:
        return False

    if bars is None or len(bars) < 2:
        return False

    today_close = float(bars["close"][-1])
    yesterday_close = float(bars["close"][-2])

    if yesterday_close <= 0:
        return False

    pct = (today_close / yesterday_close - 1) * 100
    return pct >= threshold


def handle_bar(context, bar_dict):
    context.bar_count += 1

    # Need warm-up before any signal is valid
    if context.bar_count < 5:
        return

    # --- Monthly pool rotation ---
    current_month = context.now.strftime("%Y%m")
    if current_month != context.current_month:
        new_pool = context.monthly_pools.get(current_month)
        if new_pool is not None:
            context.current_pool = new_pool
            logger.info(f"Month rotated to {current_month}: "
                        f"pool={len(context.current_pool)} stocks")
        else:
            # Fallback: keep previous pool if no data for this month
            logger.info(f"Month rotated to {current_month}: "
                        f"no precomputed pool, keeping {len(context.current_pool)} stocks")
        context.current_month = current_month

    # Use current_pool as the stock universe; fallback to empty
    stock_universe = context.current_pool

    # --- Compute current holdings ---
    held_stocks = [
        s for s in context.portfolio.positions
        if context.portfolio.positions[s].quantity > 0
    ]
    current_held = len(held_stocks)

    # --- Progress logging ---
    if context.bar_count % 50 == 0:
        logger.info(f"Bar #{context.bar_count} | held={current_held} | "
                    f"universe={len(stock_universe)}")

    # --- Sell logic: dead cross OR max hold days ---
    # NOTE: Sell logic does NOT filter by limit-up (spec requirement)
    for stock in held_stocks:
        try:
            bars = history_bars(stock, context.lookback, "1d", ["close", "high", "low"])
        except Exception:
            continue
        if bars is None or len(bars) < 20:
            continue

        C = bars["close"].astype(np.float64)
        H = bars["high"].astype(np.float64)
        L = bars["low"].astype(np.float64)

        _, dead_cross = compute_bulao(C, H, L)

        should_sell = False
        if dead_cross[-1]:
            should_sell = True

        # Safety: force sell after MAX_HOLD_DAYS
        entry = context.entry_bar.get(stock)
        if entry is not None and (context.bar_count - entry) >= MAX_HOLD_DAYS:
            should_sell = True

        if should_sell:
            order_target_percent(stock, 0)
            context.entry_bar.pop(stock, None)

    # --- Recompute holdings after sells ---
    current_held = sum(
        1 for s in context.portfolio.positions
        if context.portfolio.positions[s].quantity > 0
    )
    available_slots = context.max_positions - current_held
    if available_slots <= 0:
        return

    # --- Buy logic: golden cross scan ---
    buy_candidates = []
    for stock in stock_universe:
        # Skip already held
        if stock in held_stocks:
            continue

        try:
            bars = history_bars(stock, context.lookback, "1d", ["close", "high", "low"])
        except Exception:
            continue
        if bars is None or len(bars) < 20:
            continue

        C = bars["close"].astype(np.float64)
        H = bars["high"].astype(np.float64)
        L = bars["low"].astype(np.float64)

        golden_cross, _ = compute_bulao(C, H, L)

        if golden_cross[-1]:
            # --- Limit-up filter: skip if stock is at limit-up ---
            if _is_limit_up(stock):
                continue
            buy_candidates.append(stock)

    # Buy up to available slots (equal weight)
    for stock in buy_candidates[:available_slots]:
        weight = 1.0 / context.max_positions
        order_target_percent(stock, weight)
        context.entry_bar[stock] = context.bar_count
