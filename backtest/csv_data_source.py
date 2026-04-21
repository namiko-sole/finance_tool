# -*- coding: utf-8 -*-
"""
CSVDataSource - Load local tushare-format CSV stock data into rqalpha.

Data layout expected:
  stock_daily/000001.SZ.csv   (ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount)
  stock_list.csv              (ts_code,symbol,name,area,industry,...,market,list_date,...,exchange)
  trade_calendar.csv          (exchange,cal_date,is_open,pretrade_date)

Optional (enable richer backtesting when present):
  adj_factor/000001.SZ.csv    (ts_code,trade_date,adj_factor)
  suspend_d.csv               (ts_code,trade_date,suspend_timing,suspend_type)
  namechange.csv              (ts_code,name,start_date,end_date,ann_date,change_reason)
"""

import os
import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from rqalpha.const import EXCHANGE, INSTRUMENT_TYPE, TRADING_CALENDAR_TYPE
from rqalpha.interface import AbstractDataSource, ExchangeRate
from rqalpha.model.instrument import Instrument
from rqalpha.utils.datetime_func import convert_date_to_int

# numpy structured array dtype matching rqalpha's expectations
BAR_DTYPE = np.dtype([
    ("datetime", np.uint64),
    ("open", np.float64),
    ("close", np.float64),
    ("high", np.float64),
    ("low", np.float64),
    ("volume", np.float64),
    ("total_turnover", np.float64),
])

# Tushare -> rqalpha exchange mapping
_EXCHANGE_MAP = {
    "SZ": EXCHANGE.XSHE,
    "SH": EXCHANGE.XSHG,
    "BJ": EXCHANGE.XSHE,
}

# Tushare market field -> board_type
_BOARD_TYPE_MAP = {
    "主板": "MainBoard",
    "创业板": "GEM",
    "科创板": "KSH",
    "北交所": "BJS",
}

# change_reason values that indicate ST status
_ST_REASONS = {"ST", "*ST", "SST", "S*ST"}


def _ts_code_to_order_book_id(ts_code: str) -> str:
    """000001.SZ -> 000001.XSHE"""
    code, exchange = ts_code.split(".")
    rq_exchange = "XSHE" if exchange == "SZ" else "XSHG"
    return f"{code}.{rq_exchange}"


def _order_book_id_to_ts_code(order_book_id: str) -> str:
    """000001.XSHE -> 000001.SZ"""
    code, exchange = order_book_id.split(".")
    ts_exchange = "SZ" if exchange == "XSHE" else "SH"
    return f"{code}.{ts_exchange}"


def _parse_date_int(date_str: str) -> int:
    """'20260402' -> 20260402000000 (rqalpha datetime int format)"""
    return int(date_str) * 1000000


def _parse_list_date(raw: str) -> str:
    """'19910403' -> '1991-04-03'"""
    if not raw or len(raw) < 8:
        return "1990-01-01"
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"


class CSVDataSource(AbstractDataSource):
    """
    rqalpha DataSource backed by local tushare-format CSV files.

    Loads all stock daily data into memory at init time.
    Supports stock (CS) daily backtesting with optional:
      - Price adjustment via adj_factor
      - Suspension detection via suspend_d
      - ST status detection via namechange
    """

    def __init__(
        self,
        data_dir: str,
        stock_list_path: str,
        trade_calendar_path: str,
    ):
        self._data_dir = data_dir
        self._instruments: Dict[str, Instrument] = {}
        self._bar_data: Dict[str, np.ndarray] = {}
        self._adj_factor: Dict[str, np.ndarray] = {}  # order_book_id -> sorted array of (date_int, factor)
        self._suspended_dates: Dict[str, set] = {}     # order_book_id -> set of date strings "YYYYMMDD"
        self._st_date_ranges: Dict[str, List[Tuple[int, int]]] = {}  # order_book_id -> [(start, end)]
        self._trading_dates: Optional[pd.DatetimeIndex] = None
        self._min_date: datetime.date = datetime.date.max
        self._max_date: datetime.date = datetime.date.min

        self._load_stock_list(stock_list_path)
        self._load_index_list(os.path.join(os.path.dirname(data_dir), "index_list.csv"))
        self._load_trade_calendar(trade_calendar_path)
        self._load_stock_data(data_dir)

        # Optional enhancements - gracefully skip if files don't exist
        adj_dir = os.path.join(os.path.dirname(data_dir), "adj_factor")
        self._load_adj_factor(adj_dir)

        suspend_path = os.path.join(os.path.dirname(data_dir), "suspend_d.csv")
        self._load_suspend_data(suspend_path)

        namechange_path = os.path.join(os.path.dirname(data_dir), "namechange.csv")
        self._load_st_data(namechange_path)

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _load_stock_list(self, path: str) -> None:
        df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
        for _, row in df.iterrows():
            ts_code = row["ts_code"]
            code, exchange_suffix = ts_code.split(".")
            order_book_id = _ts_code_to_order_book_id(ts_code)

            exchange = _EXCHANGE_MAP.get(exchange_suffix, EXCHANGE.XSHE)

            market = row.get("market", "主板")
            board_type = _BOARD_TYPE_MAP.get(market, "MainBoard")

            round_lot = 1 if board_type == "KSH" else 100

            listed_date = _parse_list_date(row.get("list_date", ""))
            de_listed_date = "2999-12-31"

            # Detect current ST status from name
            name = row.get("name", code)
            special_type = "ST" if "ST" in name.upper() else "Normal"

            instrument_dict = {
                "order_book_id": order_book_id,
                "symbol": name,
                "round_lot": round_lot,
                "listed_date": listed_date,
                "de_listed_date": de_listed_date,
                "type": "CS",
                "exchange": exchange,
                "board_type": board_type,
                "market_tplus": 1,
                "status": "Active",
                "special_type": special_type,
                "sector_code_name": row.get("industry", ""),
            }
            self._instruments[order_book_id] = Instrument(instrument_dict)

    def _load_index_list(self, path: str) -> None:
        """Load index instruments from a separate index_list.csv.

        Index CSV has different columns from stock_list (e.g. base_date,
        base_point, category) and is kept separate to avoid polluting the
        stock pool.  The file is optional — if missing, indices are simply
        not available as benchmarks.
        """
        if not os.path.isfile(path):
            return
        df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
        for _, row in df.iterrows():
            ts_code = row["ts_code"]
            code, exchange_suffix = ts_code.split(".")
            order_book_id = _ts_code_to_order_book_id(ts_code)

            exchange = _EXCHANGE_MAP.get(exchange_suffix, EXCHANGE.XSHG)

            listed_date = _parse_list_date(row.get("list_date", "") or row.get("base_date", ""))
            de_listed_date = "2999-12-31"

            instrument_dict = {
                "order_book_id": order_book_id,
                "symbol": row.get("name", code),
                "round_lot": 1,
                "listed_date": listed_date,
                "de_listed_date": de_listed_date,
                "type": "CS",          # rqalpha treats indices as CS for benchmark purposes
                "exchange": exchange,
                "board_type": "MainBoard",
                "market_tplus": 1,
                "status": "Active",
                "special_type": "Normal",
                "sector_code_name": row.get("category", "指数"),
            }
            self._instruments[order_book_id] = Instrument(instrument_dict)

    def _load_trade_calendar(self, path: str) -> None:
        df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
        trading = df[df["is_open"] == "1"]["cal_date"].tolist()
        dates = pd.to_datetime(trading, format="%Y%m%d")
        self._trading_dates = pd.DatetimeIndex(sorted(dates))

    def _load_stock_data(self, data_dir: str) -> None:
        loaded = 0
        skipped = 0
        for fname in os.listdir(data_dir):
            if not fname.endswith(".csv"):
                continue
            ts_code = fname[:-4]
            order_book_id = _ts_code_to_order_book_id(ts_code)

            if order_book_id not in self._instruments:
                skipped += 1
                continue

            fpath = os.path.join(data_dir, fname)
            df = pd.read_csv(fpath, dtype=str, encoding="utf-8-sig")
            if df.empty:
                continue

            n = len(df)
            bars = np.empty(n, dtype=BAR_DTYPE)
            bars["datetime"] = df["trade_date"].apply(_parse_date_int).values.astype(np.uint64)
            bars["open"] = pd.to_numeric(df["open"], errors="coerce").values.astype(np.float64)
            bars["high"] = pd.to_numeric(df["high"], errors="coerce").values.astype(np.float64)
            bars["low"] = pd.to_numeric(df["low"], errors="coerce").values.astype(np.float64)
            bars["close"] = pd.to_numeric(df["close"], errors="coerce").values.astype(np.float64)
            bars["volume"] = pd.to_numeric(df["vol"], errors="coerce").values.astype(np.float64)
            bars["total_turnover"] = pd.to_numeric(df["amount"], errors="coerce").values.astype(np.float64)

            bars.sort(order="datetime")

            first_date_str = df["trade_date"].iloc[-1]
            last_date_str = df["trade_date"].iloc[0]
            first_date = datetime.date(
                int(first_date_str[:4]), int(first_date_str[4:6]), int(first_date_str[6:8])
            )
            last_date = datetime.date(
                int(last_date_str[:4]), int(last_date_str[4:6]), int(last_date_str[6:8])
            )
            self._min_date = min(self._min_date, first_date)
            self._max_date = max(self._max_date, last_date)

            self._bar_data[order_book_id] = bars
            loaded += 1

        print(f"[CSVDataSource] Loaded {loaded} stocks, skipped {skipped}, "
              f"data range: {self._min_date} ~ {self._max_date}")

    def _load_adj_factor(self, adj_dir: str) -> None:
        """Load per-stock adj_factor CSVs. Gracefully skip if directory missing."""
        if not os.path.isdir(adj_dir):
            print(f"[CSVDataSource] adj_factor dir not found ({adj_dir}), price adjustment disabled")
            return

        count = 0
        for fname in os.listdir(adj_dir):
            if not fname.endswith(".csv"):
                continue
            ts_code = fname[:-4]
            order_book_id = _ts_code_to_order_book_id(ts_code)

            fpath = os.path.join(adj_dir, fname)
            df = pd.read_csv(fpath, dtype=str, encoding="utf-8-sig")
            if df.empty or "adj_factor" not in df.columns:
                continue

            # Build sorted array of (date_int, factor_float)
            dates = df["trade_date"].apply(_parse_date_int).values.astype(np.int64)
            factors = pd.to_numeric(df["adj_factor"], errors="coerce").fillna(1.0).values.astype(np.float64)
            combined = np.column_stack([dates, factors])
            # Sort by date ascending
            combined = combined[combined[:, 0].argsort()]
            self._adj_factor[order_book_id] = combined
            count += 1

        print(f"[CSVDataSource] Loaded adj_factor for {count} stocks")

    def _load_suspend_data(self, suspend_path: str) -> None:
        """Load suspend_d.csv into per-stock suspended date sets."""
        if not os.path.isfile(suspend_path):
            print(f"[CSVDataSource] suspend_d.csv not found, suspension detection disabled")
            return

        df = pd.read_csv(suspend_path, dtype=str, encoding="utf-8-sig")
        if df.empty:
            return

        # Only keep rows where suspend_type == "S" (suspended, not resumed)
        df_suspended = df[df["suspend_type"] == "S"]
        for _, row in df_suspended.iterrows():
            ts_code = row["ts_code"]
            order_book_id = _ts_code_to_order_book_id(ts_code)
            trade_date = row["trade_date"]
            if order_book_id not in self._suspended_dates:
                self._suspended_dates[order_book_id] = set()
            self._suspended_dates[order_book_id].add(trade_date)

        total_dates = sum(len(v) for v in self._suspended_dates.values())
        print(f"[CSVDataSource] Loaded suspend data: {len(self._suspended_dates)} stocks, "
              f"{total_dates} suspended date records")

    def _load_st_data(self, namechange_path: str) -> None:
        """Load namechange.csv and build per-stock ST date ranges."""
        if not os.path.isfile(namechange_path):
            print(f"[CSVDataSource] namechange.csv not found, ST detection using current name only")
            return

        df = pd.read_csv(namechange_path, dtype=str, encoding="utf-8-sig")
        if df.empty:
            return

        # Build ST date ranges per stock
        # namechange has: ts_code, name, start_date, end_date, change_reason
        # ST status is active when change_reason is in _ST_REASONS
        st_rows = df[df["change_reason"].isin(_ST_REASONS)]
        for _, row in st_rows.iterrows():
            ts_code = row["ts_code"]
            order_book_id = _ts_code_to_order_book_id(ts_code)
            start_str = row.get("start_date", "")
            end_str = row.get("end_date", None)

            if not start_str or len(start_str) < 8:
                continue

            start_int = int(start_str)
            # end_date None/NaN means still active, use a far future date
            if end_str and not pd.isna(end_str) and isinstance(end_str, str) and len(end_str) >= 8 and end_str != "None":
                end_int = int(end_str)
            else:
                end_int = 29991231

            if order_book_id not in self._st_date_ranges:
                self._st_date_ranges[order_book_id] = []
            self._st_date_ranges[order_book_id].append((start_int, end_int))

        print(f"[CSVDataSource] Loaded ST data: {len(self._st_date_ranges)} stocks with ST history")

    def _get_adj_factor_for_date(self, order_book_id: str, date_int: int) -> float:
        """Get the adj_factor value for a given stock on a given date."""
        factors = self._adj_factor.get(order_book_id)
        if factors is None or len(factors) == 0:
            return 1.0
        # Binary search for the date
        pos = np.searchsorted(factors[:, 0], date_int, side="right") - 1
        if pos < 0:
            return 1.0
        return float(factors[pos, 1])

    def _apply_adjustment(self, bars: np.ndarray, order_book_id: str, adjust_type: str,
                          adjust_orig_dt_int: int = None) -> np.ndarray:
        """Apply price adjustment to bar data based on adj_factor."""
        if adjust_type == "none" or not self._adj_factor:
            return bars

        factors = self._adj_factor.get(order_book_id)
        if factors is None or len(factors) == 0:
            return bars

        # Compute ex_cum_factor for each bar: ratio of target_date_factor / bar_date_factor
        # For "pre" (前复权): adjust all historical prices to be comparable to the latest price
        # For "post" (后复权): adjust all prices to be comparable to the earliest price
        bars_copy = bars.copy()
        price_fields = ["open", "high", "low", "close"]

        for i in range(len(bars_copy)):
            bar_date_int = int(bars_copy[i]["datetime"] // 1000000)
            bar_factor = self._get_adj_factor_for_date(order_book_id, bar_date_int)

            if adjust_type == "pre":
                # Target: adjust_orig (default: latest)
                if adjust_orig_dt_int:
                    target_factor = self._get_adj_factor_for_date(order_book_id, adjust_orig_dt_int)
                else:
                    target_factor = self._get_adj_factor_for_date(order_book_id, int(bars_copy[-1]["datetime"] // 1000000))
                ratio = target_factor / bar_factor if bar_factor != 0 else 1.0
            elif adjust_type == "post":
                # Target: earliest date
                earliest_factor = self._get_adj_factor_for_date(order_book_id, int(bars_copy[0]["datetime"] // 1000000))
                ratio = bar_factor / earliest_factor if earliest_factor != 0 else 1.0
            else:
                ratio = 1.0

            for field in price_fields:
                bars_copy[i][field] = bars_copy[i][field] * ratio

        return bars_copy

    # ------------------------------------------------------------------
    # AbstractDataSource implementation
    # ------------------------------------------------------------------

    def get_instruments(
        self,
        id_or_syms: Optional[Iterable[str]] = None,
        types: Optional[Iterable[INSTRUMENT_TYPE]] = None,
    ) -> Iterable[Instrument]:
        if id_or_syms is not None:
            id_set = set(id_or_syms)
            return [ins for obid, ins in self._instruments.items() if obid in id_set or ins.symbol in id_set]
        if types is not None:
            type_set = set(types)
            return [ins for ins in self._instruments.values() if ins.type in type_set]
        return list(self._instruments.values())

    def get_trading_calendars(self) -> Dict[TRADING_CALENDAR_TYPE, pd.DatetimeIndex]:
        return {TRADING_CALENDAR_TYPE.CN_STOCK: self._trading_dates}

    def get_bar(self, instrument, dt, frequency):
        if frequency != "1d":
            return None
        obid = instrument.order_book_id
        bars = self._bar_data.get(obid)
        if bars is None:
            return None
        dt_int = np.uint64(convert_date_to_int(dt))
        pos = np.searchsorted(bars["datetime"], dt_int)
        if pos < len(bars) and bars[pos]["datetime"] == dt_int:
            return bars[pos]
        return None

    def history_bars(
        self,
        instrument,
        bar_count,
        frequency,
        fields,
        dt,
        skip_suspended=True,
        include_now=False,
        adjust_type="pre",
        adjust_orig=None,
    ):
        if frequency != "1d":
            return None

        obid = instrument.order_book_id
        bars = self._bar_data.get(obid)
        if bars is None:
            return None

        dt_int = np.uint64(convert_date_to_int(dt))

        if include_now:
            end_pos = np.searchsorted(bars["datetime"], dt_int, side="right")
        else:
            pos = np.searchsorted(bars["datetime"], dt_int)
            if pos < len(bars) and bars[pos]["datetime"] == dt_int:
                end_pos = pos + 1  # include the exact-match bar (needed for benchmark)
            else:
                end_pos = np.searchsorted(bars["datetime"], dt_int, side="right")

        if bar_count is None:
            sliced = bars[:end_pos]
        else:
            start_pos = max(0, end_pos - bar_count)
            sliced = bars[start_pos:end_pos]

        if len(sliced) == 0:
            return None

        # Apply price adjustment
        adjust_orig_int = None
        if adjust_orig is not None:
            adjust_orig_int = int(convert_date_to_int(adjust_orig) // 1000000)
        sliced = self._apply_adjustment(sliced, obid, adjust_type, adjust_orig_int)

        if fields is None:
            return sliced
        if isinstance(fields, str):
            return sliced[fields]
        return sliced[list(fields)]

    def available_data_range(self, frequency) -> Tuple[datetime.date, datetime.date]:
        return self._min_date, self._max_date

    def is_suspended(self, order_book_id: str, dates: Sequence) -> List[bool]:
        if not self._suspended_dates:
            return [False] * len(dates)
        suspended_set = self._suspended_dates.get(order_book_id, set())
        if not suspended_set:
            return [False] * len(dates)
        return [
            d.strftime("%Y%m%d") if isinstance(d, (datetime.date, datetime.datetime, pd.Timestamp)) else str(d)
            in suspended_set
            for d in dates
        ]

    def is_st_stock(self, order_book_id: str, dates: Sequence) -> List[bool]:
        if not self._st_date_ranges:
            # Fallback: check current instrument special_type
            ins = self._instruments.get(order_book_id)
            if ins and ins.special_type in ("ST",):
                return [True] * len(dates)
            return [False] * len(dates)

        st_ranges = self._st_date_ranges.get(order_book_id, [])
        if not st_ranges:
            return [False] * len(dates)

        results = []
        for d in dates:
            if isinstance(d, (datetime.date, datetime.datetime, pd.Timestamp)):
                date_int = int(d.strftime("%Y%m%d"))
            else:
                date_int = int(str(d))
            is_st = any(start <= date_int <= end for start, end in st_ranges)
            results.append(is_st)
        return results

    def get_dividend(self, instrument) -> Optional[np.ndarray]:
        return None

    def get_split(self, instrument) -> Optional[np.ndarray]:
        return None

    def get_yield_curve(self, start_date, end_date, tenor=None):
        return None

    def get_ex_cum_factor(self, instrument):
        """Build ex_cum_factor from adj_factor for rqalpha's adjustment engine."""
        obid = instrument.order_book_id
        factors = self._adj_factor.get(obid)
        if factors is None or len(factors) == 0:
            # No adjustment data - return trivial factor
            bars = self._bar_data.get(obid)
            if bars is None or len(bars) == 0:
                return np.array([], dtype=[("start_date", "<i8"), ("ex_cum_factor", "<f8")])
            return np.array(
                [(int(bars[0]["datetime"] // 1000000), 1.0)],
                dtype=[("start_date", "<i8"), ("ex_cum_factor", "<f8")],
            )

        # Convert adj_factor to ex_cum_factor format
        # ex_cum_factor = adj_factor / adj_factor[0] (normalized so earliest = 1.0)
        earliest_factor = float(factors[0, 1])
        if earliest_factor == 0:
            earliest_factor = 1.0

        result = np.empty(len(factors), dtype=[("start_date", "<i8"), ("ex_cum_factor", "<f8")])
        result["start_date"] = factors[:, 0].astype(np.int64)
        result["ex_cum_factor"] = factors[:, 1].astype(np.float64) / earliest_factor
        return result

    def get_exchange_rate(self, trading_date, local, settlement=None):
        return ExchangeRate(
            bid_reference=1.0,
            ask_reference=1.0,
            bid_settlement_sh=1.0,
            ask_settlement_sh=1.0,
            bid_settlement_sz=1.0,
            ask_settlement_sz=1.0,
        )
