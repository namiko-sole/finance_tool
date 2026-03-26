# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Project Overview

A Chinese A-share stock trading analysis system for short-term/ultra-short-term trading strategies. The system combines technical analysis (Chan Lun theory, BULAO indicators) with automated data collection and stock filtering.

**Key Principle**: Agent provides data and analysis, human trader makes decisions.

---

## Directory Structure

```
finance_tool/
├── scheduler/           # Data collection schedulers
│   ├── history_scheduler.py       # Historical daily K-line updates
│   ├── realtime_scheduler.py      # Real-time quote crawler (9:15-17:00)
│   ├── trade_calendar_scheduler.py # Trading calendar updates
│   └── scheduler_utils.py         # Retry/freshness utilities
├── fetchers/           # Unified data fetching
│   ├── base_fetcher.py            # Base class with retry mechanism
│   ├── data_fetcher.py            # Main DataFetcher (priority: local > tushare > akshare)
│   ├── local_fetcher.py           # Local file reading
│   └── tushare_fetcher.py         # Tushare API wrapper
├── crawlers/           # Specialized data crawlers
│   ├── crawler_akshare_*.py       # AKShare-based crawlers
│   ├── crawler_sina_kline.py      # Sina K-line data
│   ├── crawler_tushare_news.py    # News crawler
│   └── ...
├── analyze/            # Technical analysis modules
│   ├── metric_tdx/     # TongDaXin indicators ported to Python
│   │   ├── chanlun_tt/main.py     # Chan Lun buy/sell signals
│   │   ├── bulao/main.py           # BULAO golden cross signals
│   │   ├── MyTT.py                 # Technical indicator functions
│   │   └── backset.py              # BACKSET function implementation
│   └── stock_filter/               # Stock screening tools
│       └── chanlun_filter.py       # Resonance filter (Chan Lun + BULAO)
├── kline_plot/         # K-line chart visualization
│   ├── chart.py                  # Main plotting function
│   ├── indicators.py             # Technical indicator calculations
│   └── loader.py                 # Data loading
└── utils/              # Utilities
    ├── cache_utils.py            # Cache manager with TTL
    └── file_server.py            # File serving utilities
```

---

## Common Commands

### Running Schedulers

```bash
# Trade calendar (runs daily to update trading calendar info)
python scheduler/trade_calendar_scheduler.py

# Historical data update (runs on trading days only)
python scheduler/history_scheduler.py
python scheduler/history_scheduler.py --force  # Force update even on non-trading days

# Real-time quote crawler (auto-runs 9:10-17:10 on trading days)
python scheduler/realtime_scheduler.py
```

### Stock Filtering

```bash
# Filter stocks with Chan Lun + BULAO buy signal resonance (last N days)
python analyze/stock_filter/chanlun_filter.py -n 3  # Last 3 days (default)
python analyze/stock_filter/chanlun_filter.py -n 5  # Last 5 days
```

### Technical Analysis

```bash
# Get Chan Lun buy/sell signals for a stock
cd analyze/metric_tdx/chanlun_tt
python main.py  # Outputs annotations to /root/.openclaw/workspace/output/kline/

# Use as module:
from analyze.metric_tdx.chanlun_tt.main import get_buy_sell_signals
result = get_buy_sell_signals('601888', start_date='20250101')
```

### Data Fetching (Programmatic)

```python
from fetchers.data_fetcher import DataFetcher

fetcher = DataFetcher()

# Get stock daily K-line
df = fetcher.get_stock_daily('000001.SZ', start_date='20250101', end_date='20250301')

# Get stock list
stock_list = fetcher.get_stock_list()

# Get trade calendar
calendar = fetcher.get_trade_calendar(start_date='20250101')
```

### Testing

```bash
# Run tests (if any exist)
pytest

# Run with coverage
pytest --cov=. --cov-report=term-missing
```

---

## Data Architecture

### Data Directory (`/root/.openclaw/workspace/data/raw/`)

```
raw/
├── stock_list.csv              # All A-share stocks
├── stock_daily/                # Individual stock daily K-line (ts_code.csv)
│   ├── 000001.SZ.csv
│   └── 600000.SH.csv
├── board_stocks/               # Board/concept constituent stocks
│   └── {board_name}.csv
├── stock_to_boards.json        # Stock -> boards mapping
├── trade_calendar.csv          # Full trading calendar
└── trade_calendar_info.json    # Current/previous/next trade day
```

### Data Fetching Priority

The `DataFetcher` class automatically tries data sources in this order:
1. **Local files** (`/root/.openclaw/workspace/data/raw/`) - Fastest
2. **Tushare Pro** - Requires token, 800 calls/min limit
3. **AKShare** - Free, no limit

### Stock Code Format

- **Tushare format**: `000001.SZ`, `600000.SH`, `8XXXXX.BJ`
- **AKShare format**: `sh600000`, `sz000001`, `bjXXXXXX`
- **Convert with**: `convert_code()` in `realtime_scheduler.py`

---

## Key Technical Concepts

### Chan Lun Theory (缠论)

A Chinese technical analysis methodology focusing on:
- **Bi (笔)**: Trend segments formed by fractals
- **Duan (段)**: Larger trends composed of Bi
- **Zhongshu (中枢)**: Price consolidation zones
- **Buy signals**: When JDBL = -1 (third buy point is most reliable)
- **Sell signals**: When JDBL = 1

Located in: `analyze/metric_tdx/chanlun_tt/main.py`

### BULAO Indicator (不落浪)

A triple-EMA momentum indicator:
- XYS0: Rate of change after 3-layer EMA smoothing
- X1, X2: Moving averages of XYS0
- **Golden cross**: Buy signal when short-term crosses above long-term

Located in: `analyze/metric_tdx/bulao/main.py`

### Resonance Filtering

Stocks showing **both** Chan Lun buy signal AND BULAO golden cross within N days are considered high-probability setups.

Located in: `analyze/stock_filter/chanlun_filter.py`

---

## Trading Strategy Context

This system supports a **short-term/ultra-short-term** trading style:
- **Holding period**: 1-3 days
- **Decision basis**: Market sentiment cycle + technical analysis
- **Entry timing**: Chan Lun 3rd buy point + BULAO resonance
- **Risk management**: Position sizing based on market strength

### Market Sentiment Cycle

| Phase | Characteristics | Position Size |
|-------|----------------|---------------|
| Start (启动期) | New theme emerges | 30-50% |
| Acceleration (加速期) | Leader consecutive limit-ups | 70-100% |
| Divergence (分歧期) | High/low rotation | 30-50% |
| Decline (退潮期) | Height declining | 0-10% |
| Bottom (冰点期) | Few limit-ups | 10-30% |

---

## API Rate Limits

- **Tushare Pro**: 800 calls/min (use `MIN_CALL_INTERVAL_SECONDS = 0.1` in schedulers)
- **AKShare**: No official limit, but use random delays (5-6 minutes for real-time crawler)

---

## Output Locations

- **K-line annotations**: `/root/.openclaw/workspace/output/kline/*.json`
- **Filtered stocks**: `/root/.openclaw/workspace/output/*.csv`
- **Scheduler logs**: `scheduler/realtime_scheduler.log`

---

## Important Notes

1. **Hardcoded Tushare token**: Currently in codebase (`da8b0417e689bd72a3e5b3312fb6094f9405b06b667f7033eebecf83`). Should move to environment variable.

2. **Trading hours**: Real-time scheduler only runs during:
   - Morning: 9:15-11:30
   - Afternoon: 13:00-17:00
   - Auto-exits at 17:10

3. **ST stock filtering**: `chanlun_filter.py` excludes ST stocks and non-main-board (科创板, 创业板, 北交所)

4. **File encoding**: All CSV files use `utf-8-sig` encoding

5. **Date formats**:
   - Tushare: `YYYYMMDD` (e.g., `20250101`)
   - Internal DataFrame: `YYYY-MM-DD` or datetime
