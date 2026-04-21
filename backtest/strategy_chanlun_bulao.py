# -*- coding: utf-8 -*-
"""
Chan Lun + BULAO resonance backtest strategy for rqalpha.

Buy:  Chan Lun JDBL[-2] == -1 AND BULAO golden cross within last 3 bars
Sell: BULAO dead cross

Usage:
    python run_backtest.py -f strategy_chanlun_bulao.py -s 2024-06-01 -e 2025-12-31
"""

import sys
import os
import numpy as np
import pandas as pd

# Add indicator module paths
sys.path.insert(0, '/root/.openclaw/workspace/finance_tool/analyze/metric_tdx')

from MyTT import EMA, MA, REF, IF, HHV, LLV, BARSLAST, LOWRANGE, TOPRANGE
from backset import BACKSET


# ---------------------------------------------------------------------------
# Stock pool helpers
# ---------------------------------------------------------------------------

_MAIN_BOARD_PREFIXES = (
    "000", "001", "002", "003",  # SZ main board
    "600", "601", "603", "605",  # SH main board
)


def _load_main_board_stocks():
    """Load main-board non-ST stock pool with personality filter."""
    csv_path = "/root/.openclaw/workspace/data/raw/stock_list.csv"
    df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")

    # Filter main board + non-ST
    mask = pd.Series([False] * len(df))
    for i, row in df.iterrows():
        ts_code = row.get("ts_code", "")
        if not ts_code or "." not in ts_code:
            continue
        code = ts_code.split(".")[0]
        name = row.get("name", "")
        if code.startswith(_MAIN_BOARD_PREFIXES) and "ST" not in name.upper():
            mask.iloc[i] = True
    df = df[mask].copy()

    # Filter by stock personality (limit_up_count_1y >= 3) if available
    personality_path = "/root/.openclaw/workspace/data/raw/stock_personality.csv"
    if os.path.isfile(personality_path):
        p_df = pd.read_csv(personality_path, dtype=str, encoding="utf-8-sig")
        if "limit_up_count_1y" in p_df.columns:
            p_df["limit_up_count_1y"] = pd.to_numeric(
                p_df["limit_up_count_1y"], errors="coerce"
            ).fillna(0)
            active_codes = set(
                p_df[p_df["limit_up_count_1y"] >= 10]["ts_code"].tolist()
            )
            df = df[df["ts_code"].isin(active_codes)].copy()

    # Convert ts_code to rqalpha order_book_id
    stocks = []
    for ts_code in df["ts_code"]:
        code, exchange = ts_code.split(".")
        rq_exchange = "XSHE" if exchange == "SZ" else "XSHG"
        stocks.append(f"{code}.{rq_exchange}")

    return stocks


# ---------------------------------------------------------------------------
# Indicator computation (extracted from chanlun_tt/main.py, no AnnotationBuilder)
# ---------------------------------------------------------------------------

def compute_jdbl(H, L):
    """
    Compute Chan Lun JDBL signal from H, L numpy arrays.

    Returns:
        JDBL: numpy array where -1 = buy, 1 = sell, 0 = neutral
    """
    # Layer 0: fractal detection
    GSKZA = BACKSET(LLV(L, 5) < REF(LLV(L, 4), 1), 4)
    GSKZB = BACKSET((GSKZA == 0) & (REF(GSKZA, 1) == 1), 2)
    GSKZC = IF((GSKZB == 1) & (REF(GSKZB, 1) == 0), -1, 0)
    chanA = BACKSET(HHV(H, 5) > REF(HHV(H, 4), 1), 4)
    chanB = BACKSET((chanA == 0) & (REF(chanA, 1) == 1), 2)
    chanC = IF((chanB == 1) & (REF(chanB, 1) == 0), 1, 0)

    QKPD = IF(L > REF(H, 1), 1, IF(H < REF(L, 1), -1, 0))
    JQTG = BARSLAST(chanC == 1)
    JQDD = BARSLAST(GSKZC == -1)
    XZZQ = LOWRANGE(L)
    DZZQ = TOPRANGE(H)

    # Layer 1: DBLS / GBL
    DBLAA = IF(
        (GSKZC == -1) & (REF(JQTG, 1) > REF(JQDD, 1))
        & (LLV(L, JQTG + 1) < REF(LLV(L, JQTG + 1), 1)), -1, 0)
    DBLAB = IF(
        (GSKZC == -1) & (REF(JQTG, 1) <= REF(JQDD, 1))
        & ((JQTG >= 4) | (LLV(QKPD, JQTG) == -1)
           | (LLV(L, JQDD + 2) < REF(LLV(L, JQDD + 1), 1))), -1, 0)
    DBLS = IF(((DBLAA == -1) | (DBLAB == -1)) & (L < REF(H, JQTG + 1)), -1, 0)

    DTIME = 11
    # A and B are unused for signal computation (only for chart lines)
    _A = (H == HHV(H, DTIME * 5)) & (HHV(H, DTIME * 5) > REF(HHV(H, DTIME * 5), 1))
    _B = (L == LLV(L, DTIME * 5)) & (LLV(L, DTIME * 5) < REF(LLV(L, DTIME * 5), 1))

    YP = IF(((JQDD < 4) & (HHV(QKPD, JQDD) == 1)) | (REF(DBLS, JQDD) == 0), 1, 0)
    PD = IF(
        (chanC == 1) & (REF(JQDD, 1) <= REF(JQTG, 1)) & (YP == 1)
        & (DZZQ > REF(XZZQ, JQDD + 1)) & (DZZQ > REF(XZZQ, JQDD))
        & (DZZQ > REF(DZZQ, JQTG)), 1, 0)
    GBLA = IF(
        (chanC == 1) & (REF(JQDD, 1) > REF(JQTG, 1))
        & (HHV(H, JQDD + 1) > REF(HHV(H, JQDD + 1), 1)), 1, 0)
    GBLB = IF(
        (chanC == 1) & (REF(JQDD, 1) <= REF(JQTG, 1)) & (REF(DBLS, JQDD) == -1)
        & ((JQDD >= 4) | (HHV(QKPD, JQDD) == 1)), 1, 0)
    GBL = IF(((GBLA == 1) | (GBLB == 1) | (PD == 1)) & (H > REF(L, JQDD + 1)), 1, 0)

    # Layer 1 sell side: DBL
    YPA = IF(((JQTG < 4) & (HHV(QKPD, JQTG) != 1)) | (REF(GBL, JQTG) == 0), 1, 0)
    PDA = IF(
        (GSKZC == -1) & (REF(JQTG, 1) <= REF(JQDD, 1)) & (YPA == 1)
        & (XZZQ > REF(DZZQ, JQTG + 1)) & (XZZQ > REF(DZZQ, JQTG))
        & (XZZQ > REF(XZZQ, JQDD)), -1, 0)
    DBLA = IF(
        (GSKZC == -1) & (REF(JQTG, 1) > REF(JQDD, 1))
        & (LLV(L, JQTG + 1) < REF(LLV(L, JQTG + 1), 1)), -1, 0)
    DBLB = IF(
        (GSKZC == -1) & (REF(JQTG, 1) <= REF(JQDD, 1))
        & ((JQTG >= 4) | (LLV(QKPD, JQTG) == -1) | (PDA == -1)), -1, 0)
    DBL = IF(((DBLA == -1) | (DBLB == -1)) & (L < REF(H, JQTG + 1)), -1, 0)

    # Layer 2
    JQTGA = BARSLAST(GBL == 1)
    JQDDA = BARSLAST(DBL == -1)
    YPX = IF(((JQDDA < 4) & (HHV(QKPD, JQDDA) == 1)) | (REF(DBL, JQDDA) == 0), 1, 0)
    PDX = IF(
        (chanC == 1) & (REF(JQDDA, 1) <= REF(JQTGA, 1)) & (YPX == 1)
        & (DZZQ > REF(XZZQ, JQDDA + 1)) & (DZZQ > REF(XZZQ, JQDDA))
        & (DZZQ > REF(DZZQ, JQTGA)), 1, 0)
    GBLXA = IF(
        (chanC == 1) & (REF(JQDDA, 1) > REF(JQTGA, 1))
        & (HHV(H, JQDDA + 1) > REF(HHV(H, JQDDA + 1), 1)), 1, 0)
    GBLXB = IF(
        (chanC == 1) & (REF(JQDDA, 1) <= REF(JQTGA, 1)) & (REF(DBL, JQDDA) == -1)
        & ((JQDDA >= 4) | (HHV(QKPD, JQDDA) == 1)), 1, 0)
    GBLX = IF(((GBLXA == 1) | (GBLXB == 1) | (PDX == 1)) & (H > REF(L, JQDDA + 1)), 1, 0)

    YPXA = IF(((JQTGA < 4) & (HHV(QKPD, JQTGA) != 1)) | (REF(GBLXA, JQTGA) == 0), 1, 0)
    PDXA = IF(
        (GSKZC == -1) & (REF(JQTGA, 1) <= REF(JQDDA, 1)) & (YPXA == 1)
        & (XZZQ > REF(DZZQ, JQTGA + 1)) & (XZZQ > REF(DZZQ, JQTGA))
        & (XZZQ > REF(XZZQ, JQDDA)), -1, 0)
    DBLXA = IF(
        (GSKZC == -1) & (REF(JQTGA, 1) > REF(JQDDA, 1))
        & (LLV(L, JQTGA + 1) < REF(LLV(L, JQTGA + 1), 1)), -1, 0)
    DBLXB = IF(
        (GSKZC == -1) & (REF(JQTGA, 1) <= REF(JQDDA, 1))
        & ((JQTGA >= 4) | (LLV(QKPD, JQTGA) == -1) | (PDXA == -1)), -1, 0)
    DBLX = IF(((DBLXA == -1) | (DBLXB == -1)) & (L < REF(H, JQTGA + 1)), -1, 0)

    # Layer 3
    JQTGYA = BARSLAST(GBLX == 1)
    JQDDYA = BARSLAST(DBLX == -1)
    YPYX = IF(((JQDDYA < 4) & (HHV(QKPD, JQDDYA) == 1)) | (REF(DBLX, JQDDYA) == 0), 1, 0)
    PDYX = IF(
        (chanC == 1) & (REF(JQDDYA, 1) <= REF(JQTGYA, 1)) & (YPYX == 1)
        & (DZZQ > REF(XZZQ, JQDDYA + 1)) & (DZZQ > REF(XZZQ, JQDDYA))
        & (DZZQ > REF(DZZQ, JQTGYA)), 1, 0)
    GBLYXA = IF(
        (chanC == 1) & (REF(JQDDYA, 1) > REF(JQTGYA, 1))
        & (HHV(H, JQDDYA + 1) > REF(HHV(H, JQDDYA + 1), 1)), 1, 0)
    GBLYXB = IF(
        (chanC == 1) & (REF(JQDDYA, 1) <= REF(JQTGYA, 1)) & (REF(DBLX, JQDDYA) == -1)
        & ((JQDDYA >= 4) | (HHV(QKPD, JQDDYA) == 1)), 1, 0)
    GBLYX = IF(
        ((GBLYXA == 1) | (GBLYXB == 1) | (PDYX == 1)) & (H > REF(L, JQDDYA + 1)), 1, 0)

    YPYXA = IF(
        ((JQTGYA < 4) & (HHV(QKPD, JQTGYA) == 1)) | (REF(GBLYXA, JQTGYA) == 0), 1, 0)
    PDYXA = IF(
        (GSKZC == -1) & (REF(JQTGYA, 1) <= REF(JQDDYA, 1)) & (YPYXA == 1)
        & (XZZQ > REF(DZZQ, JQTGYA + 1)) & (XZZQ > REF(DZZQ, JQTGYA))
        & (XZZQ > REF(XZZQ, JQDDYA)), -1, 0)
    DBLYXA = IF(
        (GSKZC == -1) & (REF(JQTGYA, 1) > REF(JQDDA, 1))
        & (LLV(L, JQTGYA + 1) < REF(LLV(L, JQTGYA + 1), 1)), -1, 0)
    DBLYXB = IF(
        (GSKZC == -1) & (REF(JQTGYA, 1) <= REF(JQDDYA, 1))
        & ((JQTGYA >= 4) | (LLV(QKPD, JQTGYA) == -1) | (PDYXA == -1)), -1, 0)
    DBLYX = IF(
        ((DBLYXA == -1) | (DBLYXB == -1)) & (L < REF(H, JQTGYA + 1)), -1, 0)

    # Final merge
    AAAD = IF(
        (GBLYX == 1) & (DBLYX == -1) & (H > REF(H, REF(JQTGYA, 1) + 2)), 1,
        IF((GBLYX == 1) & (DBLYX == -1) & (L < REF(L, REF(JQDDYA, 1) + 2)), -1, 0))
    JDBL = IF(AAAD == 0, GBLYX + DBLYX, AAAD)

    return JDBL


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

LOOKBACK = 250          # bars for Chan Lun indicator warm-up
BULAO_LOOKBACK = 80     # shorter lookback for BULAO scanning (fast)
MAX_POSITIONS = 5       # max concurrent positions
RESONANCE_WINDOW = 3    # BULAO golden cross must be within this many bars
MAX_HOLD_DAYS = 10      # safety exit: max holding period


# ---------------------------------------------------------------------------
# rqalpha strategy
# ---------------------------------------------------------------------------

def init(context):
    context.lookback = LOOKBACK
    context.max_positions = MAX_POSITIONS
    context.entry_bar = {}
    context.bar_count = 0

    context.stocks = _load_main_board_stocks()
    logger.info(f"Stock pool: {len(context.stocks)} main-board non-ST stocks")


def handle_bar(context, bar_dict):
    context.bar_count += 1

    held_stocks = [
        s for s in context.portfolio.positions
        if context.portfolio.positions[s].quantity > 0
    ]

    # --- Sell logic ---
    for stock in held_stocks:
        try:
            bars = history_bars(stock, context.lookback, "1d", ["close", "high", "low"])
        except Exception:
            continue
        if bars is None or len(bars) < 50:
            continue

        C = bars["close"].astype(np.float64)
        H = bars["high"].astype(np.float64)
        L = bars["low"].astype(np.float64)

        _, dead_cross = compute_bulao(C, H, L)

        should_sell = False
        if dead_cross[-1]:
            should_sell = True

        entry = context.entry_bar.get(stock)
        if entry is not None and (context.bar_count - entry) >= MAX_HOLD_DAYS:
            should_sell = True

        if should_sell:
            order_target_percent(stock, 0)
            context.entry_bar.pop(stock, None)

    # --- Buy logic (two-phase: cheap BULAO first, expensive Chan Lun only for candidates) ---
    current_held = sum(
        1 for s in context.portfolio.positions
        if context.portfolio.positions[s].quantity > 0
    )
    available_slots = context.max_positions - current_held
    if available_slots <= 0:
        return

    # Phase 1: scan all stocks with BULAO only (fast, short lookback)
    bulao_hits = []
    for stock in context.stocks:
        try:
            pos = context.portfolio.positions.get(stock)
            if pos and pos.quantity > 0:
                continue

            bars = history_bars(stock, BULAO_LOOKBACK, "1d", ["close", "high", "low"])
        except Exception:
            continue
        if bars is None or len(bars) < 50:
            continue

        C = bars["close"].astype(np.float64)
        H = bars["high"].astype(np.float64)
        L = bars["low"].astype(np.float64)

        golden_cross, _ = compute_bulao(C, H, L)
        if golden_cross is None or len(golden_cross) < RESONANCE_WINDOW:
            continue

        if golden_cross[-RESONANCE_WINDOW:].any():
            bulao_hits.append(stock)

    # Phase 2: compute Chan Lun only for BULAO candidates (expensive, full lookback)
    buy_candidates = []
    for stock in bulao_hits:
        try:
            bars = history_bars(stock, context.lookback, "1d", ["high", "low"])
        except Exception:
            continue
        if bars is None or len(bars) < 100:
            continue

        H = bars["high"].astype(np.float64)
        L = bars["low"].astype(np.float64)
        try:
            jdbl = compute_jdbl(H, L)
        except Exception:
            continue

        if jdbl is None or len(jdbl) < 5:
            continue

        if jdbl[-2] == -1:
            buy_candidates.append(stock)

    # Buy up to available slots
    for stock in buy_candidates[:available_slots]:
        weight = 1.0 / context.max_positions
        order_target_percent(stock, weight)
        context.entry_bar[stock] = context.bar_count
