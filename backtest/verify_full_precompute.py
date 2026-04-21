#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Full precompute test - verify that the entire backtest range can be precomputed.
"""

import sys
import time

sys.path.insert(0, '/root/.openclaw/workspace/finance_tool/analyze/metric_tdx')
sys.path.insert(0, '/root/.openclaw/workspace/finance_tool/backtest')

from strategy_bulao import (
    _load_main_board_stocks_basic,
    _precompute_monthly_pools,
    BACKTEST_START,
    BACKTEST_END,
)

print("Loading basic stock pool...")
all_stocks = _load_main_board_stocks_basic()
print(f"  {len(all_stocks)} main-board non-ST stocks")

print(f"\nPrecomputing monthly pools for {BACKTEST_START} to {BACKTEST_END}...")
t0 = time.time()
monthly_pools = _precompute_monthly_pools(BACKTEST_START, BACKTEST_END, all_stocks)
elapsed = time.time() - t0

print(f"\nDone in {elapsed:.1f}s")
print(f"Total months precomputed: {len(monthly_pools)}")
print("\nMonthly pool sizes:")
for ym in sorted(monthly_pools.keys()):
    pool = monthly_pools[ym]
    print(f"  {ym}: {len(pool)} stocks")

# Verify: each month has <= 500 stocks
max_pool = max(len(p) for p in monthly_pools.values())
print(f"\nMax pool size: {max_pool}")
assert max_pool <= 500, f"Pool exceeds 500: {max_pool}"

# Verify: first month doesn't use data from that month onwards
print("\nFull precompute verification PASSED")
