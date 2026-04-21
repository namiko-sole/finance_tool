# -*- coding: utf-8 -*-
"""
Run backtests with local CSV data via rqalpha.

Usage:
    python run_backtest.py                          # default strategy + params
    python run_backtest.py -f my_strategy.py        # custom strategy file
    python run_backtest.py -s 2024-01-01 -e 2025-01-01  # custom date range

Strategy files follow rqalpha conventions:
    def init(context): ...
    def handle_bar(context, bar_dict): ...
"""

import os
import sys
import gc
import argparse
import datetime

# Add this directory to sys.path so csv_source_mod is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rqalpha import run_code

# ---------------------------------------------------------------------------
# Default example strategy: equal-weight hold N stocks
# ---------------------------------------------------------------------------

DEFAULT_STRATEGY = """
def init(context):
    context.stocks = [
        "000001.XSHE",  # 平安银行
        "600036.XSHG",  # 招商银行
        "000858.XSHE",  # 五粮液
        "600519.XSHG",  # 贵州茅台
    ]

def handle_bar(context, bar_dict):
    for stock in context.stocks:
        order_target_percent(stock, 1.0 / len(context.stocks))
"""

# ---------------------------------------------------------------------------
# A-Share transaction cost defaults
# ---------------------------------------------------------------------------
# rqalpha base commission rate = 0.0008 (万八)
# To get 万三 (0.0003): multiplier = 0.0003 / 0.0008 = 0.375
_DEFAULT_COMMISSION_MULTIPLIER = 0.375

# rqalpha base stamp tax rate = 0.0005 (千分之0.5, post 2023-08-28)
# To get 千分之一 (0.001): multiplier = 0.001 / 0.0005 = 2.0
_DEFAULT_TAX_MULTIPLIER = 2.0

# Minimum commission per order (yuan)
_DEFAULT_MIN_COMMISSION = 5

# Default slippage as price ratio (0.1%)
_DEFAULT_SLIPPAGE = 0.001


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args():
    parser = argparse.ArgumentParser(description="Run rqalpha backtest with local CSV data")
    parser.add_argument("-f", "--file", default=None, help="Strategy .py file path")
    parser.add_argument("-s", "--start", default="2024-01-02", help="Start date (YYYY-MM-DD)")
    parser.add_argument("-e", "--end", default="2025-12-31", help="End date (YYYY-MM-DD)")
    parser.add_argument("--cash", type=float, default=1000000, help="Initial cash (default: 1M)")
    parser.add_argument("--benchmark", default=None, help="Benchmark (e.g. 000300.XSHG); None to disable")
    parser.add_argument("--plot", action="store_true", help="Show plot after backtest")

    # Transaction cost arguments
    cost_group = parser.add_argument_group("Transaction Cost / Slippage")
    cost_group.add_argument(
        "--commission", type=float, default=0.0003,
        help="Commission rate in decimal, e.g. 0.0003 for 万三 (default: 0.0003)"
    )
    cost_group.add_argument(
        "--min-commission", type=float, default=_DEFAULT_MIN_COMMISSION,
        help=f"Minimum commission per order in yuan (default: {_DEFAULT_MIN_COMMISSION})"
    )
    cost_group.add_argument(
        "--stamp-tax", type=float, default=0.001,
        help="Stamp tax rate (sell-side only) in decimal, e.g. 0.001 for 千一 (default: 0.001)"
    )
    cost_group.add_argument(
        "--no-stamp-tax", action="store_true",
        help="Disable stamp tax entirely"
    )
    cost_group.add_argument(
        "--slippage", type=float, default=_DEFAULT_SLIPPAGE,
        help=f"Slippage as price ratio, e.g. 0.001 for 0.1%% (default: {_DEFAULT_SLIPPAGE})"
    )
    cost_group.add_argument(
        "--no-slippage", action="store_true",
        help="Disable slippage entirely (set to 0)"
    )
    cost_group.add_argument(
        "--no-commission", action="store_true",
        help="Disable all commissions and taxes (zero-cost backtest)"
    )
    cost_group.add_argument(
        "--pit-tax", action="store_true",
        help="Use point-in-time historical stamp tax rate (千一 before 2023-08-28, 千分之0.5 after)"
    )

    return parser.parse_args()


def build_config(args):
    """Build rqalpha config dict from CLI arguments, including transaction cost settings."""

    # --- Commission & tax config ---
    if args.no_commission:
        # Zero-cost mode: set multipliers to 0, min commission to 0
        commission_multiplier = 0
        min_commission = 0
        tax_multiplier = 0
    else:
        # rqalpha's internal base rates:
        #   commission base = 0.0008 (万八)
        #   stamp tax base  = 0.0005 (千分之0.5, post 2023-08-28)
        # We compute multipliers so that actual_rate = base_rate * multiplier
        commission_multiplier = args.commission / 0.0008
        min_commission = args.min_commission

        if args.no_stamp_tax:
            tax_multiplier = 0
        else:
            tax_multiplier = args.stamp_tax / 0.0005

    # --- Slippage config ---
    if args.no_slippage:
        slippage = 0
    else:
        slippage = args.slippage

    config = {
        "base": {
            "start_date": args.start,
            "end_date": args.end,
            "frequency": "1d",
            "accounts": {"stock": args.cash},
            "benchmark": args.benchmark,
        },
        "extra": {
            "log_level": "warning",
        },
        "mod": {
            "sys_analyser": {
                "enabled": True,
                "plot": args.plot,
            },
            "csv_source": {
                "enabled": True,
                "lib": "csv_source_mod",
                "priority": 50,
            },
            # Transaction cost configuration
            "sys_transaction_cost": {
                "enabled": True,
                "stock_commission_multiplier": commission_multiplier,
                "stock_min_commission": min_commission,
                "tax_multiplier": tax_multiplier,
                "pit_tax": args.pit_tax,
            },
            # Simulation / slippage configuration
            "sys_simulation": {
                "slippage": slippage,
                "slippage_model": "PriceRatioSlippage",
            },
        },
    }
    return config


def run_backtest(args):
    config = build_config(args)

    # Inject date range into environment so strategy code can pick it up
    # (e.g. for precomputing monthly pools)
    os.environ["RQ_START"] = args.start.replace("-", "")
    os.environ["RQ_END"] = args.end.replace("-", "")

    if args.file:
        # Read strategy file and use run_code
        with open(args.file, "r", encoding="utf-8") as f:
            code = f.read()
        result = run_code(code, config=config)
    else:
        result = run_code(DEFAULT_STRATEGY, config=config)

    # Force GC to avoid memory leak (rqalpha issue #232)
    gc.collect()
    return result


def print_summary(result):
    if not result:
        print("No result returned.")
        return

    # result is a dict from mod tear_down, key is mod_name
    # sys_analyser returns a dict with 'summary' etc.
    analyser_result = result.get("sys_analyser", {})
    if not analyser_result:
        print("Keys in result:", list(result.keys()))
        print("No analyser result.")
        return

    summary = analyser_result.get("summary", {})
    if not summary:
        print("No summary.")
        print("Analyser keys:", list(analyser_result.keys()))
        return

    print("\n" + "=" * 60)
    print("  BACKTEST SUMMARY")
    print("=" * 60)
    for key in [
        "total_returns", "annualized_returns", "sharpe",
        "max_drawdown", "volatility", "alpha", "beta",
    ]:
        val = summary.get(key)
        if val is not None:
            if isinstance(val, float):
                print(f"  {key:25s}: {val:.4f}")
            else:
                print(f"  {key:25s}: {val}")
    print("=" * 60)


if __name__ == "__main__":
    args = parse_args()
    result = run_backtest(args)
    print_summary(result)
