# -*- coding: utf-8 -*-
"""
rqalpha mod that injects CSVDataSource.

Loaded via config: mod.csv_source.lib = "csv_source_mod"
"""

from rqalpha.interface import AbstractMod

from csv_data_source import CSVDataSource

# Default paths
_DEFAULT_DATA_DIR = "/root/.openclaw/workspace/data/raw/stock_daily"
_DEFAULT_STOCK_LIST = "/root/.openclaw/workspace/data/raw/stock_list.csv"
_DEFAULT_INDEX_LIST = "/root/.openclaw/workspace/data/raw/index_list.csv"
_DEFAULT_CALENDAR = "/root/.openclaw/workspace/data/raw/trade_calendar.csv"


def load_mod():
    return CSVSourceMod()


__config__ = {
    "data_dir": _DEFAULT_DATA_DIR,
    "stock_list_path": _DEFAULT_STOCK_LIST,
    "index_list_path": _DEFAULT_INDEX_LIST,
    "trade_calendar_path": _DEFAULT_CALENDAR,
}


class CSVSourceMod(AbstractMod):
    def start_up(self, env, mod_config):
        data_dir = getattr(mod_config, "data_dir", _DEFAULT_DATA_DIR)
        stock_list = getattr(mod_config, "stock_list_path", _DEFAULT_STOCK_LIST)
        calendar = getattr(mod_config, "trade_calendar_path", _DEFAULT_CALENDAR)

        ds = CSVDataSource(
            data_dir=data_dir,
            stock_list_path=stock_list,
            trade_calendar_path=calendar,
        )
        env.set_data_source(ds)

    def tear_down(self, code, exception=None):
        pass
