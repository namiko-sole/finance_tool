"""状态管理模块 - 每只股票一个JSON文件"""

import json
import os
from typing import Dict, Any, List

STATE_DIR = os.path.join(os.path.dirname(__file__), "state")
MAX_TICKS = 60


def _state_path(ts_code: str) -> str:
    return os.path.join(STATE_DIR, f"{ts_code}.json")


def load(ts_code: str, today_str: str) -> Dict[str, Any]:
    """加载状态，如果日期不是今天则自动reset"""
    path = _state_path(ts_code)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            state = json.load(f)
        if state.get("date") != today_str:
            return reset(ts_code, today_str)
        return state
    return reset(ts_code, today_str)


def save(state: Dict[str, Any]) -> None:
    """保存状态到JSON文件"""
    os.makedirs(STATE_DIR, exist_ok=True)
    path = _state_path(state["ts_code"])
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def reset(ts_code: str, today_str: str) -> Dict[str, Any]:
    """清空ticks和strategy_states"""
    state = {
        "ts_code": ts_code,
        "date": today_str,
        "ticks": [],
        "strategy_states": {},
    }
    save(state)
    return state


def append_tick(state: Dict[str, Any], tick_dict: Dict[str, Any]) -> None:
    """追加一个tick，最多保留60个"""
    state["ticks"].append(tick_dict)
    if len(state["ticks"]) > MAX_TICKS:
        state["ticks"] = state["ticks"][-MAX_TICKS:]
