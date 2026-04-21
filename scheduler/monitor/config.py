"""监控配置加载模块 - 读取 watches.yaml"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
import yaml
import os


@dataclass
class StrategyConfig:
    type: str
    params: Dict[str, Any] = field(default_factory=dict)
    notify: List[str] = field(default_factory=list)


@dataclass
class WatchEntry:
    ts_code: str
    name: str
    strategies: List[StrategyConfig] = field(default_factory=list)


@dataclass
class NotifyChannel:
    type: str
    target: str = ""


@dataclass
class WatchConfig:
    watches: List[WatchEntry] = field(default_factory=list)
    notify_channels: Dict[str, NotifyChannel] = field(default_factory=dict)


def load_config(config_path: Optional[str] = None) -> WatchConfig:
    """读取 watches.yaml 并返回 WatchConfig 数据结构"""
    if config_path is None:
        config_path = os.path.join(os.path.dirname(__file__), "watches.yaml")

    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    watches = []
    for w in raw.get("watches", []):
        strategies = []
        for s in w.get("strategies", []):
            strategies.append(StrategyConfig(
                type=s["type"],
                params=s.get("params", {}),
                notify=s.get("notify", []),
            ))
        watches.append(WatchEntry(
            ts_code=w["ts_code"],
            name=w["name"],
            strategies=strategies,
        ))

    notify_channels = {}
    for name, ch in raw.get("notify_channels", {}).items():
        notify_channels[name] = NotifyChannel(
            type=ch["type"],
            target=ch.get("target", ""),
        )

    return WatchConfig(watches=watches, notify_channels=notify_channels)
