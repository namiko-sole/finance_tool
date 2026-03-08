#!/usr/bin/env python3
"""
配置文件解析
"""

import os
import json


def load_config(config_path):
    """
    加载配置文件
    
    Args:
        config_path: JSON配置文件路径
    
    Returns:
        dict: 配置内容
    """
    if not config_path or not os.path.exists(config_path):
        return {}
    
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def merge_cli_and_config(cli_args, config):
    """
    合并命令行参数和配置文件
    命令行参数优先
    """
    result = config.copy()
    
    for key, value in cli_args.items():
        if value is not None:
            result[key] = value
    
    return result


def validate_config(config):
    """
    验证配置有效性
    """
    errors = []
    
    # 检查股票代码
    if 'stock' not in config:
        errors.append("缺少stock字段")
    
    # 检查日期格式
    for date_key in ['start_date', 'end_date']:
        if date_key in config:
            date_val = config[date_key]
            if date_val and len(date_val) != 8:
                errors.append(f"{date_key}格式错误，应为YYYYMMDD")
    
    return errors


def get_default_config():
    """
    获取默认配置
    """
    return {
        'indicators': ['MA5', 'MA10', 'MA20'],
        'annotations': [],
        'style': 'yahoo',
        'title': None
    }
