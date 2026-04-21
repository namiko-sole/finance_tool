#!/bin/bash
# 新闻爬虫调度 - 仅交易日运行
CALENDAR="/root/.openclaw/workspace/data/raw/trade_calendar_info.json"
LOG="/root/.openclaw/workspace/data/raw/news/crawler.log"
PYTHON="/root/.openclaw/workspace/venv/bin/python"
SCRIPT="/root/.openclaw/workspace/finance_tool/crawlers/crawler_tushare_news.py"

# 检查交易日历
if [ ! -f "$CALENDAR" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') 交易日历不存在，跳过" >> "$LOG"
    exit 0
fi

TODAY=$(date '+%Y%m%d')
IS_TRADE=$($PYTHON -c "
import json
with open('$CALENDAR') as f:
    d = json.load(f)
print('yes' if d.get('current_trade_day') == '$TODAY' else 'no')
")

if [ "$IS_TRADE" = "no" ]; then
    exit 0
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') 开始爬取" >> "$LOG"
$PYTHON $SCRIPT >> "$LOG" 2>&1
echo "$(date '+%Y-%m-%d %H:%M:%S') 完成" >> "$LOG"
