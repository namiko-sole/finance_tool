"""通知模块 - 支持飞书和微信双渠道推送"""

import subprocess
import logging
from typing import Any

logger = logging.getLogger("monitor.notify")

SEND_SCRIPT = "/root/.openclaw/workspace/skills/feishu-webhook/scripts/send_message.sh"


def send_alert(alert_text: str, channel_config: Any) -> None:
    """
    发送告警通知。
    - alert_text: 完整通知文本
    - channel_config: NotifyChannel 对象，有 type 和 target 属性
    """
    if hasattr(channel_config, "type") and channel_config.type == "weixin":
        _send_weixin(alert_text, channel_config.target)
        return

    # 飞书渠道（feishu_mention 或 feishu_webhook）
    _send_feishu(alert_text, channel_config)


def _send_feishu(alert_text: str, channel_config: Any) -> None:
    """发送飞书通知"""
    # 如果是 feishu_mention 类型，在消息开头加 @{target}
    if hasattr(channel_config, "type") and channel_config.type == "feishu_mention":
        mention = f"@{channel_config.target}"
        body = f"{mention}\n{alert_text}"
    else:
        body = alert_text

    # 构造标题|正文 格式
    lines = alert_text.strip().split("\n")
    title = lines[0] if lines else "告警通知"
    content = "\n".join(lines[1:]) if len(lines) > 1 else title

    # 如果是 feishu_mention，正文前加 mention
    if hasattr(channel_config, "type") and channel_config.type == "feishu_mention":
        mention = f"@{channel_config.target}"
        content = f"{mention}\n{content}"

    msg = f"{title}|{content}"

    try:
        subprocess.run(
            ["bash", SEND_SCRIPT, msg, "card"],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except Exception as e:
        logger.error(f"飞书通知发送失败: {e}")


def _send_weixin(alert_text: str, target: str = "") -> None:
    """发送微信通知"""
    try:
        from .weixin_notify import send_weixin_message
        chat_id = target if target else "o9cq809quOi3wbLt4p9qI-cjq6r8@im.wechat"
        success = send_weixin_message(alert_text, chat_id)
        if not success:
            logger.error("微信消息发送失败")
    except ImportError:
        logger.error("微信通知模块未找到，跳过微信推送")
    except Exception as e:
        logger.error(f"微信通知发送异常: {e}", exc_info=True)
