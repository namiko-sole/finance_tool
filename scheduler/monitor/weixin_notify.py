"""微信通知模块 - 通过 Hermes gateway 内部 API 发送微信消息"""

import asyncio
import logging
import os
import sys

logger = logging.getLogger("monitor.weixin")

# Hermes agent 路径
HERMES_AGENT_PATH = "/root/.hermes/hermes-agent"
WEIXIN_CHAT_ID = "o9cq809quOi3wbLt4p9qI-cjq6r8@im.wechat"


def send_weixin_message(message: str, chat_id: str = WEIXIN_CHAT_ID) -> bool:
    """
    通过 Hermes gateway 的 send_weixin_direct 发送微信消息。
    返回 True 表示成功，False 表示失败。
    """
    try:
        # 添加 Hermes agent 到 Python 路径
        if HERMES_AGENT_PATH not in sys.path:
            sys.path.insert(0, HERMES_AGENT_PATH)

        from gateway.platforms.weixin import check_weixin_requirements, send_weixin_direct
        from gateway.config import load_gateway_config, Platform

        if not check_weixin_requirements():
            logger.error("微信依赖不满足 (需要 aiohttp + cryptography)")
            return False

        # 加载网关配置
        gw_config = load_gateway_config()
        weixin_config = gw_config.platforms.get(Platform.WEIXIN)
        if not weixin_config:
            logger.error("微信平台未配置")
            return False

        # 在新的事件循环中运行异步发送
        result = asyncio.run(send_weixin_direct(
            extra=weixin_config.extra,
            token=weixin_config.token,
            chat_id=chat_id,
            message=message,
        ))

        if result.get("error"):
            logger.error(f"微信发送失败: {result['error']}")
            return False

        logger.info(f"微信消息发送成功 -> {chat_id}")
        return True

    except Exception as e:
        logger.error(f"微信发送异常: {e}", exc_info=True)
        return False
