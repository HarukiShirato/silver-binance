# notifier.py - 飞书通知模块

import asyncio
import aiohttp
from typing import Optional
from datetime import datetime

import logging
logger = logging.getLogger(__name__)


class FeishuNotifier:
    """飞书 Webhook 通知"""

    def __init__(self, webhook_url: str, enabled: bool = True):
        """
        Args:
            webhook_url: 飞书机器人 Webhook 地址
                        格式: https://open.feishu.cn/open-apis/bot/v2/hook/xxx
        """
        self.webhook_url = webhook_url
        self.enabled = enabled and webhook_url
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def send_message(self, title: str, content: str):
        """发送富文本消息"""
        if not self.enabled:
            return

        try:
            session = await self._get_session()

            # 飞书富文本消息格式
            payload = {
                "msg_type": "post",
                "content": {
                    "post": {
                        "zh_cn": {
                            "title": title,
                            "content": [[{"tag": "text", "text": content}]]
                        }
                    }
                }
            }

            async with session.post(self.webhook_url, json=payload) as resp:
                result = await resp.json()
                if result.get("code") != 0:
                    logger.error(f"Feishu send failed: {result}")

        except Exception as e:
            logger.error(f"Feishu error: {e}")

    async def send_card(self, title: str, fields: dict, color: str = "blue"):
        """发送卡片消息"""
        if not self.enabled:
            return

        try:
            session = await self._get_session()

            # 构建字段内容
            field_elements = []
            for key, value in fields.items():
                field_elements.append({
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**{key}**\n{value}"
                    }
                })

            # 飞书卡片消息
            payload = {
                "msg_type": "interactive",
                "card": {
                    "header": {
                        "title": {"tag": "plain_text", "content": title},
                        "template": color  # blue, green, red, orange
                    },
                    "elements": [
                        {
                            "tag": "div",
                            "fields": field_elements
                        },
                        {
                            "tag": "note",
                            "elements": [
                                {"tag": "plain_text", "content": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                            ]
                        }
                    ]
                }
            }

            async with session.post(self.webhook_url, json=payload) as resp:
                result = await resp.json()
                if result.get("code") != 0:
                    logger.error(f"Feishu card send failed: {result}")

        except Exception as e:
            logger.error(f"Feishu card error: {e}")

    async def notify_trade(
        self,
        pair_name: str,
        signal: str,
        xyz_price: float,
        lighter_price: float,
        zscore: float,
        size: float,
        spread: float,
    ):
        """交易通知"""
        color_map = {
            "LONG": "green",
            "SHORT": "red",
            "EXIT_LONG": "orange",
            "EXIT_SHORT": "orange",
        }
        color = color_map.get(signal, "blue")

        title = f"📊 {signal} {pair_name}"

        fields = {
            "信号": signal,
            "交易对": pair_name,
            "XYZ价格": f"${xyz_price:.4f}",
            "Lighter价格": f"${lighter_price:.4f}",
            "价差": f"${spread:.4f}",
            "Z-score": f"{zscore:.2f}",
            "数量": f"{size:.4f}",
        }

        await self.send_card(title, fields, color)

    async def notify_trade_result(
        self,
        pair_name: str,
        signal: str,
        xyz_fill_price: float,
        lighter_fill_price: float,
        size: float,
        fees: float,
        status: str,
    ):
        """交易成交结果通知"""
        color = "green" if status == "filled" else "red"
        title = f"✅ 成交确认 - {pair_name}" if status == "filled" else f"❌ 交易失败 - {pair_name}"

        fields = {
            "状态": status,
            "信号": signal,
            "XYZ成交价": f"${xyz_fill_price:.4f}",
            "Lighter成交价": f"${lighter_fill_price:.4f}",
            "成交数量": f"{size:.4f}",
            "手续费": f"${fees:.4f}",
        }

        await self.send_card(title, fields, color)

    async def notify_error(self, error: str, pair_name: str = ""):
        """错误通知"""
        title = "⚠️ 错误警告"
        fields = {
            "交易对": pair_name or "N/A",
            "错误信息": error,
        }
        await self.send_card(title, fields, "red")

    async def notify_daily_summary(
        self,
        date: str,
        trade_count: int,
        total_pnl: float,
        win_count: int,
        loss_count: int,
        total_fees: float,
        max_drawdown: float,
        positions: dict,
    ):
        """每日盈利汇总"""
        win_rate = win_count / trade_count * 100 if trade_count > 0 else 0
        color = "green" if total_pnl > 0 else "red"

        title = f"📈 每日汇总 - {date}" if total_pnl >= 0 else f"📉 每日汇总 - {date}"

        fields = {
            "交易次数": str(trade_count),
            "总盈亏": f"${total_pnl:.2f}",
            "胜率": f"{win_rate:.1f}%",
            "胜/负": f"{win_count}/{loss_count}",
            "总手续费": f"${total_fees:.2f}",
            "最大回撤": f"${max_drawdown:.2f}",
        }

        # 添加当前持仓信息
        if positions:
            pos_str = "\n".join([f"{k}: {v}" for k, v in positions.items()])
            fields["当前持仓"] = pos_str
        else:
            fields["当前持仓"] = "无"

        await self.send_card(title, fields, color)

    async def notify_emergency(self, reason: str):
        """紧急停止通知"""
        title = "🚨 紧急停止"
        fields = {
            "原因": reason,
            "状态": "所有交易已暂停",
            "操作": "请检查后手动重启",
        }
        await self.send_card(title, fields, "red")

    async def notify_startup(self, pairs: list, capital: float):
        """启动通知"""
        title = "🚀 套利机器人启动"
        fields = {
            "交易对": ", ".join(pairs),
            "总资金": f"${capital:,.0f}",
            "策略": "XYZ-Lighter 价差套利",
            "状态": "运行中",
        }
        await self.send_card(title, fields, "blue")

    async def notify_position_update(
        self,
        pair_name: str,
        position_type: str,  # LONG/SHORT
        entry_spread: float,
        current_spread: float,
        unrealized_pnl: float,
        hold_hours: float,
    ):
        """持仓更新通知（可选，用于定期汇报持仓状态）"""
        color = "green" if unrealized_pnl > 0 else "orange"
        title = f"📋 持仓更新 - {pair_name}"

        fields = {
            "方向": position_type,
            "入场价差": f"${entry_spread:.4f}",
            "当前价差": f"${current_spread:.4f}",
            "浮动盈亏": f"${unrealized_pnl:.2f}",
            "持仓时间": f"{hold_hours:.1f}小时",
        }

        await self.send_card(title, fields, color)
