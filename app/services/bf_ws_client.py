"""
BitFlyer Public WebSocket 클라이언트 — JSON-RPC 2.0 프로토콜
coinmarket-data 서비스용 — bitflyer-trader에서 이관
"""
import asyncio
import json
import logging
from collections import deque
from datetime import datetime
from typing import Optional

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_ws_client: Optional["BitFlyerWSClient"] = None


class BitFlyerWSClient:
    """BitFlyer Public WebSocket 클라이언트 (JSON-RPC 2.0)"""

    def __init__(self):
        settings = get_settings()
        self.ws_url = settings.BITFLYER_WS_URL
        self.product_code = settings.BITFLYER_PRODUCT_CODE.upper()
        self.window_sec = settings.WS_WINDOW_SEC
        self.max_trades = settings.WS_MAX_TRADES
        self.reconnect_max_delay = settings.WS_RECONNECT_MAX_DELAY

        self.trades: deque = deque(maxlen=self.max_trades)
        self.latest_ticker: Optional[dict] = None
        self.latest_board: Optional[dict] = None

        self._subscribers: list[asyncio.Queue] = []
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._rpc_id = 0

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        if q in self._subscribers:
            self._subscribers.remove(q)

    async def _publish(self, message: dict):
        for q in self._subscribers:
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:
                pass

    def _next_id(self) -> int:
        self._rpc_id += 1
        return self._rpc_id

    async def _subscribe_channels(self, ws):
        channels = [
            f"lightning_ticker_{self.product_code}",
            f"lightning_executions_{self.product_code}",
            f"lightning_board_snapshot_{self.product_code}",
        ]
        for channel in channels:
            msg = json.dumps({
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": {"channel": channel},
                "id": self._next_id(),
            })
            await ws.send(msg)
        logger.info(f"BitFlyer WS 채널 구독 요청: {channels}")

    async def _handle_message(self, raw: str):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return

        if "result" in data or "error" in data:
            if data.get("error"):
                logger.error(f"JSON-RPC 에러: {data['error']}")
            return

        if data.get("method") == "channelMessage":
            params = data.get("params", {})
            channel: str = params.get("channel", "")
            message = params.get("message")
            if not channel or message is None:
                return

            if channel.startswith("lightning_ticker_"):
                self.latest_ticker = message
                await self._publish({"type": "ticker", "channel": channel, "data": message})
            elif channel.startswith("lightning_executions_"):
                now = datetime.utcnow().timestamp()
                for exec_item in (message if isinstance(message, list) else []):
                    self.trades.append({**exec_item, "_ts": now})
                await self._publish({"type": "executions", "channel": channel, "data": message})
            elif channel.startswith("lightning_board_snapshot_"):
                self.latest_board = message
                await self._publish({"type": "board_snapshot", "channel": channel, "data": message})
            elif channel.startswith("lightning_board_"):
                await self._publish({"type": "board_diff", "channel": channel, "data": message})

    async def connect(self):
        self._running = True
        delay = 1
        while self._running:
            try:
                logger.info(f"BitFlyer WS 연결 시도: {self.ws_url}")
                async with websockets.connect(self.ws_url, ping_interval=30, ping_timeout=10) as ws:
                    self._ws = ws
                    delay = 1
                    logger.info("BitFlyer WS 연결 성공")
                    await self._subscribe_channels(ws)
                    async for raw in ws:
                        if not self._running:
                            break
                        await self._handle_message(raw)
            except ConnectionClosedOK:
                logger.info("BitFlyer WS 정상 종료")
                break
            except ConnectionClosedError as e:
                logger.warning(f"BitFlyer WS 연결 끊김: {e}. {delay}초 후 재연결...")
            except Exception as e:
                logger.error(f"BitFlyer WS 오류: {e}. {delay}초 후 재연결...")
            if self._running:
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.reconnect_max_delay)

    async def disconnect(self):
        self._running = False
        if self._ws and not self._ws.closed:
            try:
                await self._ws.close()
            except Exception:
                pass
        logger.info("BitFlyer WS 클라이언트 종료")

    def get_recent_trades(self, seconds: int = 30) -> list:
        cutoff = datetime.utcnow().timestamp() - seconds
        return [t for t in self.trades if t.get("_ts", 0) >= cutoff]


def get_bitflyer_ws_client() -> BitFlyerWSClient:
    global _ws_client
    if _ws_client is None:
        _ws_client = BitFlyerWSClient()
    return _ws_client


async def close_bitflyer_ws_client():
    global _ws_client
    if _ws_client is not None:
        await _ws_client.disconnect()
        _ws_client = None
