"""
BitFlyer Public WebSocket 클라이언트 — JSON-RPC 2.0 프로토콜
trading-data 서비스용

멀티 product 지원 (BF_WS_PRODUCTS 환경변수)
  예: BF_WS_PRODUCTS=BTC_JPY,ETH_JPY,XRP_JPY

각 product마다 3개 채널 구독:
  lightning_ticker_{PRODUCT}
  lightning_executions_{PRODUCT}
  lightning_board_snapshot_{PRODUCT}

체결 틱은 "{product}-executions" 채널 Queue로 발행
→ BfCandlePipeline이 소비하여 캔들 집계
"""
import asyncio
import json
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_ws_client: Optional["BitFlyerWSClient"] = None


class BitFlyerWSClient:
    """BitFlyer Public WebSocket 클라이언트 (JSON-RPC 2.0, 멀티 product)"""

    def __init__(self):
        settings = get_settings()
        self.ws_url = settings.BITFLYER_WS_URL
        self.products = settings.bf_ws_products_list          # ["BTC_JPY", "ETH_JPY", "XRP_JPY"]
        self.window_sec = settings.WS_WINDOW_SEC
        self.max_trades = settings.WS_MAX_TRADES
        self.reconnect_max_delay = settings.WS_RECONNECT_MAX_DELAY

        # product별 거래 버퍼 (market-pulse용)
        self.trades: dict[str, deque] = {
            pc: deque(maxlen=self.max_trades) for pc in self.products
        }
        # product별 최신 ticker / board
        self.latest_ticker: dict[str, Optional[dict]] = {pc: None for pc in self.products}
        self.latest_board: dict[str, Optional[dict]] = {pc: None for pc in self.products}

        # 채널별 subscriber Queue (캔들 파이프라인 연결용)
        self._channel_subscribers: dict[str, list[asyncio.Queue]] = {}
        # 전체 메시지 subscriber (기존 호환)
        self._subscribers: list[asyncio.Queue] = []

        self._ws = None
        self._running = False
        self._rpc_id = 0
        self._connected = False

    # ── 구독 인터페이스 ────────────────────────────────────────

    def subscribe_executions(self, product_code: str) -> asyncio.Queue:
        """특정 product의 체결 틱만 구독 (캔들 파이프라인용)"""
        channel = f"{product_code.upper()}-executions"
        q: asyncio.Queue = asyncio.Queue(maxsize=500)
        if channel not in self._channel_subscribers:
            self._channel_subscribers[channel] = []
        self._channel_subscribers[channel].append(q)
        return q

    def unsubscribe_executions(self, product_code: str, q: asyncio.Queue):
        channel = f"{product_code.upper()}-executions"
        if channel in self._channel_subscribers:
            try:
                self._channel_subscribers[channel].remove(q)
            except ValueError:
                pass

    # ── 조회 ──────────────────────────────────────────────────

    def get_recent_trades(self, seconds: int = 30, product_code: Optional[str] = None) -> list:
        """product_code 미지정 시 첫 번째 product 사용"""
        pc = (product_code or self.products[0]).upper()
        buf = self.trades.get(pc, deque())
        cutoff = datetime.now(timezone.utc).timestamp() - seconds
        return [t for t in buf if t.get("_ts", 0) >= cutoff]

    def get_latest_ticker(self, product_code: Optional[str] = None) -> Optional[dict]:
        pc = (product_code or self.products[0]).upper()
        return self.latest_ticker.get(pc)

    def get_latest_board(self, product_code: Optional[str] = None) -> Optional[dict]:
        pc = (product_code or self.products[0]).upper()
        return self.latest_board.get(pc)

    def get_fx_spread(self) -> Optional[float]:
        """
        FX_BTC_JPY vs BTC_JPY ticker 스프레드 계산 (SFD 레벨 판단용).
        두 product 모두 ticker가 있을 때만 계산. 없으면 None 반환.
        반환값: FX 프리미엄 비율 (%) — 양수=FX가 현물보다 비쌈, 음수=FX가 쌈.
        """
        fx_ticker = self.latest_ticker.get("FX_BTC_JPY")
        spot_ticker = self.latest_ticker.get("BTC_JPY")
        if not fx_ticker or not spot_ticker:
            return None
        fx_ltp = fx_ticker.get("ltp") or fx_ticker.get("last")
        spot_ltp = spot_ticker.get("ltp") or spot_ticker.get("last")
        if not fx_ltp or not spot_ltp or float(spot_ltp) == 0:
            return None
        return round((float(fx_ltp) - float(spot_ltp)) / float(spot_ltp) * 100, 4)

    def get_status(self) -> dict:
        return {
            "connected": self._connected,
            "subscribed_products": self.products,
            "trade_buffer_size": {pc: len(buf) for pc, buf in self.trades.items()},
            "ticker_available": {pc: t is not None for pc, t in self.latest_ticker.items()},
        }

    # ── 내부 ──────────────────────────────────────────────────

    def _next_id(self) -> int:
        self._rpc_id += 1
        return self._rpc_id

    async def _publish(self, message: dict):
        for q in self._subscribers:
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:
                pass

    async def _publish_to_channel(self, channel: str, message: dict):
        for q in self._channel_subscribers.get(channel, []):
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:
                pass

    async def _subscribe_channels(self, ws):
        channels = []
        for pc in self.products:
            channels += [
                f"lightning_ticker_{pc}",
                f"lightning_executions_{pc}",
                f"lightning_board_snapshot_{pc}",
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

        if data.get("method") != "channelMessage":
            return

        params = data.get("params", {})
        channel: str = params.get("channel", "")
        message = params.get("message")
        if not channel or message is None:
            return

        if channel.startswith("lightning_ticker_"):
            pc = channel[len("lightning_ticker_"):]
            if pc in self.latest_ticker:
                self.latest_ticker[pc] = message
            await self._publish({"type": "ticker", "channel": channel, "data": message})

        elif channel.startswith("lightning_executions_"):
            pc = channel[len("lightning_executions_"):]
            now = datetime.now(timezone.utc).timestamp()
            buf = self.trades.get(pc)
            exec_list = message if isinstance(message, list) else []
            if buf is not None:
                for item in exec_list:
                    buf.append({**item, "_ts": now, "_product": pc})
            await self._publish({"type": "executions", "channel": channel, "data": message})
            # 캔들 파이프라인용 채널 발행
            ch_key = f"{pc}-executions"
            if self._channel_subscribers.get(ch_key):
                for item in exec_list:
                    await self._publish_to_channel(ch_key, {**item, "_ts": now, "_product": pc})

        elif channel.startswith("lightning_board_snapshot_"):
            pc = channel[len("lightning_board_snapshot_"):]
            if pc in self.latest_board:
                self.latest_board[pc] = message
            await self._publish({"type": "board_snapshot", "channel": channel, "data": message})

        elif channel.startswith("lightning_board_"):
            await self._publish({"type": "board_diff", "channel": channel, "data": message})

    async def connect(self):
        self._running = True
        delay = 1
        while self._running:
            try:
                logger.info(f"BitFlyer WS 연결 시도: {self.ws_url} products={self.products}")
                async with websockets.connect(self.ws_url, ping_interval=30, ping_timeout=10) as ws:
                    self._ws = ws
                    self._connected = True
                    delay = 1
                    logger.info(f"BitFlyer WS 연결 성공. products={self.products}")
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
            finally:
                self._connected = False
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
