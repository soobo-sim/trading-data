"""
Coincheck Public WebSocket 클라이언트
coinmarket-data 서비스용 — coincheck-trader에서 이관
체결 내역(trades)과 호가창(orderbook) 실시간 수신
"""
import asyncio
import json
import logging
import time
from collections import deque
from typing import Optional
from datetime import datetime, timezone

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_ws_client: Optional["CoincheckWSClient"] = None


class CoincheckWSClient:
    """Coincheck Public WebSocket 클라이언트"""

    def __init__(self):
        settings = get_settings()
        self._ws_url = settings.COINCHECK_WS_URL
        self._pairs = settings.ck_ws_pairs_list
        self._max_trades = settings.WS_MAX_TRADES
        self._reconnect_max_delay = settings.WS_RECONNECT_MAX_DELAY

        self._trades: dict[str, deque] = {
            pair: deque(maxlen=self._max_trades) for pair in self._pairs
        }
        self._orderbooks: dict[str, Optional[dict]] = {pair: None for pair in self._pairs}
        self._subscribers: dict[str, list[asyncio.Queue]] = {}
        self._connected: bool = False
        self._ws = None
        self._running: bool = False
        self._connected_at: Optional[float] = None

    # ── 공개 인터페이스 ────────────────────────────────────────

    def get_recent_trades(self, pair: str, window_sec: int = 30) -> list[dict]:
        if pair not in self._trades:
            return []
        cutoff = time.time() - window_sec
        return [t for t in self._trades[pair] if t["timestamp"] >= cutoff]

    def get_latest_orderbook(self, pair: str) -> Optional[dict]:
        return self._orderbooks.get(pair)

    def get_status(self) -> dict:
        uptime_sec = None
        if self._connected and self._connected_at:
            uptime_sec = int(time.time() - self._connected_at)
        return {
            "connected": self._connected,
            "subscribed_pairs": self._pairs,
            "uptime_sec": uptime_sec,
            "trade_buffer_size": {pair: len(self._trades[pair]) for pair in self._pairs},
            "orderbook_available": {pair: self._orderbooks[pair] is not None for pair in self._pairs},
        }

    def subscribe(self, channel: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=200)
        if channel not in self._subscribers:
            self._subscribers[channel] = []
        self._subscribers[channel].append(q)
        return q

    def unsubscribe(self, channel: str, q: asyncio.Queue):
        if channel in self._subscribers:
            try:
                self._subscribers[channel].remove(q)
            except ValueError:
                pass

    async def add_pair(self, pair: str):
        if pair in self._pairs:
            return
        self._pairs.append(pair)
        self._trades[pair] = deque(maxlen=self._max_trades)
        self._orderbooks[pair] = None
        if self._connected and self._ws:
            try:
                await self._ws.send(json.dumps({"type": "subscribe", "channel": f"{pair}-trades"}))
                await self._ws.send(json.dumps({"type": "subscribe", "channel": f"{pair}-orderbook"}))
                logger.info(f"Dynamic subscription added: {pair}")
            except Exception as e:
                logger.warning(f"Failed to subscribe {pair} on live WS: {e}")

    # ── 백그라운드 실행 ─────────────────────────────────────────

    async def run(self):
        self._running = True
        delay = 1
        logger.info(f"Coincheck WS client starting. Pairs: {self._pairs}")
        while self._running:
            try:
                await self._connect_and_receive()
                delay = 1
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connected = False
                logger.warning(f"WS disconnected: {type(e).__name__}: {e}. Reconnecting in {delay}s...")
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._reconnect_max_delay)
        logger.info("Coincheck WS client stopped")

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()

    # ── 내부 구현 ────────────────────────────────────────────────

    async def _connect_and_receive(self):
        async with websockets.connect(self._ws_url, ping_interval=20, ping_timeout=10, close_timeout=5) as ws:
            self._ws = ws
            self._connected = True
            self._connected_at = time.time()
            logger.info(f"Coincheck WS connected: {self._ws_url}")
            for pair in self._pairs:
                await ws.send(json.dumps({"type": "subscribe", "channel": f"{pair}-trades"}))
                await ws.send(json.dumps({"type": "subscribe", "channel": f"{pair}-orderbook"}))
            async for raw in ws:
                if not self._running:
                    break
                try:
                    await self._handle_message(raw)
                except Exception as e:
                    logger.warning(f"WS message handling error: {e}")
        self._connected = False

    async def _handle_message(self, raw: str):
        data = json.loads(raw)
        # 체결 내역: [[ts, id, pair, rate, amount, order_type, ...], ...]
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list):
            for trade_arr in data:
                await self._handle_trade(trade_arr)
            return
        # 호가창: [pair, {bids: [...], asks: [...]}]
        if isinstance(data, list) and len(data) == 2 and isinstance(data[1], dict):
            await self._handle_orderbook(data[0], data[1])

    async def _handle_trade(self, trade_arr: list):
        if len(trade_arr) < 6:
            return
        try:
            trade = {
                "timestamp": float(trade_arr[0]),
                "id": str(trade_arr[1]),
                "pair": str(trade_arr[2]),
                "rate": str(trade_arr[3]),
                "amount": str(trade_arr[4]),
                "order_type": str(trade_arr[5]),
            }
        except (IndexError, ValueError, TypeError):
            return
        pair = trade["pair"]
        if pair not in self._trades:
            self._trades[pair] = deque(maxlen=self._max_trades)
        self._trades[pair].append(trade)
        await self._broadcast(f"{pair}-trades", trade)

    async def _handle_orderbook(self, pair: str, orderbook: dict):
        new_bids = orderbook.get("bids", [])
        new_asks = orderbook.get("asks", [])
        existing = self._orderbooks.get(pair)
        if existing is None:
            bids_dict = {b[0]: b[1] for b in new_bids if len(b) >= 2}
            asks_dict = {a[0]: a[1] for a in new_asks if len(a) >= 2}
        else:
            bids_dict = {b[0]: b[1] for b in existing.get("bids", [])}
            asks_dict = {a[0]: a[1] for a in existing.get("asks", [])}
            for b in new_bids:
                if len(b) >= 2:
                    if b[1] == "0":
                        bids_dict.pop(b[0], None)
                    else:
                        bids_dict[b[0]] = b[1]
            for a in new_asks:
                if len(a) >= 2:
                    if a[1] == "0":
                        asks_dict.pop(a[0], None)
                    else:
                        asks_dict[a[0]] = a[1]

        sorted_bids = sorted(bids_dict.items(), key=lambda x: float(x[0]), reverse=True)
        sorted_asks = sorted(asks_dict.items(), key=lambda x: float(x[0]))
        merged = {
            "bids": [[p, v] for p, v in sorted_bids],
            "asks": [[p, v] for p, v in sorted_asks],
            "last_update_at": orderbook.get("last_update_at"),
        }
        self._orderbooks[pair] = merged
        await self._broadcast(f"{pair}-orderbook", merged)

    async def _broadcast(self, channel: str, data: dict):
        for q in self._subscribers.get(channel, []):
            try:
                q.put_nowait(data)
            except asyncio.QueueFull:
                pass


def get_coincheck_ws_client() -> CoincheckWSClient:
    global _ws_client
    if _ws_client is None:
        _ws_client = CoincheckWSClient()
    return _ws_client


async def close_coincheck_ws_client():
    global _ws_client
    if _ws_client is not None:
        await _ws_client.stop()
        _ws_client = None
