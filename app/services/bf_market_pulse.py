"""
Market Pulse 서비스 — BitFlyer WS 기반 시장 분석
coinmarket-data 서비스용 — bitflyer-trader에서 이관
"""
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def calculate_bf_market_pulse(
    trades: list[dict],
    orderbook: Optional[dict],
    product_code: str,
    window_sec: int,
    fx_spread_pct: Optional[float] = None,
) -> dict:
    """BitFlyer 체결 데이터 기반 Market Pulse 계산 (side=BUY/SELL, size 필드)"""
    trade_count = len(trades)
    has_trades = trade_count > 0

    price_change_pct = 0.0
    price_direction = "sideways"
    first_price = last_price = price_high = price_low = None

    if has_trades:
        prices = [float(t.get("price", 0)) for t in trades if t.get("price")]
        if prices:
            first_price = prices[0]
            last_price = prices[-1]
            price_high = max(prices)
            price_low = min(prices)
            if first_price > 0:
                price_change_pct = round((last_price - first_price) / first_price * 100, 4)
            if price_change_pct > 0.1:
                price_direction = "up"
            elif price_change_pct < -0.1:
                price_direction = "down"

    volatility = "unknown"
    volatility_pct = 0.0
    if has_trades and price_low and price_low > 0:
        volatility_pct = round((price_high - price_low) / price_low * 100, 4)
        if volatility_pct >= 0.3:
            volatility = "high"
        elif volatility_pct >= 0.1:
            volatility = "medium"
        else:
            volatility = "low"

    buy_volume = sum(float(t.get("size", 0)) for t in trades if t.get("side", "").upper() == "BUY")
    sell_volume = sum(float(t.get("size", 0)) for t in trades if t.get("side", "").upper() == "SELL")
    total_volume = buy_volume + sell_volume
    buy_pressure = round(buy_volume / total_volume * 100, 1) if total_volume > 0 else 50.0

    if buy_pressure > 60:
        market_sentiment = "bullish"
    elif buy_pressure < 40:
        market_sentiment = "bearish"
    else:
        market_sentiment = "neutral"

    orderbook_imbalance = 0.0
    if orderbook:
        bid_qty = sum(float(b.get("size", 0)) for b in (orderbook.get("bids") or [])[:10])
        ask_qty = sum(float(a.get("size", 0)) for a in (orderbook.get("asks") or [])[:10])
        total = bid_qty + ask_qty
        if total > 0:
            orderbook_imbalance = round((bid_qty - ask_qty) / total * 100, 1)

    trades_per_min = round(trade_count / window_sec * 60, 1) if window_sec > 0 else 0

    return {
        "product_code": product_code,
        "window_sec": window_sec,
        "trade_count": trade_count,
        "trades_per_minute": trades_per_min,
        "price_direction": price_direction,
        "price_change_pct": price_change_pct,
        "volatility": volatility,
        "volatility_pct": volatility_pct,
        "current_price": last_price,
        "price_high": price_high,
        "price_low": price_low,
        "buy_pressure_pct": buy_pressure,
        "market_sentiment": market_sentiment,
        "buy_volume": round(buy_volume, 6),
        "sell_volume": round(sell_volume, 6),
        "orderbook_imbalance": orderbook_imbalance,
        "fx_spread_pct": fx_spread_pct,
    }
