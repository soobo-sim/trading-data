"""
Market Pulse 서비스 — Coincheck WS 기반 시장 분석
coinmarket-data 서비스용 — coincheck-trader에서 이관 (trading_pattern 의존 제거)
"""
import logging
import statistics
from typing import Optional

logger = logging.getLogger(__name__)


def calculate_market_pulse(
    trades: list[dict],
    orderbook: Optional[dict],
    pair: str,
    window_sec: int,
) -> dict:
    """최근 trades와 orderbook을 분석해 Market Pulse 지표 계산"""
    trade_count = len(trades)
    has_trades = trade_count > 0

    price_change_pct = 0.0
    price_direction = "sideways"
    first_price = last_price = price_high = price_low = None

    if has_trades:
        prices = [float(t["rate"]) for t in trades]
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

    buy_count = sum(1 for t in trades if t.get("order_type") == "buy")
    sell_count = sum(1 for t in trades if t.get("order_type") == "sell")
    buy_sell_ratio = None
    if sell_count > 0:
        buy_sell_ratio = round(buy_count / sell_count, 3)
    elif buy_count > 0:
        buy_sell_ratio = 99.999

    buy_sell_pressure = "neutral"
    if buy_sell_ratio is not None:
        if buy_sell_ratio > 1.5:
            buy_sell_pressure = "strong_buy"
        elif buy_sell_ratio > 1.1:
            buy_sell_pressure = "buy"
        elif buy_sell_ratio < 0.67:
            buy_sell_pressure = "strong_sell"
        elif buy_sell_ratio < 0.9:
            buy_sell_pressure = "sell"

    orderbook_imbalance = bid_total_volume = ask_total_volume = None
    best_bid = best_ask = spread = spread_pct = None

    if orderbook:
        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])
        top_bids = bids[:20]
        top_asks = asks[:20]
        bid_total_volume = sum(float(b[1]) for b in top_bids if len(b) >= 2)
        ask_total_volume = sum(float(a[1]) for a in top_asks if len(a) >= 2)
        total_volume = bid_total_volume + ask_total_volume
        if total_volume > 0:
            orderbook_imbalance = round((bid_total_volume - ask_total_volume) / total_volume, 4)
        if bids and asks:
            best_bid = float(bids[0][0]) if bids[0] else None
            best_ask = float(asks[0][0]) if asks[0] else None
            if best_bid and best_ask:
                spread = round(best_ask - best_bid, 1)
                mid_price = (best_bid + best_ask) / 2
                spread_pct = round(spread / mid_price * 100, 4) if mid_price > 0 else None

    trade_frequency_per_min = round(trade_count / window_sec * 60, 1) if window_sec > 0 else 0
    activity_level = "no_data"
    if has_trades:
        if trade_frequency_per_min >= 20:
            activity_level = "very_active"
        elif trade_frequency_per_min >= 10:
            activity_level = "active"
        elif trade_frequency_per_min >= 3:
            activity_level = "normal"
        else:
            activity_level = "quiet"

    ws_data_available = has_trades or orderbook is not None
    low_liquidity_warning = (not has_trades) and (orderbook is not None)

    price_from_window_high_pct = (
        round((last_price - price_high) / price_high * 100, 4) if has_trades and price_high else None
    )
    price_from_window_low_pct = (
        round((last_price - price_low) / price_low * 100, 4) if has_trades and price_low else None
    )

    return {
        "pair": pair,
        "window_sec": window_sec,
        "ws_data_available": ws_data_available,
        "last_price": last_price,
        "price_change_pct": price_change_pct,
        "price_direction": price_direction,
        "price_range": {"high": price_high, "low": price_low} if has_trades else None,
        "price_from_window_high_pct": price_from_window_high_pct,
        "price_from_window_low_pct": price_from_window_low_pct,
        "volatility": volatility,
        "volatility_pct": volatility_pct,
        "trade_count": trade_count,
        "trade_frequency_per_min": trade_frequency_per_min,
        "activity_level": activity_level,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "buy_sell_ratio": buy_sell_ratio,
        "buy_sell_pressure": buy_sell_pressure,
        "low_liquidity_warning": low_liquidity_warning,
        "orderbook_imbalance": orderbook_imbalance,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "spread_pct": spread_pct,
        "bid_volume_top20": round(bid_total_volume, 6) if bid_total_volume is not None else None,
        "ask_volume_top20": round(ask_total_volume, 6) if ask_total_volume is not None else None,
    }
