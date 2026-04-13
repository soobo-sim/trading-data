"""Common error handling utilities for CoinMarket Data service"""
import logging
import httpx
from functools import wraps
from typing import Callable, Any
from fastapi import HTTPException
from app.core.exceptions import MarketDataAPIError

logger = logging.getLogger(__name__)


def handle_api_errors(operation: str):
    """외부 API 오류를 HTTPException으로 변환하는 데코레이터"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            try:
                return await func(*args, **kwargs)
            except HTTPException:
                raise  # FastAPI가 직접 처리하도록 re-raise
            except httpx.HTTPStatusError as e:
                logger.error(f"{operation} HTTP error: {e.response.status_code} - {e.response.text}")
                raise MarketDataAPIError(
                    detail=f"거래소 API 오류: {e.response.status_code}",
                    status_code=e.response.status_code if 400 <= e.response.status_code < 600 else 502,
                    operation=operation,
                )
            except httpx.TimeoutException:
                logger.error(f"{operation} timeout")
                raise MarketDataAPIError(detail="거래소 API 타임아웃", status_code=504, operation=operation)
            except MarketDataAPIError:
                raise
            except Exception as e:
                logger.error(f"{operation} unexpected error: {e}", exc_info=True)
                raise MarketDataAPIError(detail=f"예상치 못한 오류: {e}", status_code=500, operation=operation)
        return wrapper
    return decorator
