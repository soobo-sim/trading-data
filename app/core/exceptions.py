"""Custom exceptions for CoinMarket Data service"""
from fastapi import HTTPException, status
from typing import Optional


class MarketDataAPIError(HTTPException):
    """외부 거래소 API 호출 실패"""
    def __init__(
        self,
        detail: str,
        status_code: int = status.HTTP_502_BAD_GATEWAY,
        operation: Optional[str] = None,
    ):
        if operation:
            detail = f"{operation} 실패: {detail}"
        super().__init__(status_code=status_code, detail=detail)
