"""
Utility functions for stock code conversion
"""

from functools import wraps
import time


def convert_to_ptrade_code(code: str, source: str = "baostock") -> str:
    """
    Convert stock code from various sources to PTrade format

    Args:
        code: Stock code in source format
        source: Data source name ('baostock', 'qstock', 'yahoo')

    Returns:
        Stock code in PTrade format (e.g., '600000.SS', '000001.SZ')

    Note:
        PTrade/SimTradeLab uses:
        - Shanghai stocks: .SS (not .SH)
        - Shenzhen stocks: .SZ

    Examples:
        >>> convert_to_ptrade_code('sh.600000', 'baostock')
        '600000.SS'
        >>> convert_to_ptrade_code('000001', 'qstock')
        '000001.SZ'
    """
    if source == "baostock":
        # BaoStock format: sh.600000, sz.000001
        if "." in code:
            market, symbol = code.split(".")
            # Map to SimTradeLab format: SS for Shanghai, SZ for Shenzhen
            market_map = {"sh": "SS", "sz": "SZ"}
            return f"{symbol}.{market_map[market.lower()]}"
        return code

    elif source == "qstock":
        # QStock format: 600000, 000001
        # Determine market by code prefix
        if code.startswith("6") or code.startswith("5"):
            return f"{code}.SS"  # Shanghai uses .SS
        elif code.startswith("0") or code.startswith("3"):
            return f"{code}.SZ"
        return code

    elif source == "yahoo":
        # Yahoo format: 600000.SS (Shanghai), 000001.SZ (Shenzhen)
        # Yahoo already uses .SS, so no conversion needed
        return code

    return code


def convert_from_ptrade_code(code: str, target_source: str) -> str:
    """
    Convert PTrade format code to target source format

    Args:
        code: Stock code in PTrade format (e.g., '600000.SS')
        target_source: Target source name ('baostock', 'qstock', 'yahoo', 'mootdx')

    Returns:
        Stock code in target source format

    Examples:
        >>> convert_from_ptrade_code('600000.SS', 'baostock')
        'sh.600000'
        >>> convert_from_ptrade_code('000001.SZ', 'qstock')
        '000001'
        >>> convert_from_ptrade_code('000001.SZ', 'mootdx')
        '000001'
    """
    if "." not in code:
        return code

    symbol, market = code.split(".")

    if target_source == "baostock":
        # Map SS back to sh for BaoStock
        market_map = {"SS": "sh", "SZ": "sz", "SH": "sh"}  # Support both SS and SH
        return f"{market_map.get(market, market.lower())}.{symbol}"

    elif target_source in ("qstock", "mootdx"):
        # Both qstock and mootdx use simple code format (e.g., '000001')
        return symbol

    elif target_source == "yahoo":
        # Yahoo uses .SS for Shanghai (same as PTrade)
        return code

    return code


def get_mootdx_market(symbol: str) -> int:
    """
    Convert PTrade code to mootdx market code.

    Args:
        symbol: Stock code in PTrade format (e.g., '000001.SZ', '600000.SS')

    Returns:
        0 for Shenzhen (codes starting with 0/1/2/3)
        1 for Shanghai (codes starting with 5/6/7/8/9)

    Examples:
        >>> get_mootdx_market('000001.SZ')
        0
        >>> get_mootdx_market('600000.SS')
        1
    """
    code = symbol.split(".")[0] if "." in symbol else symbol
    return 0 if code[0] in "0123" else 1


def retry_on_failure(max_retries: int = 1, delay: float = 0.0):
    """
    Decorator factory for retrying a function on failure.

    Args:
        max_retries (int): Maximum number of retries.
        delay (float): Delay between retries in seconds.

    Returns:
        A decorator.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        time.sleep(delay)
            raise last_exception

        return wrapper

    return decorator
