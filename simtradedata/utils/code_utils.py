"""
Utility functions for stock code conversion and date handling
"""

from datetime import datetime, timedelta


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


def parse_date(date_str: str) -> datetime:
    """
    Parse date string to datetime object

    Supports formats: 'YYYY-MM-DD', 'YYYYMMDD'

    Args:
        date_str: Date string

    Returns:
        datetime object
    """
    if "-" in date_str:
        return datetime.strptime(date_str, "%Y-%m-%d")
    return datetime.strptime(date_str, "%Y%m%d")


def format_date(dt: datetime, format_type: str = "dash") -> str:
    """
    Format datetime to string

    Args:
        dt: datetime object
        format_type: 'dash' for YYYY-MM-DD, 'compact' for YYYYMMDD

    Returns:
        Formatted date string
    """
    if format_type == "dash":
        return dt.strftime("%Y-%m-%d")
    elif format_type == "compact":
        return dt.strftime("%Y%m%d")
    return dt.strftime("%Y-%m-%d")


def get_trading_dates(start_date: str, end_date: str) -> list[str]:
    """
    Generate list of potential trading dates (excluding weekends)

    Note: This is a simple implementation. For accurate trading calendar,
    use trading calendar data from data sources.

    Args:
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)

    Returns:
        List of date strings
    """
    start = parse_date(start_date)
    end = parse_date(end_date)

    dates = []
    current = start

    while current <= end:
        # Exclude weekends
        if current.weekday() < 5:  # Monday=0, Friday=4
            dates.append(format_date(current))
        current += timedelta(days=1)

    return dates


def chunk_list(items: list, chunk_size: int) -> list[list]:
    """
    Split list into chunks

    Args:
        items: List to split
        chunk_size: Size of each chunk

    Returns:
        List of chunks
    """
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def retry_on_failure(func, max_retries: int = 3, delay: float = 5.0):
    """
    Decorator for retrying function on failure

    Args:
        func: Function to retry
        max_retries: Maximum number of retries
        delay: Delay between retries in seconds

    Returns:
        Decorated function
    """
    import time
    from functools import wraps

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
                else:
                    raise last_exception

    return wrapper
