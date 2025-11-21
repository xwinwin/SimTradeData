"""
PTrade-compatible API interface using BaoStock as backend

All functions use BaoStockFetcher internally for consistency.
"""

import atexit
import logging
from datetime import datetime
from typing import Dict, List, Optional, Union

import pandas as pd

from simtradedata.fetchers.baostock_fetcher import BaoStockFetcher
from simtradedata.utils.code_utils import convert_to_ptrade_code

logger = logging.getLogger(__name__)

# Global fetcher instance
_fetcher = None


def _get_fetcher() -> BaoStockFetcher:
    """Get or create global BaoStock fetcher instance"""
    global _fetcher
    if _fetcher is None:
        _fetcher = BaoStockFetcher()
        _fetcher.login()
        atexit.register(_fetcher.logout)
    return _fetcher


def get_price(
    security: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    frequency: str = "1d",
    fields: Optional[List[str]] = None,
    fq: str = "none",
    **kwargs,
) -> Optional[pd.DataFrame]:
    """
    Get price data (PTrade compatible)

    Args:
        security: Stock code in PTrade format
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        frequency: '1d' for daily (BaoStock only supports daily)
        fields: List of fields ['open', 'high', 'low', 'close', 'volume', 'money']
        fq: 'none', 'pre', 'post'

    Returns:
        DataFrame with datetime index
    """
    if frequency != "1d":
        logger.warning(
            f"BaoStock only supports daily data, frequency='{frequency}' ignored"
        )
        return None

    if fields is None:
        fields = ["open", "high", "low", "close", "volume", "money"]

    # Map fq to adjustflag
    adjustflag_map = {"none": "3", "pre": "2", "post": "1"}
    adjustflag = adjustflag_map.get(fq, "3")

    try:
        fetcher = _get_fetcher()
        df = fetcher.fetch_market_data(
            symbol=security,
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag=adjustflag,
        )

        if df is None or df.empty:
            return None

        # Set date as index
        if "date" in df.columns:
            df = df.set_index("date")

        # Rename amount -> money
        if "amount" in df.columns:
            df = df.rename(columns={"amount": "money"})

        # Select requested fields
        available_fields = [f for f in fields if f in df.columns]
        if available_fields:
            df = df[available_fields]

        return df

    except Exception as e:
        logger.error(f"get_price failed for {security}: {e}")
        return None


def _get_stock_and_index(date: str = "") -> tuple[list[str], list[str]]:
    try:
        fetcher = _get_fetcher()
        df = fetcher.fetch_stock_list_by_date(date)

        if df is None or df.empty:
            return [], []

        # Filter: only A-shares that are listed
        df = df[df["tradeStatus"] != "0"]
        # Filter indeces which start with "sh.000" or "sz.399"

        df_stock = df[~df["code"].str.startswith(("sh.000", "sz.399"))]
        df_index = df[df["code"].str.startswith(("sh.000", "sz.399"))]

        # Convert to PTrade format
        stocks = [
            convert_to_ptrade_code(code, "baostock")
            for code in df_stock["code"].tolist()
        ]
        indeces = [
            convert_to_ptrade_code(code, "baostock")
            for code in df_index["code"].tolist()
        ]

        return stocks, indeces

    except Exception as e:
        logger.error(f"get_Ashares failed for {date}: {e}")
        return [], []


def get_Ashares(date: str = "") -> list[str]:
    """
    Get all A-share stock codes on a specific date (PTrade compatible)

    Args:
        date: Date string 'YYYYMMDD' or 'YYYY-MM-DD'

    Returns:
        List of stock codes in PTrade format
    """
    return _get_stock_and_index(date)[0]


def get_Indeces(date: str = "") -> list[str]:
    return _get_stock_and_index(date)[1]


def get_stock_info(
    security: Union[str, List[str]], field: Optional[List[str]] = None
) -> Optional[Dict]:
    """
    Get stock basic information (PTrade compatible)

    Args:
        security: Single stock or list of stocks
        field: ['stock_name', 'listed_date', 'de_listed_date']

    Returns:
        Dict: {stock: {field: value}}
    """
    if isinstance(security, str):
        securities = [security]
    else:
        securities = security

    if field is None:
        field = ["stock_name", "listed_date", "de_listed_date"]

    result = {}
    fetcher = _get_fetcher()

    for stock in securities:
        try:
            df = fetcher.fetch_stock_basic(stock)

            if df is None or df.empty:
                continue

            stock_info = {}

            # Map fields
            if "stock_name" in field and "code_name" in df.columns:
                stock_info["stock_name"] = df["code_name"].values[0]

            if "listed_date" in field and "ipoDate" in df.columns:
                ipo_date = df["ipoDate"].values[0]
                stock_info["listed_date"] = ipo_date if ipo_date else None

            if "de_listed_date" in field and "outDate" in df.columns:
                out_date = df["outDate"].values[0]
                stock_info["de_listed_date"] = out_date if out_date else None

            result[stock] = stock_info

        except Exception as e:
            logger.error(f"get_stock_info failed for {stock}: {e}")
            continue

    return result if result else None


def get_stock_blocks(security: str) -> Optional[Dict]:
    """
    Get stock industry/sector info (PTrade compatible)

    Args:
        security: Stock code

    Returns:
        Dict with industry classification
    """
    try:
        fetcher = _get_fetcher()
        industry_df = fetcher.fetch_stock_industry(security)

        if industry_df is None or industry_df.empty:
            return {}

        blocks = {}

        if "industry" in industry_df.columns:
            blocks["industry"] = industry_df["industry"].values[0]

        if "industryClassification" in industry_df.columns:
            blocks["industry_classification"] = industry_df[
                "industryClassification"
            ].values[0]

        return blocks

    except Exception as e:
        logger.error(f"get_stock_blocks failed for {security}: {e}")
        return {}


def get_stock_exrights(security: str) -> Optional[pd.DataFrame]:
    """
    Get ex-rights data (PTrade compatible)

    Args:
        security: Stock code

    Returns:
        DataFrame with adjust factors
    """
    try:
        fetcher = _get_fetcher()
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = "2017-01-01"

        df = fetcher.fetch_adjust_factor(security, start_date, end_date)

        # Rename 'date' column to match PTrade format if needed
        if df is not None and "date" in df.columns:
            df = df.rename(columns={"date": "dividOperateDate"})

        return df

    except Exception as e:
        logger.error(f"get_stock_exrights failed for {security}: {e}")
        return None


def get_trade_days(start_date: str, end_date: str) -> Optional[List]:
    """
    Get trading days (PTrade compatible)

    Args:
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD

    Returns:
        List of trading days
    """
    try:
        fetcher = _get_fetcher()
        df = fetcher.fetch_trade_calendar(start_date, end_date)

        if df is None or df.empty:
            return []

        # Return as list of datetime objects
        # Filter only trading days
        trading_days = df.loc[df["is_trading_day"] == "1", "calendar_date"].tolist()

        return trading_days

    except Exception as e:
        logger.error(f"get_trade_days failed: {e}")
        return []


def get_all_trades_days(start_date: str, end_date: str) -> Optional[List]:
    """Alias for get_trade_days (PTrade compatible)"""
    return get_trade_days(start_date, end_date)


def get_stock_status(
    stocks: Union[str, List[str]],
    query_type: str = "ST",
    query_date: Optional[str] = None,
) -> Optional[Dict[str, bool]]:
    """
    Get stock status: ST/HALT/DELISTING (PTrade compatible)

    Args:
        stocks: Single stock or list of stocks
        query_type: 'ST', 'HALT', 'DELISTING'
        query_date: 'YYYYMMDD' or None for today

    Returns:
        Dict: {stock: True/False}
    """
    if isinstance(stocks, str):
        securities = [stocks]
    else:
        securities = stocks

    if query_date is None:
        query_date = datetime.now().strftime("%Y%m%d")
    else:
        if len(query_date) == 8:
            query_date = datetime.strptime(query_date, "%Y%m%d").strftime("%Y-%m-%d")

    result = {}
    fetcher = _get_fetcher()

    # Optimized path for HALT
    if query_type == "HALT":
        try:
            all_stocks_df = fetcher.fetch_stock_list_by_date(query_date)
            if all_stocks_df is not None and not all_stocks_df.empty:
                # Create a lookup map for tradeStatus
                status_map = {
                    convert_to_ptrade_code(row["code"], "baostock"): row[
                        "tradeStatus"
                    ]
                    == "0"
                    for _, row in all_stocks_df.iterrows()
                }
                # Populate result based on the map
                for stock in securities:
                    result[stock] = status_map.get(stock, False)
            else:
                # Fallback or set all to False if API fails
                for stock in securities:
                    result[stock] = False
            return result
        except Exception as e:
            logger.error(f"Optimized get_stock_status HALT failed: {e}")
            # Fallback to old method on error
            pass

    for stock in securities:
        try:
            if query_type == "ST":
                # Query isST field
                df = fetcher.fetch_market_data(
                    symbol=stock,
                    start_date=query_date,
                    end_date=query_date,
                    extra_fields=["isST"],
                )

                if df is not None and not df.empty and "isST" in df.columns:
                    is_st = str(df["isST"].values[0]) == "1"
                    result[stock] = is_st
                else:
                    result[stock] = False

            elif query_type == "HALT":
                # Query tradestatus field
                df = fetcher.fetch_market_data(
                    symbol=stock,
                    start_date=query_date,
                    end_date=query_date,
                    extra_fields=["tradestatus"],
                )

                if df is not None and not df.empty and "tradestatus" in df.columns:
                    is_halt = str(df["tradestatus"].values[0]) == "0"
                    result[stock] = is_halt
                else:
                    result[stock] = False

            elif query_type == "DELISTING":
                # Query from stock basic info
                basic_df = fetcher.fetch_stock_basic(stock)

                if basic_df is not None and not basic_df.empty:
                    status = str(basic_df["status"].values[0])
                    out_date = basic_df["outDate"].values[0]

                    is_delisted = status == "0"
                    if not is_delisted and out_date:
                        out_date_int = int(out_date.replace("-", ""))
                        query_date_int = int(query_date.replace("-", ""))
                        is_delisted = query_date_int > out_date_int

                    result[stock] = is_delisted
                else:
                    result[stock] = False

            else:
                logger.warning(f"Unknown query_type: {query_type}")
                result[stock] = False

        except Exception as e:
            logger.error(f"get_stock_status failed for {stock}: {e}")
            result[stock] = False

    return result


def get_fundamentals(
    stocks: Union[str, List[str]],
    table: str,
    fields: Optional[List[str]] = None,
    date: Optional[str] = None,
    start_year: Optional[str] = None,
    end_year: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """
    Get fundamental data (PTrade compatible)

    Args:
        stocks: Single stock or list
        table: 'valuation', 'profit_ability', 'growth_ability', 'operating_ability', 'debt_paying_ability'
        fields: List of fields
        date: Specific date (for valuation)
        start_year: Start year (for quarterly data)
        end_year: End year (for quarterly data)

    Returns:
        DataFrame with fundamental data
    """
    if isinstance(stocks, str):
        securities = [stocks]
    else:
        securities = stocks

    fetcher = _get_fetcher()

    if table == "valuation":
        # Valuation data (daily)
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        else:
            # Normalize date
            if len(date) == 8:
                date = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")

        result_dfs = []

        for stock in securities:
            try:
                df = fetcher.fetch_valuation_data(stock, date, date)

                if df is not None and not df.empty:
                    df["stock"] = stock
                    result_dfs.append(df)

            except Exception as e:
                logger.error(f"get_fundamentals valuation failed for {stock}: {e}")
                continue

        if result_dfs:
            combined = pd.concat(result_dfs)
            combined = combined.set_index("stock")
            return combined
        else:
            return None

    elif table == "profit_ability":
        # Profit data (quarterly)
        if start_year is None or end_year is None:
            logger.warning("start_year and end_year required for profit_ability")
            return None

        result_dfs = []

        for stock in securities:
            for year in range(int(start_year), int(end_year) + 1):
                for quarter in [1, 2, 3, 4]:
                    try:
                        df = fetcher.fetch_profit_data(stock, year, quarter)

                        if df is not None and not df.empty:
                            result_dfs.append(df)

                    except Exception as e:
                        logger.debug(
                            f"fetch_profit_data failed for {stock} {year}Q{quarter}: {e}"
                        )
                        continue

        if result_dfs:
            return pd.concat(result_dfs)
        else:
            return None

    elif table == "growth_ability":
        # Growth data (quarterly)
        if start_year is None or end_year is None:
            logger.warning("start_year and end_year required for growth_ability")
            return None

        result_dfs = []

        for stock in securities:
            for year in range(int(start_year), int(end_year) + 1):
                for quarter in [1, 2, 3, 4]:
                    try:
                        df = fetcher.fetch_growth_data(stock, year, quarter)

                        if df is not None and not df.empty:
                            result_dfs.append(df)

                    except Exception as e:
                        logger.debug(
                            f"fetch_growth_data failed for {stock} {year}Q{quarter}: {e}"
                        )
                        continue

        if result_dfs:
            return pd.concat(result_dfs)
        else:
            return None

    elif table == "operating_ability":
        # Operation data (quarterly)
        if start_year is None or end_year is None:
            logger.warning("start_year and end_year required for operating_ability")
            return None

        result_dfs = []

        for stock in securities:
            for year in range(int(start_year), int(end_year) + 1):
                for quarter in [1, 2, 3, 4]:
                    try:
                        df = fetcher.fetch_operation_data(stock, year, quarter)

                        if df is not None and not df.empty:
                            result_dfs.append(df)

                    except Exception as e:
                        logger.debug(
                            f"fetch_operation_data failed for {stock} {year}Q{quarter}: {e}"
                        )
                        continue

        if result_dfs:
            return pd.concat(result_dfs)
        else:
            return None

    elif table == "debt_paying_ability":
        # Balance data (quarterly)
        if start_year is None or end_year is None:
            logger.warning("start_year and end_year required for debt_paying_ability")
            return None

        result_dfs = []

        for stock in securities:
            for year in range(int(start_year), int(end_year) + 1):
                for quarter in [1, 2, 3, 4]:
                    try:
                        df = fetcher.fetch_balance_data(stock, year, quarter)

                        if df is not None and not df.empty:
                            result_dfs.append(df)

                    except Exception as e:
                        logger.debug(
                            f"fetch_balance_data failed for {stock} {year}Q{quarter}: {e}"
                        )
                        continue

        if result_dfs:
            return pd.concat(result_dfs)
        else:
            return None

    else:
        logger.warning(f"Table '{table}' not supported")
        return None


def get_index_stocks(
    index_code: str, date: Optional[str] = None
) -> Optional[List[str]]:
    """
    Get index constituent stocks (PTrade compatible)

    Args:
        index_code: Index code (e.g., '000016.SS', '000300.SS', '000905.SS')
        date: Date string 'YYYYMMDD' or 'YYYY-MM-DD'

    Returns:
        List of stock codes in PTrade format

    Note:
        BaoStock only supports:
        - 000016.SS (上证50)
        - 000300.SS (沪深300)
        - 000905.SS (中证500)
    """
    try:
        # Normalize date format
        if date is None:
            date_formatted = datetime.now().strftime("%Y%m%d")
        else:
            if len(date) == 8:
                date_formatted = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")

        fetcher = _get_fetcher()
        df = fetcher.fetch_index_stocks(index_code, date_formatted)

        if df is None or df.empty:
            return []

        # Convert to PTrade format
        if "code" in df.columns:
            stocks = [
                convert_to_ptrade_code(code, "baostock") for code in df["code"].tolist()
            ]
            return stocks
        else:
            return []

    except Exception as e:
        logger.error(f"get_index_stocks failed for {index_code}: {e}")
        return []


def get_industry_stocks(
    industry: str, date: Optional[str] = None
) -> Optional[List[str]]:
    """
    Get stocks in an industry (PTrade compatible)

    Note: Not implemented yet
    """
    logger.warning("get_industry_stocks not yet implemented")
    return []
