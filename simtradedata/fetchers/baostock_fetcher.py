"""
BaoStock data fetcher implementation
"""

import logging
from datetime import datetime

import baostock as bs
import pandas as pd

from simtradedata.utils.code_utils import (
    convert_from_ptrade_code,
    format_date,
    retry_on_failure,
)

logger = logging.getLogger(__name__)


class BaoStockFetcher:
    """
    Fetch data from BaoStock API

    BaoStock provides free A-share market data including:
    - Daily K-line data
    - Financial statements
    - Valuation indicators
    - Adjust factors
    - Dividend data
    """

    def __init__(self):
        self._logged_in = False

    def login(self):
        """Login to BaoStock"""
        if not self._logged_in:
            lg = bs.login()
            if lg.error_code != "0":
                raise ConnectionError(f"BaoStock login failed: {lg.error_msg}")
            self._logged_in = True
            logger.info("BaoStock login successful")

    def logout(self):
        """Logout from BaoStock"""
        if self._logged_in:
            try:
                bs.logout()
                self._logged_in = False
                logger.info("BaoStock logout successful")
            except Exception as e:
                logger.warning(f"BaoStock logout error (non-critical): {e}")
                self._logged_in = False

    def __enter__(self):
        self.login()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logout()
        return False  # Don't suppress exceptions

    def __del__(self):
        """Destructor to ensure logout on object deletion"""
        try:
            self.logout()
        except:
            pass  # Ignore errors in destructor

    @retry_on_failure
    def fetch_stock_list(self) -> pd.DataFrame:
        """
        Fetch list of all stocks (excluding indices)

        Returns:
            DataFrame with columns: code, tradeStatus, code_name

        Note:
            Filters to only include actual stocks by code pattern:
            - SH stocks: 600xxx, 601xxx, 603xxx, 605xxx, 688xxx (科创板)
            - SZ stocks: 000xxx (主板), 001xxx, 002xxx (中小板), 003xxx, 300xxx (创业板)
            Excludes indices like: sh.000001-000999, sz.399001-399999
        """
        self.login()

        rs = bs.query_all_stock(day=format_date(datetime.now()))
        data_list = []

        while (rs.error_code == "0") & rs.next():
            data_list.append(rs.get_row_data())

        df = pd.DataFrame(data_list, columns=rs.fields)

        # Filter to only include stocks, exclude indices
        # Extract stock number from code (e.g., 'sh.600000' -> '600000')
        df["stock_num"] = df["code"].str.split(".").str[1]

        total_count = len(df)

        # Define stock code patterns (6-digit codes starting with specific prefixes)
        # SH stocks: 600, 601, 603, 605, 688 (科创板)
        # SZ stocks: 000 (主板), 001, 002 (中小板), 003, 300 (创业板)
        stock_patterns = [
            "600",
            "601",
            "603",
            "605",
            "688",  # Shanghai
            "000",
            "001",
            "002",
            "003",
            "300",
        ]  # Shenzhen

        df = df[df["stock_num"].str[:3].isin(stock_patterns)].copy()
        df = df.drop(columns=["stock_num"])

        stocks_count = len(df)

        logger.info(
            f"Fetched {stocks_count} stocks from BaoStock "
            f"(filtered out {total_count - stocks_count} non-stock items like indices)"
        )

        return df

    @retry_on_failure
    def fetch_market_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        frequency: str = "d",
        adjustflag: str = "3",
    ) -> pd.DataFrame:
        """
        Fetch market K-line data

        Args:
            symbol: Stock code in PTrade format (e.g., '600000.SH')
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            frequency: d=daily, w=weekly, m=monthly
            adjustflag: "1"=forward, "2"=backward, "3"=none

        Returns:
            DataFrame with columns: date, open, high, low, close, volume, amount
        """
        self.login()

        # Convert to BaoStock format
        bs_code = convert_from_ptrade_code(symbol, "baostock")

        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,open,high,low,close,volume,amount",
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            adjustflag=adjustflag,
        )

        data_list = []
        while (rs.error_code == "0") & rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            logger.warning(f"No market data for {symbol}")
            return pd.DataFrame()

        df = pd.DataFrame(data_list, columns=rs.fields)

        # Convert data types
        df["date"] = pd.to_datetime(df["date"])
        numeric_cols = ["open", "high", "low", "close", "volume", "amount"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Note: Keep 'date' as column for converter to handle

        logger.info(f"Fetched {len(df)} market data rows for {symbol}")

        return df

    @retry_on_failure
    def fetch_valuation_data(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        Fetch valuation indicators (PE, PB, PS, PCF)

        Args:
            symbol: Stock code in PTrade format
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            DataFrame with valuation indicators
        """
        self.login()

        bs_code = convert_from_ptrade_code(symbol, "baostock")

        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,peTTM,pbMRQ,psTTM,pcfNcfTTM,turn",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3",
        )

        data_list = []
        while (rs.error_code == "0") & rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            logger.warning(f"No valuation data for {symbol}")
            return pd.DataFrame()

        df = pd.DataFrame(data_list, columns=rs.fields)

        df["date"] = pd.to_datetime(df["date"])
        numeric_cols = ["peTTM", "pbMRQ", "psTTM", "pcfNcfTTM", "turn"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Note: Keep 'date' as a column, not as index
        # Converter will handle index setting and column renaming

        logger.info(f"Fetched {len(df)} valuation data rows for {symbol}")

        return df

    @retry_on_failure
    def fetch_adjust_factor(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        Fetch adjust factors

        Args:
            symbol: Stock code in PTrade format
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            DataFrame with columns: date, foreAdjustFactor, backAdjustFactor
        """
        self.login()

        bs_code = convert_from_ptrade_code(symbol, "baostock")

        rs = bs.query_adjust_factor(
            code=bs_code, start_date=start_date, end_date=end_date
        )

        data_list = []
        while (rs.error_code == "0") & rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            # Check if it's an index (indices don't have adjust factors)
            if bs_code.startswith("sh.") and bs_code[3:].startswith("00"):
                logger.debug(f"No adjust factor data for index {symbol} (expected)")
            elif bs_code.startswith("sz.399"):  # Shenzhen indices
                logger.debug(f"No adjust factor data for index {symbol} (expected)")
            else:
                logger.warning(f"No adjust factor data for {symbol}")
            return pd.DataFrame()

        df = pd.DataFrame(data_list, columns=rs.fields)

        df["date"] = pd.to_datetime(df["date"])
        df["foreAdjustFactor"] = pd.to_numeric(df["foreAdjustFactor"], errors="coerce")
        df["backAdjustFactor"] = pd.to_numeric(df["backAdjustFactor"], errors="coerce")

        # Note: Keep 'date' as column for converter to handle

        logger.info(f"Fetched {len(df)} adjust factor rows for {symbol}")

        return df

    @retry_on_failure
    def fetch_dividend_data(self, symbol: str, year: int) -> pd.DataFrame:
        """
        Fetch dividend data

        Args:
            symbol: Stock code in PTrade format
            year: Year to fetch

        Returns:
            DataFrame with dividend information
        """
        self.login()

        bs_code = convert_from_ptrade_code(symbol, "baostock")

        rs = bs.query_dividend_data(code=bs_code, year=str(year), yearType="report")

        data_list = []
        while (rs.error_code == "0") & rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            # Check if it's an index (indices don't have dividends)
            if bs_code.startswith("sh.") and bs_code[3:].startswith("00"):
                logger.debug(
                    f"No dividend data for index {symbol} in {year} (expected)"
                )
            elif bs_code.startswith("sz.399"):  # Shenzhen indices
                logger.debug(
                    f"No dividend data for index {symbol} in {year} (expected)"
                )
            else:
                logger.debug(f"No dividend data for {symbol} in {year}")
            return pd.DataFrame()

        df = pd.DataFrame(data_list, columns=rs.fields)

        # Convert numeric columns
        numeric_cols = [
            "perCashDiv",
            "perShareDivRatio",
            "perShareTransRatio",
            "allotmentRatio",
            "allotmentPrice",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        logger.info(f"Fetched {len(df)} dividend records for {symbol} in {year}")

        return df

    @retry_on_failure
    def fetch_profit_data(self, symbol: str, year: int, quarter: int) -> pd.DataFrame:
        """
        Fetch quarterly profit data (盈利能力)

        Args:
            symbol: Stock code in PTrade format
            year: Year
            quarter: Quarter (1-4)

        Returns:
            DataFrame with profit indicators
        """
        self.login()

        bs_code = convert_from_ptrade_code(symbol, "baostock")

        rs = bs.query_profit_data(code=bs_code, year=year, quarter=quarter)

        data_list = []
        while (rs.error_code == "0") & rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            return pd.DataFrame()

        df = pd.DataFrame(data_list, columns=rs.fields)

        # Convert numeric columns
        for col in df.columns:
            if col not in ["code", "pubDate", "statDate"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    @retry_on_failure
    def fetch_operation_data(
        self, symbol: str, year: int, quarter: int
    ) -> pd.DataFrame:
        """
        Fetch quarterly operation data (营运能力)
        """
        self.login()

        bs_code = convert_from_ptrade_code(symbol, "baostock")
        rs = bs.query_operation_data(code=bs_code, year=year, quarter=quarter)

        data_list = []
        while (rs.error_code == "0") & rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            return pd.DataFrame()

        df = pd.DataFrame(data_list, columns=rs.fields)

        for col in df.columns:
            if col not in ["code", "pubDate", "statDate"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    @retry_on_failure
    def fetch_growth_data(self, symbol: str, year: int, quarter: int) -> pd.DataFrame:
        """
        Fetch quarterly growth data (成长能力)
        """
        self.login()

        bs_code = convert_from_ptrade_code(symbol, "baostock")
        rs = bs.query_growth_data(code=bs_code, year=year, quarter=quarter)

        data_list = []
        while (rs.error_code == "0") & rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            return pd.DataFrame()

        df = pd.DataFrame(data_list, columns=rs.fields)

        for col in df.columns:
            if col not in ["code", "pubDate", "statDate"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    @retry_on_failure
    def fetch_balance_data(self, symbol: str, year: int, quarter: int) -> pd.DataFrame:
        """
        Fetch quarterly balance data (偿债能力)
        """
        self.login()

        bs_code = convert_from_ptrade_code(symbol, "baostock")
        rs = bs.query_balance_data(code=bs_code, year=year, quarter=quarter)

        data_list = []
        while (rs.error_code == "0") & rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            return pd.DataFrame()

        df = pd.DataFrame(data_list, columns=rs.fields)

        for col in df.columns:
            if col not in ["code", "pubDate", "statDate"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    @retry_on_failure
    def fetch_stock_basic(self, symbol: str) -> pd.DataFrame:
        """
        Fetch stock basic information

        Args:
            symbol: Stock code in PTrade format

        Returns:
            DataFrame with basic stock information
        """
        self.login()

        bs_code = convert_from_ptrade_code(symbol, "baostock")
        rs = bs.query_stock_basic(code=bs_code)

        data_list = []
        while (rs.error_code == "0") & rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            return pd.DataFrame()

        df = pd.DataFrame(data_list, columns=rs.fields)

        return df
