"""
BaoStock data fetcher implementation
"""

import logging
from datetime import datetime

import baostock as bs
import pandas as pd

from simtradedata.utils.code_utils import convert_from_ptrade_code, retry_on_failure

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
    def fetch_market_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        frequency: str = "d",
        adjustflag: str = "3",
        extra_fields: list = None,
    ) -> pd.DataFrame:
        """
        Fetch market K-line data

        Args:
            symbol: Stock code in PTrade format (e.g., '600000.SH')
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            frequency: d=daily, w=weekly, m=monthly
            adjustflag: "1"=forward, "2"=backward, "3"=none
            extra_fields: Additional fields to fetch (e.g., ['isST', 'tradestatus'])

        Returns:
            DataFrame with columns: date, open, high, low, close, volume, amount, and extra fields
        """

        # Convert to BaoStock format
        bs_code = convert_from_ptrade_code(symbol, "baostock")

        # Build fields string
        base_fields = "date,open,high,low,close,volume,amount"
        if extra_fields:
            fields_str = base_fields + "," + ",".join(extra_fields)
        else:
            fields_str = base_fields

        rs = bs.query_history_k_data_plus(
            bs_code,
            fields_str,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            adjustflag=adjustflag,
        )

        if rs.error_code != "0":
            raise RuntimeError(
                f"Failed to query market data for {symbol}: {rs.error_msg}"
            )

        df = rs.get_data()

        if df.empty:
            logger.warning(f"No market data for {symbol}")
            return pd.DataFrame()

        # Convert data types
        df["date"] = pd.to_datetime(df["date"])
        numeric_cols = ["open", "high", "low", "close", "volume", "amount"]
        for col in numeric_cols:
            if col in df.columns:
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

        bs_code = convert_from_ptrade_code(symbol, "baostock")

        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,peTTM,pbMRQ,psTTM,pcfNcfTTM,turn",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3",
        )

        if rs.error_code != "0":
            raise RuntimeError(
                f"Failed to query valuation data for {symbol}: {rs.error_msg}"
            )

        df = rs.get_data()

        if df.empty:
            logger.warning(f"No valuation data for {symbol}")
            return pd.DataFrame()

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

        bs_code = convert_from_ptrade_code(symbol, "baostock")

        rs = bs.query_adjust_factor(
            code=bs_code, start_date=start_date, end_date=end_date
        )

        if rs.error_code != "0":
            raise RuntimeError(
                f"Failed to query adjust factor for {symbol}: {rs.error_msg}"
            )

        df = rs.get_data()

        if df.empty:
            # Check if it's an index (indices don't have adjust factors)
            if bs_code.startswith("sh.") and bs_code[3:].startswith("00"):
                logger.debug(f"No adjust factor data for index {symbol} (expected)")
            elif bs_code.startswith("sz.399"):  # Shenzhen indices
                logger.debug(f"No adjust factor data for index {symbol} (expected)")
            else:
                logger.warning(f"No adjust factor data for {symbol}")
            return pd.DataFrame()

        # Note: BaoStock returns 'dividOperateDate', not 'date'
        df = df.rename(columns={"dividOperateDate": "date"})

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

        bs_code = convert_from_ptrade_code(symbol, "baostock")

        rs = bs.query_dividend_data(code=bs_code, year=str(year), yearType="report")

        if rs.error_code != "0":
            raise RuntimeError(
                f"Failed to query dividend data for {symbol}: {rs.error_msg}"
            )

        df = rs.get_data()

        if df.empty:
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

        bs_code = convert_from_ptrade_code(symbol, "baostock")

        rs = bs.query_profit_data(code=bs_code, year=year, quarter=quarter)

        if rs.error_code != "0":
            raise RuntimeError(
                f"Failed to query profit data for {symbol}: {rs.error_msg}"
            )

        df = rs.get_data()

        if df.empty:
            return pd.DataFrame()

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

        bs_code = convert_from_ptrade_code(symbol, "baostock")
        rs = bs.query_operation_data(code=bs_code, year=year, quarter=quarter)

        if rs.error_code != "0":
            raise RuntimeError(
                f"Failed to query operation data for {symbol}: {rs.error_msg}"
            )

        df = rs.get_data()

        if df.empty:
            return pd.DataFrame()

        for col in df.columns:
            if col not in ["code", "pubDate", "statDate"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    @retry_on_failure
    def fetch_growth_data(self, symbol: str, year: int, quarter: int) -> pd.DataFrame:
        """
        Fetch quarterly growth data (成长能力)
        """

        bs_code = convert_from_ptrade_code(symbol, "baostock")
        rs = bs.query_growth_data(code=bs_code, year=year, quarter=quarter)

        if rs.error_code != "0":
            raise RuntimeError(
                f"Failed to query growth data for {symbol}: {rs.error_msg}"
            )

        df = rs.get_data()

        if df.empty:
            return pd.DataFrame()

        for col in df.columns:
            if col not in ["code", "pubDate", "statDate"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    @retry_on_failure
    def fetch_balance_data(self, symbol: str, year: int, quarter: int) -> pd.DataFrame:
        """
        Fetch quarterly balance data (偿债能力)
        """

        bs_code = convert_from_ptrade_code(symbol, "baostock")
        rs = bs.query_balance_data(code=bs_code, year=year, quarter=quarter)

        if rs.error_code != "0":
            raise RuntimeError(
                f"Failed to query balance data for {symbol}: {rs.error_msg}"
            )

        df = rs.get_data()

        if df.empty:
            return pd.DataFrame()

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

        bs_code = convert_from_ptrade_code(symbol, "baostock")
        rs = bs.query_stock_basic(code=bs_code)

        if rs.error_code != "0":
            raise RuntimeError(
                f"Failed to query stock basic info for {symbol}: {rs.error_msg}"
            )

        df = rs.get_data()

        if df.empty:
            return pd.DataFrame()

        return df

    @retry_on_failure
    def fetch_stock_list_by_date(self, date: str = "") -> pd.DataFrame:
        """
        Fetch stock list on a specific date

        Args:
            date: Date string in format 'YYYY-MM-DD' or 'YYYYMMDD'

        Returns:
            DataFrame with columns: code, code_name, type, status
        """

        rs = bs.query_all_stock(day=date)

        if rs.error_code != "0":
            raise RuntimeError(f"Failed to query stock list for {date}: {rs.error_msg}")

        df = rs.get_data()

        if df.empty:
            logger.warning(f"No stocks found for {date}")
            return pd.DataFrame()

        return df

    @retry_on_failure
    def fetch_stock_industry(self, symbol: str, date: str = None) -> pd.DataFrame:
        """
        Fetch stock industry classification

        Args:
            symbol: Stock code in PTrade format
            date: Date string (YYYY-MM-DD), if None use today

        Returns:
            DataFrame with industry classification
        """

        bs_code = convert_from_ptrade_code(symbol, "baostock")

        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        # Normalize date format
        date_str = date.replace("-", "")

        rs = bs.query_stock_industry(code=bs_code, date=date_str)

        if rs.error_code != "0":
            raise RuntimeError(f"Failed to query industry for {symbol}: {rs.error_msg}")

        df = rs.get_data()

        if df.empty:
            logger.warning(f"No industry data for {symbol}")
            return pd.DataFrame()

        return df

    @retry_on_failure
    def fetch_trade_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch trading calendar

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            DataFrame with trading days
        """

        rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)

        if rs.error_code != "0":
            raise RuntimeError(f"Failed to query trade calendar: {rs.error_msg}")

        df = rs.get_data()

        if df.empty:
            return pd.DataFrame()

        return df

    @retry_on_failure
    def fetch_index_stocks(self, index_code: str, date: str = None) -> pd.DataFrame:
        """
        Fetch index constituent stocks

        Args:
            index_code: Index code in PTrade format (e.g., '000016.SS', '000300.SS', '000905.SS')
            date: Date string (YYYY-MM-DD), if None use latest

        Returns:
            DataFrame with stock codes

        Note:
            BaoStock only supports specific indices:
            - 000016.SS (上证50): query_sz50_stocks
            - 000300.SS (沪深300): query_hs300_stocks
            - 000905.SS (中证500): query_zz500_stocks
        """

        query_date = date
        if query_date is None:
            query_date = datetime.now().strftime("%Y-%m-%d")

        # Map PTrade index codes to BaoStock query methods
        index_map = {
            "000016.SS": "sz50",  # 上证50
            "000300.SS": "hs300",  # 沪深300
            "000905.SS": "zz500",  # 中证500
        }

        if index_code not in index_map:
            logger.warning(f"Index {index_code} not supported by BaoStock")
            return pd.DataFrame()

        index_type = index_map[index_code]

        # Call corresponding BaoStock API
        if index_type == "sz50":
            rs = bs.query_sz50_stocks(date=query_date)
        elif index_type == "hs300":
            rs = bs.query_hs300_stocks(date=query_date)
        elif index_type == "zz500":
            rs = bs.query_zz500_stocks(date=query_date)
        else:
            logger.warning(f"Unknown index type: {index_type}")
            return pd.DataFrame()

        if rs.error_code != "0":
            raise RuntimeError(
                f"Failed to query index stocks for {index_code}: {rs.error_msg}"
            )

        df = rs.get_data()

        if df.empty:
            logger.warning(f"No constituent stocks found for {index_code}")
            return pd.DataFrame()

        return df
