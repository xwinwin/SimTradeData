"""
Mootdx Affair (financial data) fetcher implementation

This module handles batch financial data download using mootdx's Affair API.
Key advantage: one ZIP file contains all stocks' financial data for a quarter,
much more efficient than BaoStock's per-stock API approach.
"""

import logging
import tempfile
from pathlib import Path
from typing import List, Optional

import pandas as pd

from simtradedata.config.mootdx_finvalue_map import (
    CORE_FUNDAMENTAL_FIELDS,
    FINVALUE_TO_PTRADE,
    parse_finvalue_date,
)

logger = logging.getLogger(__name__)


class MootdxAffairFetcher:
    """
    Fetch batch financial data via mootdx Affair API.

    This fetcher is stateless - it downloads ZIP files from TDX servers
    and parses them into DataFrames. No persistent connection needed.

    Data source: TDX gpcw (股票财务) ZIP files containing FINVALUE arrays
    for all listed stocks in a given quarter.
    """

    def __init__(self, download_dir: str = None):
        """
        Initialize MootdxAffairFetcher.

        Args:
            download_dir: Directory for downloading ZIP files.
                         Defaults to system temp directory.
        """
        if download_dir:
            self._download_dir = Path(download_dir)
            self._download_dir.mkdir(parents=True, exist_ok=True)
        else:
            self._download_dir = Path(tempfile.gettempdir()) / "mootdx_affair"
            self._download_dir.mkdir(parents=True, exist_ok=True)

    def list_available_reports(self) -> List[dict]:
        """
        List available financial report files on TDX server.

        Returns:
            List of dicts with keys: filename, hash, filesize
            Example: [{'filename': 'gpcw20231231.zip', 'hash': '...', 'filesize': 12345}]
        """
        from mootdx.affair import Affair

        try:
            files = Affair.files()
            if files:
                logger.info(f"Found {len(files)} available financial reports")
            return files or []
        except Exception as e:
            logger.error(f"Failed to list available reports: {e}")
            raise

    def fetch_and_parse(self, filename: str) -> pd.DataFrame:
        """
        Download and parse a financial data ZIP file.

        Args:
            filename: ZIP filename (e.g., 'gpcw20231231.zip')

        Returns:
            Raw DataFrame with FINVALUE array columns (0-indexed)
        """
        from mootdx.affair import Affair

        try:
            df = Affair.fetch(
                downdir=str(self._download_dir),
                filename=filename,
            )

            if df is None or df.empty:
                logger.warning(f"No data in {filename}")
                return pd.DataFrame()

            logger.info(f"Parsed {filename}: {len(df)} rows")
            return df

        except Exception as e:
            logger.error(f"Failed to fetch and parse {filename}: {e}")
            raise

    def parse_local(self, filename: str) -> pd.DataFrame:
        """
        Parse a locally stored financial data file.

        Args:
            filename: ZIP or DAT filename in the download directory

        Returns:
            Raw DataFrame with FINVALUE array columns
        """
        from mootdx.affair import Affair

        try:
            df = Affair.parse(
                downdir=str(self._download_dir),
                filename=filename,
            )

            if df is None or df.empty:
                return pd.DataFrame()

            return df

        except Exception as e:
            logger.error(f"Failed to parse local file {filename}: {e}")
            raise

    def fetch_fundamentals_for_quarter(
        self,
        year: int,
        quarter: int,
        fields: List[str] = None,
    ) -> pd.DataFrame:
        """
        Fetch all stocks' financial data for a given quarter.

        This downloads the corresponding gpcw ZIP file and parses it
        into PTrade-compatible format.

        Args:
            year: Year (e.g., 2024)
            quarter: Quarter (1-4)
            fields: List of PTrade field names to include.
                   Defaults to CORE_FUNDAMENTAL_FIELDS.

        Returns:
            DataFrame indexed by stock code, with PTrade field names as columns.
            Includes 'end_date' and 'publ_date' columns.
        """
        # Determine quarter end date -> filename
        quarter_end = {1: "0331", 2: "0630", 3: "0930", 4: "1231"}
        mmdd = quarter_end.get(quarter)
        if not mmdd:
            raise ValueError(f"Invalid quarter: {quarter}")

        filename = f"gpcw{year}{mmdd}.zip"

        # Download and parse
        raw_df = self.fetch_and_parse(filename)
        if raw_df.empty:
            logger.warning(f"No data for {year}Q{quarter}")
            return pd.DataFrame()

        return self._convert_to_ptrade_format(raw_df, fields)

    def _convert_to_ptrade_format(
        self,
        raw_df: pd.DataFrame,
        fields: List[str] = None,
    ) -> pd.DataFrame:
        """
        Convert raw FINVALUE DataFrame to PTrade-compatible format.

        Args:
            raw_df: Raw DataFrame from Affair.fetch/parse
            fields: Fields to include (PTrade names). None = CORE_FUNDAMENTAL_FIELDS

        Returns:
            DataFrame with PTrade field names
        """
        target_fields = fields or CORE_FUNDAMENTAL_FIELDS

        # Build column selection: find which raw columns map to our target fields
        result_cols = {}

        for finvalue_idx, (ptrade_name, desc, unit) in FINVALUE_TO_PTRADE.items():
            if ptrade_name.startswith("_") or ptrade_name in target_fields:
                # Raw column might be named by index or string index
                for col_name in [finvalue_idx, str(finvalue_idx)]:
                    if col_name in raw_df.columns:
                        result_cols[col_name] = ptrade_name
                        break

        if not result_cols:
            logger.warning("No matching columns found in raw data")
            return pd.DataFrame()

        # Select and rename columns
        available_cols = [c for c in result_cols.keys() if c in raw_df.columns]
        result = raw_df[available_cols].copy()
        result = result.rename(columns=result_cols)

        # Parse report date (YYMMDD format)
        if "_report_date_raw" in result.columns:
            result["end_date"] = result["_report_date_raw"].apply(parse_finvalue_date)
            result["end_date"] = pd.to_datetime(result["end_date"], errors="coerce")
            result = result.drop(columns=["_report_date_raw"])

        # Parse publication date
        if "_publ_date_raw" in result.columns:
            result["publ_date"] = result["_publ_date_raw"].apply(parse_finvalue_date)
            result["publ_date"] = pd.to_datetime(result["publ_date"], errors="coerce")
            result = result.drop(columns=["_publ_date_raw"])

        # Convert numeric fields
        numeric_cols = [c for c in result.columns if c not in ("end_date", "publ_date")]
        for col in numeric_cols:
            result[col] = pd.to_numeric(result[col], errors="coerce")

        # Preserve stock code column if present
        for code_col in ["code", "symbol", "stock_code"]:
            if code_col in raw_df.columns:
                result["code"] = raw_df[code_col]
                break

        logger.info(f"Converted to PTrade format: {len(result)} rows, {len(result.columns)} columns")
        return result

    def get_quarter_filename(self, year: int, quarter: int) -> str:
        """
        Get the expected filename for a given quarter.

        Args:
            year: Year
            quarter: Quarter (1-4)

        Returns:
            Filename string (e.g., 'gpcw20231231.zip')
        """
        quarter_end = {1: "0331", 2: "0630", 3: "0930", 4: "1231"}
        return f"gpcw{year}{quarter_end[quarter]}.zip"
