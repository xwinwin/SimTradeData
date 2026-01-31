# -*- coding: utf-8 -*-
"""
Import TDX (通达信) financial data from mootdx Affair API.

Data source: TDX gpcw (股票财务) ZIP files
Each ZIP contains all stocks' financial data for one quarter.

Features:
1. Batch download by quarter (one ZIP = all stocks for that quarter)
2. Incremental: skips already completed quarters
3. Maps FINVALUE indices to PTrade field names

Usage:
    # Import all available quarters
    poetry run python scripts/import_tdx_finance.py

    # Import specific year range
    poetry run python scripts/import_tdx_finance.py --start-year 2020 --end-year 2025

    # List available data without importing
    poetry run python scripts/import_tdx_finance.py --list-only

    # Force reimport (ignore completed quarters)
    poetry run python scripts/import_tdx_finance.py --full
"""

import argparse
import logging
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from simtradedata.config.mootdx_finvalue_map import (
    FINVALUE_TO_PTRADE,
    parse_finvalue_date,
)
from simtradedata.utils.code_utils import convert_to_ptrade_code
from simtradedata.writers.duckdb_writer import DEFAULT_DB_PATH, DuckDBWriter

# Configuration
LOG_FILE = "data/import_tdx_finance.log"
BATCH_SIZE = 100  # Stocks per transaction

Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w",
)
logger = logging.getLogger(__name__)


def parse_quarter_from_filename(filename: str) -> tuple:
    """
    Extract year and quarter from gpcw filename.

    Args:
        filename: e.g., 'gpcw20231231.zip'

    Returns:
        (year, quarter) tuple, e.g., (2023, 4)
    """
    # Extract date part: gpcw20231231.zip -> 20231231
    date_str = filename.replace("gpcw", "").replace(".zip", "")
    if len(date_str) != 8:
        return None, None

    year = int(date_str[:4])
    month = int(date_str[4:6])

    quarter_map = {3: 1, 6: 2, 9: 3, 12: 4}
    quarter = quarter_map.get(month)

    return year, quarter


def is_a_share_stock(code: str) -> bool:
    """Filter to only A-share stocks."""
    if len(code) != 6:
        return False
    prefix = code[:3]
    valid_prefixes = {
        '600', '601', '603', '605', '688', '689',  # SH
        '000', '001', '002', '003', '300', '301',  # SZ
    }
    return prefix in valid_prefixes


class TdxFinanceImporter:
    """Import TDX financial data into DuckDB."""

    def __init__(
        self,
        db_path: str = DEFAULT_DB_PATH,
        download_dir: str = None,
        full_import: bool = False,
    ):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.writer = DuckDBWriter(db_path=str(self.db_path))
        self.full_import = full_import

        if download_dir:
            self.download_dir = Path(download_dir)
        else:
            self.download_dir = Path(tempfile.gettempdir()) / "mootdx_finance"
        self.download_dir.mkdir(parents=True, exist_ok=True)

        self.stats = {
            "quarters_processed": 0,
            "quarters_skipped": 0,
            "stocks_imported": 0,
            "records_imported": 0,
        }

    def list_available_quarters(self) -> list:
        """List all available quarter files from TDX server."""
        from mootdx.affair import Affair

        files = Affair.files()
        quarters = []

        for f in files:
            filename = f["filename"]
            filesize = f["filesize"]

            # Skip empty files
            if filesize < 1000:
                continue

            year, quarter = parse_quarter_from_filename(filename)
            if year and quarter:
                quarters.append({
                    "filename": filename,
                    "year": year,
                    "quarter": quarter,
                    "filesize": filesize,
                })

        # Sort by year, quarter
        quarters.sort(key=lambda x: (x["year"], x["quarter"]))
        return quarters

    def get_completed_quarters(self) -> set:
        """Get set of (year, quarter) already imported."""
        if self.full_import:
            return set()
        return self.writer.get_completed_fundamental_quarters()

    def fetch_and_parse_quarter(self, filename: str) -> pd.DataFrame:
        """
        Download and parse a quarter's financial data.

        Affair.fetch() downloads the file, Affair.parse() reads it.
        The returned DataFrame has:
        - index: stock code (e.g., '000001')
        - columns: Chinese field names, positional index = FINVALUE ID
        - column[0] = report_date (YYYYMMDD as float)

        Returns DataFrame with columns mapped to PTrade format.
        """
        from mootdx.affair import Affair

        try:
            # Download the file
            Affair.fetch(
                downdir=str(self.download_dir),
                filename=filename,
            )

            # Parse the downloaded file
            raw_df = Affair.parse(
                downdir=str(self.download_dir),
                filename=filename,
            )

            if raw_df is None or raw_df.empty:
                return pd.DataFrame()

            return self._convert_to_ptrade_format(raw_df)

        except Exception as e:
            logger.error(f"Failed to fetch {filename}: {e}")
            raise

    def _convert_to_ptrade_format(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert Affair DataFrame to PTrade format.

        raw_df structure:
        - index (name='code'): stock codes like '000001'
        - columns: positional index matches FINVALUE ID
        - column 0 = report_date (YYYYMMDD format), 314 = 财报公告日期 (YYMMDD format)
        """
        records = []
        num_cols = len(raw_df.columns)

        for code, row in raw_df.iterrows():
            code = str(code)
            if not is_a_share_stock(code):
                continue

            ptrade_code = convert_to_ptrade_code(code, "qstock")
            record = {"symbol": ptrade_code}

            for idx, (field_name, desc, unit) in FINVALUE_TO_PTRADE.items():
                if idx >= num_cols:
                    continue

                value = row.iloc[idx]

                # Parse date fields
                if field_name == "_report_date_raw":
                    # YYYYMMDD format (8 digits)
                    try:
                        date_int = int(value)
                        year = date_int // 10000
                        month = (date_int % 10000) // 100
                        day = date_int % 100
                        if 1990 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                            record["end_date"] = f"{year:04d}-{month:02d}-{day:02d}"
                    except (ValueError, TypeError):
                        pass
                elif field_name == "_publ_date_raw":
                    # YYMMDD format (6 digits)
                    try:
                        date_str = parse_finvalue_date(int(value))
                        if date_str:
                            record["publ_date"] = date_str
                    except (ValueError, TypeError):
                        pass
                elif not field_name.startswith("_"):
                    try:
                        v = float(value)
                        # FINVALUE uses 0 for null
                        record[field_name] = v if v != 0.0 else None
                    except (ValueError, TypeError):
                        record[field_name] = None

            if "end_date" in record:
                records.append(record)

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records)
        df["end_date"] = pd.to_datetime(df["end_date"])
        if "publ_date" in df.columns:
            df["publ_date"] = pd.to_datetime(df["publ_date"])

        logger.info(f"Converted {len(df)} rows, {len(df.columns)} columns")
        return df

    def import_quarter(self, filename: str, year: int, quarter: int) -> int:
        """Import a single quarter's data."""
        df = self.fetch_and_parse_quarter(filename)

        if df.empty:
            logger.warning(f"No data for {year}Q{quarter}")
            return 0

        # Group by symbol and write
        success_count = 0
        symbols = df["symbol"].unique()

        # Process in batches
        for i in range(0, len(symbols), BATCH_SIZE):
            batch_symbols = symbols[i:i + BATCH_SIZE]

            self.writer.begin()
            try:
                for symbol in batch_symbols:
                    symbol_df = df[df["symbol"] == symbol].copy()
                    symbol_df = symbol_df.drop(columns=["symbol"])

                    if "end_date" in symbol_df.columns:
                        symbol_df = symbol_df.set_index("end_date")

                    self.writer.write_fundamentals(symbol, symbol_df)
                    success_count += 1
                    self.stats["records_imported"] += len(symbol_df)

                self.writer.commit()
            except Exception as e:
                logger.error(f"Batch failed for {year}Q{quarter}: {e}")
                self.writer.rollback()
                raise

        self.stats["stocks_imported"] += success_count
        return success_count

    def import_all(
        self,
        start_year: int = None,
        end_year: int = None,
    ) -> dict:
        """Import all available quarters."""
        quarters = self.list_available_quarters()

        # Filter by year range
        if start_year:
            quarters = [q for q in quarters if q["year"] >= start_year]
        if end_year:
            quarters = [q for q in quarters if q["year"] <= end_year]

        if not quarters:
            print("No quarters to import")
            return self.stats

        completed = self.get_completed_quarters()
        pending = [
            q for q in quarters
            if (q["year"], q["quarter"]) not in completed
        ]

        print(f"Total quarters: {len(quarters)}")
        print(f"Already completed: {len(quarters) - len(pending)}")
        print(f"Pending: {len(pending)}")

        if not pending:
            print("\nAll quarters already imported!")
            return self.stats

        print()

        for q in tqdm(pending, desc="Importing quarters", unit="quarter"):
            year, quarter = q["year"], q["quarter"]
            filename = q["filename"]

            try:
                count = self.import_quarter(filename, year, quarter)

                if count > 0:
                    self.writer.mark_fundamental_quarter_completed(
                        year, quarter, count
                    )
                    self.stats["quarters_processed"] += 1
                else:
                    self.stats["quarters_skipped"] += 1

            except Exception as e:
                logger.error(f"Failed {year}Q{quarter}: {e}")
                self.stats["quarters_skipped"] += 1

        return self.stats

    def close(self):
        """Close database connection."""
        self.writer.close()


def main():
    parser = argparse.ArgumentParser(
        description="Import TDX financial data from mootdx",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--db",
        type=str,
        default=DEFAULT_DB_PATH,
        help=f"Database path (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help="Start year (default: all available)",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="End year (default: all available)",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="List available quarters without importing",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full import (ignore completed quarters)",
    )
    parser.add_argument(
        "--download-dir",
        type=str,
        default=None,
        help="Directory for downloaded ZIP files",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("TDX Financial Data Import")
    print("=" * 60)

    importer = TdxFinanceImporter(
        db_path=args.db,
        download_dir=args.download_dir,
        full_import=args.full,
    )

    try:
        if args.list_only:
            quarters = importer.list_available_quarters()
            print(f"\nAvailable quarters: {len(quarters)}")
            print()
            for q in quarters:
                size_mb = q["filesize"] / 1024 / 1024
                print(f"  {q['year']}Q{q['quarter']}: {q['filename']} ({size_mb:.1f} MB)")
            return 0

        print(f"Database: {args.db}")
        if args.start_year or args.end_year:
            print(f"Year range: {args.start_year or 'start'} - {args.end_year or 'end'}")
        print()

        stats = importer.import_all(
            start_year=args.start_year,
            end_year=args.end_year,
        )

        print()
        print("=" * 60)
        print("Import Complete")
        print("=" * 60)
        print(f"Quarters processed: {stats['quarters_processed']}")
        print(f"Quarters skipped: {stats['quarters_skipped']}")
        print(f"Stocks imported: {stats['stocks_imported']}")
        print(f"Records imported: {stats['records_imported']}")

    finally:
        importer.close()

    return 0


if __name__ == "__main__":
    exit(main())
