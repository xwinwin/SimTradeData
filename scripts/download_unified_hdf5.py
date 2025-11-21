# -*- coding: utf-8 -*-
"""
Download all data types to unified HDF5 file using BaoStock backend
Compatible with PTrade data format
"""

import json
import logging
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from tables import NaturalNameWarning
from tqdm import tqdm

# Import PTrade-compatible API
from simtradedata.fetchers.baostock_fetcher import BaoStockFetcher
from simtradedata.interfaces.ptrade_data_api import (
    get_Ashares,
    get_index_stocks,
    get_price,
    get_stock_blocks,
    get_stock_exrights,
    get_stock_info,
    get_stock_status,
    get_trade_days,
)

warnings.filterwarnings("ignore", category=NaturalNameWarning)

# Configuration
OUTPUT_FILE = "ptrade_data.h5"
LOG_FILE = "download_unified.log"
CHECKPOINT_INTERVAL = 100

HDF5_COMPLEVEL = 9
HDF5_COMPLIB = "blosc"

# Data fields
REQUIRED_PRICE_FIELDS = ["close", "open", "high", "low", "volume", "money"]

# Date range configuration
START_DATE = "2017-01-01"
END_DATE = None  # None means use current date
INCREMENTAL_DAYS = None  # Set to N to only update last N days (for incremental updates)

# Logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w",
)
logger = logging.getLogger(__name__)


# Download functions
def download_stock_price(stock, start_date, end_date):
    """Download price data"""
    try:
        price_data = get_price(
            stock,
            start_date=start_date,
            end_date=end_date,
            frequency="1d",
            fields=REQUIRED_PRICE_FIELDS,
            fq="none",
        )
        if price_data is None or len(price_data) < 60:
            return None

        if isinstance(price_data, dict):
            df = pd.DataFrame(price_data)
        else:
            df = price_data

        # Ensure numeric column types for PyTables
        for col in REQUIRED_PRICE_FIELDS:
            if col in df.columns:
                df[col] = df[col].astype("float64")

        return df
    except Exception as e:
        logger.error("Price download failed: {} - {}".format(stock, e))
        return None


def download_stock_metadata(stock):
    """Download metadata"""
    metadata = {}
    info_success = False
    try:
        info = get_stock_info(
            stock, field=["stock_name", "listed_date", "de_listed_date"]
        )
        if info and stock in info:
            stock_info = info[stock]
            metadata["stock_name"] = stock_info.get("stock_name")
            metadata["listed_date"] = stock_info.get("listed_date")
            metadata["de_listed_date"] = stock_info.get("de_listed_date")
            info_success = True
        else:
            logger.warning("Stock info not found: {}".format(stock))
    except Exception as e:
        logger.warning("Failed to get stock info: {} - {}".format(stock, e))

    try:
        metadata["blocks"] = get_stock_blocks(stock)
    except Exception as e:
        logger.warning("Failed to get blocks: {} - {}".format(stock, e))
        metadata["blocks"] = []

    metadata["has_info"] = info_success
    return metadata


def download_stock_exrights(stock):
    """Download ex-rights data"""
    try:
        data = get_stock_exrights(stock)
        if data is not None and len(data) > 0:
            return data
    except Exception as e:
        logger.error("Ex-rights download failed: {} - {}".format(stock, e))
    return None


def download_index_constituents(sample_dates):
    """Download index constituents (quarterly sampling)"""
    indices = {
        "000016.SS": "SSE 50",
        "000300.SS": "CSI 300",
        "000905.SS": "CSI 500",
    }

    constituents = {}
    print("\nDownloading index constituents...")

    for date_obj in tqdm(sample_dates, desc="Downloading index constituents"):
        date_str = date_obj.strftime("%Y%m%d")
        constituents[date_str] = {}

        for index_code, index_name in indices.items():
            try:
                stocks = get_index_stocks(index_code, date=date_str)
                if stocks:
                    constituents[date_str][index_code] = stocks
                    logger.info(
                        "Index constituents: {} {} - {} stocks".format(
                            date_str, index_name, len(stocks)
                        )
                    )
                else:
                    constituents[date_str][index_code] = []
            except Exception as e:
                logger.error(
                    "Index constituents download failed: {} {} - {}".format(
                        date_str, index_name, e
                    )
                )
                constituents[date_str][index_code] = []

    return constituents


def download_stock_status_history(
    stocks, sample_dates, start_date, end_date, fetcher
):
    """Download stock status history (ST/HALT/DELISTING)"""
    status_types = ["ST", "HALT", "DELISTING"]
    status_history = {}

    print("\nDownloading stock status history...")

    # Optimized: Fetch all delisting info at once
    all_stock_basics = _get_all_stock_basics(stocks, fetcher)

    # Optimized: Fetch all ST history at once
    st_history = _get_st_stock_history(stocks, start_date, end_date, fetcher)

    for date_obj in tqdm(sample_dates, desc="Processing stock status history"):
        date_str = date_obj.strftime("%Y%m%d")
        date_iso = date_obj.strftime("%Y-%m-%d")
        status_history[date_str] = {}

        for status_type in status_types:
            try:
                result = {}
                if status_type == "DELISTING":
                    # Use pre-fetched data
                    date_int = int(date_iso.replace("-", ""))
                    for stock, basic_info in all_stock_basics.items():
                        is_delisted = basic_info["status"] == "0"
                        out_date = basic_info["outDate"]
                        if not is_delisted and out_date and out_date.strip():
                            out_date_int = int(out_date.replace("-", ""))
                            if date_int >= out_date_int:
                                is_delisted = True
                        if is_delisted:
                            result[stock] = True

                elif status_type == "ST":
                    # Use pre-fetched history
                    for stock, st_df in st_history.items():
                        if not st_df.empty:
                            # Check if the date is in the history
                            if pd.Timestamp(date_iso) in st_df.index:
                                if st_df.loc[pd.Timestamp(date_iso)]["isST"] == "1":
                                    result[stock] = True

                elif status_type == "HALT":
                    # Use the optimized API call
                    result = get_stock_status(
                        stocks, query_type="HALT", query_date=date_str
                    )

                if result:
                    # Only save True values to save space
                    status_history[date_str][status_type] = {
                        k: v for k, v in result.items() if v
                    }
                    count = len(status_history[date_str][status_type])
                    if count > 0:
                        logger.info(
                            "Stock status: {} {} - {} stocks".format(
                                date_str, status_type, count
                            )
                        )
                else:
                    status_history[date_str][status_type] = {}

            except Exception as e:
                logger.error(
                    "Stock status processing failed: {} {} - {}".format(
                        date_str, status_type, e
                    )
                )
                status_history[date_str][status_type] = {}

    return status_history


def _get_all_stock_basics(stocks, fetcher):
    """Helper to fetch basic info for all stocks at once."""
    print("Fetching all stock basic info for delisting checks...")
    all_stock_basics = {}
    for stock in tqdm(stocks, desc="Fetching stock basics"):
        try:
            basic_df = fetcher.fetch_stock_basic(stock)
            if basic_df is not None and not basic_df.empty:
                all_stock_basics[stock] = {
                    "status": str(basic_df["status"].values[0]),
                    "outDate": basic_df["outDate"].values[0],
                }
        except Exception as e:
            logger.error(f"Failed to get basic info for {stock}: {e}")
    return all_stock_basics


def _get_st_stock_history(stocks, start_date, end_date, fetcher):
    """Helper to fetch ST history for all stocks in a date range."""
    print("Fetching ST history for all stocks...")
    st_history = {}
    for stock in tqdm(stocks, desc="Fetching ST history"):
        try:
            df = fetcher.fetch_market_data(
                symbol=stock,
                start_date=start_date,
                end_date=end_date,
                extra_fields=["isST"],
            )
            if df is not None and not df.empty:
                df = df.set_index("date")
                # Filter for only ST days to save memory
                st_days = df[df["isST"] == "1"]
                if not st_days.empty:
                    st_history[stock] = st_days

        except Exception as e:
            logger.error(f"Failed to get ST history for {stock}: {e}")
            st_history[stock] = pd.DataFrame()

    return st_history


def download_trade_days(start_date, end_date):
    """Download trading calendar"""
    try:
        trade_days = get_trade_days(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
        )
        if trade_days:
            df = pd.DataFrame({"trade_date": pd.to_datetime(trade_days)})
            df.set_index("trade_date", inplace=True)
            logger.info(
                "Trading calendar downloaded: {} trading days".format(len(trade_days))
            )
            return df
        return None
    except Exception as e:
        logger.error("Trading calendar download failed: {}".format(e))
        return None


# Main download process
def download_all_data(incremental_days=None):
    """
    Download all data to single HDF5 file

    Args:
        incremental_days: If set, only update last N days for existing stocks
    """
    print("=" * 70)
    if incremental_days:
        print("Incremental update: last {} days".format(incremental_days))
    else:
        print("Download all data to unified HDF5 file")
    print("=" * 70)

    # Date range
    end_date = (
        datetime.now().date()
        if END_DATE is None
        else datetime.strptime(END_DATE, "%Y-%m-%d").date()
    )

    # For incremental mode, adjust start_date
    if incremental_days:
        start_date = end_date - timedelta(days=incremental_days)
        print("\nIncremental mode: updating last {} days".format(incremental_days))
    else:
        start_date = datetime.strptime(START_DATE, "%Y-%m-%d").date()

    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    print("\nDate range: {} ~ {}".format(start_date_str, end_date_str))

    # Get stock pool
    print("\nGetting stock pool...")
    stock_pool = []

    # For incremental mode, load existing stocks from HDF5
    if incremental_days and Path(OUTPUT_FILE).exists():
        print("Loading existing stocks from {}...".format(OUTPUT_FILE))
        try:
            with pd.HDFStore(OUTPUT_FILE, mode="r") as store:
                existing_keys = [
                    k for k in store.keys() if k.startswith("/stock_data/")
                ]
                stock_pool = sorted([k.split("/")[-1] for k in existing_keys])
                print("  Found {} existing stocks".format(len(stock_pool)))
        except Exception as e:
            logger.error("Failed to load existing stocks: {}".format(e))
            print("  Failed to load existing stocks, will fetch from API")
            stock_pool = []

    # If not incremental or failed to load, fetch from API
    if not stock_pool:
        # Calculate sample dates (quarterly sampling)
        sample_dates = (
            pd.date_range(start=start_date, end=end_date, freq="QS")
            .to_pydatetime()
            .tolist()
        )
        # Ensure start and end dates are included
        sample_dates_set = set(d.date() for d in sample_dates)
        if start_date not in sample_dates_set:
            sample_dates.insert(0, datetime.combine(start_date, datetime.min.time()))
        if end_date not in sample_dates_set:
            sample_dates.append(datetime.combine(end_date, datetime.min.time()))

        # Convert to date objects
        sample_dates.sort()

        print(
            "\nSampling strategy: Quarterly sampling, {} sampling points".format(
                len(sample_dates)
            )
        )
        print("  First sample: {}".format(sample_dates[0]))
        print("  Last sample: {}".format(sample_dates[-1]))

        all_stocks = set()
        for date_obj in tqdm(sample_dates, desc="Getting stock pool"):
            date_str = date_obj.strftime("%Y-%m-%d")
            try:
                stocks = get_Ashares(date_str)
                if stocks:
                    all_stocks.update(stocks)
            except Exception as e:
                logger.error("Failed to get stock pool: {} - {}".format(date_str, e))

        stock_pool = sorted(list(all_stocks))
        print("  Stock pool: {} stocks".format(len(stock_pool)))
    else:
        # For incremental mode, still need sample_dates for index constituents
        sample_dates = [end_date]

    logger.info(f"Stock pool size: {len(stock_pool)}")

    # Initialize BaoStock Fetcher
    with BaoStockFetcher() as fetcher:
        # Download index constituents
        index_constituents = download_index_constituents(sample_dates)

        # Download stock status history (batch processing to avoid timeout)
        stock_status_history = download_stock_status_history(
            stock_pool, sample_dates, start_date_str, end_date_str, fetcher
        )

        # Download trading calendar
        print("\nDownloading trading calendar...")
        trade_days_df = download_trade_days(start_date, end_date)

        # Download data (temporary storage)
        price_data = {}
        metadata_list = []
        exrights_data = {}

        success = 0
        fail = 0

        for stock in tqdm(stock_pool, desc="Downloading stock data"):
            logger.info(f"Processing stock: {stock}")
            try:
                # Download price
                price_df = download_stock_price(stock, start_date_str, end_date_str)
                if price_df is not None:
                    price_data[stock] = price_df
                    logger.info(f"Downloaded price for {stock}: {len(price_df)} rows")
                else:
                    logger.warning(f"No price data for {stock}")

                # Download metadata
                meta = download_stock_metadata(stock)
                metadata_list.append(
                    {
                        "stock_code": stock,
                        "stock_name": meta.get("stock_name"),
                        "listed_date": meta.get("listed_date"),
                        "de_listed_date": meta.get("de_listed_date"),
                        "blocks": (
                            json.dumps(meta.get("blocks", {}), ensure_ascii=False)
                            if meta.get("blocks")
                            else None
                        ),
                        "has_info": meta.get("has_info", False),
                    }
                )

                # Download exrights
                ex_df = download_stock_exrights(stock)
                if ex_df is not None:
                    exrights_data[stock] = ex_df
                    logger.info(
                        f"Downloaded exrights for {stock}: {len(ex_df)} rows"
                    )
                else:
                    logger.warning(f"No exrights data for {stock}")

                success += 1

            except Exception as e:
                logger.error("Download failed: {} - {}".format(stock, e))
                fail += 1

    print("\n\nDownload complete, starting save and optimization...")

    # Save to HDF5 (sorted)
    with pd.HDFStore(
        OUTPUT_FILE, mode="w", complevel=HDF5_COMPLEVEL, complib=HDF5_COMPLIB
    ) as store:

        # 1. Save price (sorted by stock code)
        print("\nSaving price data...")
        for stock in tqdm(sorted(price_data.keys()), desc="Saving price data"):
            key = "/stock_data/{}".format(stock)
            store.put(key, price_data[stock], format="fixed")

        # 2. Save exrights (sorted by stock code)
        print("Saving exrights data...")
        for stock in tqdm(sorted(exrights_data.keys()), desc="Saving exrights data"):
            key = "/exrights/{}".format(stock)
            store.put(key, exrights_data[stock], format="fixed")

        # 3. Save metadata (single DataFrame, sorted)
        print("Saving metadata...")
        if metadata_list:
            meta_df = pd.DataFrame(metadata_list)
            meta_df.set_index("stock_code", inplace=True)
            meta_df = meta_df.sort_index()
            store.put("stock_metadata", meta_df, format="table")

        # 4. Save benchmark
        print("Saving benchmark...")
        try:
            benchmark = get_price(
                "000300.SS",
                start_date=start_date_str,
                end_date=end_date_str,
                frequency="1d",
                fields=REQUIRED_PRICE_FIELDS,
                fq="none",
            )
            if benchmark is not None:
                if isinstance(benchmark, dict):
                    benchmark = pd.DataFrame(benchmark)
                store.put("benchmark", benchmark, format="fixed")
        except:
            logger.warning("Benchmark download failed")

        # 5. Save trading calendar
        print("Saving trading calendar...")
        if trade_days_df is not None:
            store.put("trade_days", trade_days_df, format="fixed")

        # 6. Save global metadata
        print("Saving global metadata...")

        # Serialize large dicts to JSON
        global_meta = {
            "download_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "start_date": start_date_str,
            "end_date": end_date_str,
            "stock_count": len(stock_pool),
            "sample_count": len(sample_dates),
            "format_version": 3,  # Version 3: quarterly sampling + trading calendar
        }

        # Add index_constituents and stock_status_history (serialized to JSON)
        if index_constituents:
            global_meta["index_constituents"] = json.dumps(
                index_constituents, ensure_ascii=False
            )
        if stock_status_history:
            global_meta["stock_status_history"] = json.dumps(
                stock_status_history, ensure_ascii=False
            )

        # Save as Series
        meta_series = pd.Series(global_meta)
        store.put("metadata", meta_series, format="fixed")

    # Statistics
    file_size = Path(OUTPUT_FILE).stat().st_size / (1024 * 1024)

    print("\n" + "=" * 70)
    print("Complete")
    print("=" * 70)
    print("Success: {} Failed: {}".format(success, fail))
    print("Output file: {}".format(OUTPUT_FILE))
    print("File size: {:.1f} MB".format(file_size))
    print("\nData structure:")
    print("  /stock_data/{{symbol}} - {} stocks price".format(len(price_data)))
    print("  /exrights/{{symbol}} - {} stocks exrights".format(len(exrights_data)))
    print("  /stock_metadata - metadata DataFrame")
    print("  /benchmark - benchmark data")
    print("  /metadata - global metadata")

    # Count stocks missing info
    if metadata_list:
        missing_info = [m for m in metadata_list if not m.get("has_info", False)]
        if missing_info:
            print(
                "\nWarning: {} stocks missing basic info (may be delisted)".format(
                    len(missing_info)
                )
            )
            print(
                "Recommend checking price data completeness for these stocks in backtesting"
            )

    print("\nNote: valuation data needs to be downloaded separately")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Download PTrade-compatible HDF5 data")
    parser.add_argument(
        "--incremental",
        type=int,
        metavar="DAYS",
        help="Incremental update: only update last N days for existing stocks",
    )

    args = parser.parse_args()

    # Use command line arg or config
    incremental = args.incremental or INCREMENTAL_DAYS
    download_all_data(incremental_days=incremental)
