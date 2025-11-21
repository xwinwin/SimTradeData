# -*- coding: utf-8 -*-
"""
Batch download valuation + fundamentals data to HDF5
- valuation: daily valuation data
- fundamentals: quarterly financial indicators
Based on efficient concurrent strategy with checkpoint support
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# Import PTrade-compatible API
from simtradedata.interfaces.ptrade_data_api import (
    get_Ashares,
    get_fundamentals,
    get_trade_days,
)

# ------------------------------ Configuration ------------------------------

HDF5_FILE = "ptrade_fundamentals.h5"
LOG_FILE = "fundamentals_download.log"

HDF5_COMPLEVEL = 9
HDF5_COMPLIB = "blosc"

# Valuation fields (daily valuation data)
VALUATION_FIELDS = [
    "pe_ttm",  # PE ratio TTM
    "ps_ttm",  # PS ratio TTM
    "total_value",  # Total market cap
    "turnover_rate",  # Turnover rate
    "total_shares",  # Total shares
    "pb",  # PB ratio
    "pcf",  # PCF ratio
    "float_value",  # Float market cap
]

# Fundamentals field mapping (quarterly financial indicators)
FUNDAMENTAL_FIELDS_MAPPING = {
    # Profitability - profit_ability table
    "roe": "profit_ability",
    "roe_ttm": "profit_ability",
    "roa": "profit_ability",
    "roa_ttm": "profit_ability",
    "roic": "profit_ability",
    "roa_ebit_ttm": "profit_ability",
    "gross_income_ratio": "profit_ability",
    "gross_income_ratio_ttm": "profit_ability",
    "net_profit_ratio": "profit_ability",
    "net_profit_ratio_ttm": "profit_ability",
    # Growth - growth_ability table
    "operating_revenue_grow_rate": "growth_ability",
    "net_profit_grow_rate": "growth_ability",
    "np_parent_company_yoy": "growth_ability",
    "basic_eps_yoy": "growth_ability",
    "total_asset_grow_rate": "growth_ability",
    # Operating - operating_ability table
    "total_asset_turnover_rate": "operating_ability",
    "current_assets_turnover_rate": "operating_ability",
    "inventory_turnover_rate": "operating_ability",
    "accounts_receivables_turnover_rate": "operating_ability",
    # Debt paying - debt_paying_ability table
    "current_ratio": "debt_paying_ability",
    "quick_ratio": "debt_paying_ability",
    "debt_equity_ratio": "debt_paying_ability",
    "interest_cover": "debt_paying_ability",
}

# Date range configuration
START_DATE = "2017-01-01"
END_DATE = None  # None means use current date
INCREMENTAL_DAYS = None  # Set to N to only update last N days (for incremental updates)

# Batch query configuration
BATCH_SIZE = 21  # Unified batch size (fundamentals strictest: 500÷23=21 stocks)
VALUATION_BATCH_SIZE = BATCH_SIZE
FUNDAMENTAL_BATCH_SIZE = BATCH_SIZE

# ------------------------------ Logging configuration ------------------------------

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w",
)
logger = logging.getLogger(__name__)

# ------------------------------ Concurrency control ------------------------------


# Performance monitor
class PerformanceMonitor:
    def __init__(self):
        self.api_times = []
        self.lock = threading.Lock()

    def record(self, duration):
        with self.lock:
            self.api_times.append(duration)
            if len(self.api_times) >= 100:
                avg = sum(self.api_times) / len(self.api_times)
                logger.info("Recent 100 API avg time: {:.3f}s".format(avg))
                self.api_times = []


perf_monitor = PerformanceMonitor()

# ------------------------------ Core download functions ------------------------------


def download_fundamentals_batch(stock_batch, start_year, end_year):
    """
    Batch download quarterly financial indicators

    Args:
        stock_batch: List of stock codes (max 20)
        start_year: Start year (e.g. '2017')
        end_year: End year (e.g. '2025')

    Returns:
        {stock: {field: {end_date: value}}} or {}
    """
    # Group fields by table
    table_fields = {}
    for field, table in FUNDAMENTAL_FIELDS_MAPPING.items():
        if table not in table_fields:
            table_fields[table] = []
        table_fields[table].append(field)

    # Initialize cache
    stock_caches = {
        stock: {field: {} for field in FUNDAMENTAL_FIELDS_MAPPING.keys()}
        for stock in stock_batch
    }

    # Download by table
    for table, fields in table_fields.items():
        try:
            # Batch query this table
            data = get_fundamentals(
                stock_batch,
                table,
                fields=fields,
                start_year=start_year,
                end_year=end_year,
            )

            if data is not None and not data.empty:
                # Handle Panel or DataFrame
                for stock in stock_batch:
                    try:
                        # Panel type
                        if hasattr(data, "items"):
                            if stock in data.items:
                                df = data[stock]
                            else:
                                continue
                        else:
                            # DataFrame (single stock case)
                            df = data

                        # Extract data
                        if hasattr(df, "index"):
                            for end_date in df.index:
                                for field in fields:
                                    if field in df.columns:
                                        value = df.loc[end_date, field]
                                        stock_caches[stock][field][end_date] = (
                                            value if pd.notna(value) else None
                                        )
                    except Exception as e:
                        logger.debug("Extract {} data failed: {}".format(stock, str(e)))
                        continue

            logger.info(
                "{} table batch download: {} stocks".format(table, len(stock_batch))
            )

        except Exception as e:
            logger.error("{} table batch download failed: {}".format(table, str(e)))

    return stock_caches


def download_valuation_batch(
    stock_batch, trading_days, max_retries=3, progress_callback=None
):
    """
    Batch download valuation data for multiple stocks

    Args:
        stock_batch: List of stock codes (max 60)
        trading_days: List of trading days
        max_retries: Max retry count
        progress_callback: Progress callback function(completed, total)

    Returns:
        {stock: DataFrame} or {}
    """
    # Initialize cache for each stock
    stock_caches = {
        stock: {field: {} for field in VALUATION_FIELDS} for stock in stock_batch
    }

    completed_dates = 0
    total_dates = len(trading_days)
    dates_lock = threading.Lock()  # Counter lock

    def fetch_one_date(date_point):
        """Batch query valuation data for single date"""
        nonlocal completed_dates

        if isinstance(date_point, str):
            date_str = date_point.replace("-", "")
        else:
            date_str = date_point.strftime("%Y%m%d")

        # Retry mechanism
        for retry in range(max_retries):
            try:
                # Batch query
                api_start = time.time()
                data = get_fundamentals(
                    stock_batch, "valuation", fields=VALUATION_FIELDS, date=date_str
                )
                api_duration = time.time() - api_start
                perf_monitor.record(api_duration)

                # Progress callback (thread-safe)
                with dates_lock:
                    completed_dates += 1
                    current_completed = completed_dates

                if progress_callback:
                    progress_callback(current_completed, total_dates)

                return (date_point, data, None)
            except Exception as e:
                error_msg = str(e)
                if "gaierror" in error_msg or "Name or service" in error_msg:
                    if retry < max_retries - 1:
                        logger.warning(
                            "Network error, retry {}/{}: batch - {}".format(
                                retry + 1, max_retries, error_msg
                            )
                        )
                        time.sleep(2)
                        continue
                return (date_point, None, error_msg)

        return (date_point, None, "Max retries exceeded")

    # Concurrent query all dates
    results = []
    with ThreadPoolExecutor(
        max_workers=7
    ) as executor:  # Lower concurrency to avoid rate limiting
        futures = {executor.submit(fetch_one_date, date): date for date in trading_days}

        for future in as_completed(futures):
            results.append(future.result())

    # Process results
    success_dates = 0
    for date_point, data, error in results:
        if error is None and data is not None and not data.empty:
            # Iterate each stock
            for stock in stock_batch:
                if stock in data.index:
                    for field in VALUATION_FIELDS:
                        if field in data.columns:
                            value = data.loc[stock, field]
                            stock_caches[stock][field][date_point] = (
                                value if pd.notna(value) else None
                            )
                        else:
                            stock_caches[stock][field][date_point] = None
                else:
                    for field in VALUATION_FIELDS:
                        stock_caches[stock][field][date_point] = None
            success_dates += 1
        else:
            # Query failed, fill None for all stocks on this date
            for stock in stock_batch:
                for field in VALUATION_FIELDS:
                    stock_caches[stock][field][date_point] = None

    # Convert to DataFrame
    result_dfs = {}
    dates_ts = pd.to_datetime(trading_days)

    for stock in stock_batch:
        field_cache = stock_caches[stock]

        # Check if has valid data
        has_data = any(len(field_cache[field]) > 0 for field in VALUATION_FIELDS)
        if not has_data:
            logger.warning(
                "Valuation download failed: {} - no valid data".format(stock)
            )
            continue

        data_dict = {}
        for field in VALUATION_FIELDS:
            series = pd.Series(field_cache[field])
            if len(series) > 0:
                series.index = pd.to_datetime(series.index)
                series = series.sort_index()
                series = series.reindex(dates_ts, method="ffill")
                data_dict[field] = series.values
            else:
                data_dict[field] = [None] * len(dates_ts)

        df = pd.DataFrame(data_dict, index=dates_ts)
        df.index.name = "date"

        # Force convert to numeric type
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        result_dfs[stock] = df

    logger.info(
        "Batch download complete: {} stocks, success dates {}/{}".format(
            len(result_dfs), success_dates, len(trading_days)
        )
    )

    return result_dfs


# ------------------------------ Storage functions ------------------------------


def load_existing_stocks():
    """Load already downloaded stock list"""
    hdf5_path = Path(HDF5_FILE)
    if not hdf5_path.exists():
        return set()

    try:
        with pd.HDFStore(HDF5_FILE, mode="r") as store:
            val_stocks = set(
                key.split("/")[-1]
                for key in store.keys()
                if key.startswith("/valuation/")
            )
            fund_stocks = set(
                key.split("/")[-1]
                for key in store.keys()
                if key.startswith("/fundamentals/")
            )
            # Only count as complete if both exist
            return val_stocks & fund_stocks
    except Exception as e:
        logger.warning("Read existing data failed: {}".format(str(e)))
        return set()


def save_batch_to_disk(batch_data):
    """Incremental save batch data to HDF5 (fast append mode, no compression)"""
    if not batch_data:
        return

    hdf5_path = Path(HDF5_FILE)
    mode = "a" if hdf5_path.exists() else "w"

    # Download phase: no compression, fast write
    with pd.HDFStore(HDF5_FILE, mode=mode) as store:
        for stock, stock_data in batch_data.items():
            # Save valuation (daily)
            if "valuation" in stock_data:
                df = stock_data["valuation"]
                key = "valuation/{}".format(stock)
                if key in store:
                    del store[key]
                store.put(key, df, format="fixed")

            # Save fundamentals (quarterly)
            if "fundamentals" in stock_data:
                field_cache = stock_data["fundamentals"]

                # Get all quarter time points
                all_dates = set()
                for field_dict in field_cache.values():
                    all_dates.update(field_dict.keys())

                if all_dates:
                    all_dates = sorted(list(all_dates))
                    dates_ts = pd.to_datetime(all_dates)

                    # Build DataFrame (fast mode, no sorting)
                    data_dict = {}
                    for field in FUNDAMENTAL_FIELDS_MAPPING.keys():
                        field_data = field_cache.get(field, {})
                        values = [field_data.get(d) for d in all_dates]
                        data_dict[field] = values

                    df = pd.DataFrame(data_dict, index=dates_ts)
                    df.index.name = "end_date"

                    key = "fundamentals/{}".format(stock)
                    if key in store:
                        del store[key]
                    store.put(key, df, format="fixed")

    logger.info("Saved batch data: {} stocks".format(len(batch_data)))


# ------------------------------ Main download process ------------------------------


def download_all_data(incremental_days=None):
    """
    Main download process (valuation + fundamentals)

    Args:
        incremental_days: If set, only update last N days for existing stocks
    """
    print("=" * 70)
    if incremental_days:
        print("Fundamentals incremental update: last {} days".format(incremental_days))
    else:
        print("Fundamentals data batch download (optimized)")
    print("=" * 70)
    print("Output file: {}".format(HDF5_FILE))
    print("Log file: {}".format(LOG_FILE))
    print("Unified batch: {} stocks/batch".format(BATCH_SIZE))

    # Get date range
    end_date = (
        datetime.now().date()
        if END_DATE is None
        else datetime.strptime(END_DATE, "%Y-%m-%d").date()
    )

    # For incremental mode, adjust start_date (valuation is daily)
    if incremental_days:
        start_date = end_date - timedelta(days=incremental_days)
        print(
            "\nIncremental mode: updating last {} days of valuation data".format(
                incremental_days
            )
        )
    else:
        start_date = datetime.strptime(START_DATE, "%Y-%m-%d").date()

    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_year = start_date.year
    end_year = end_date.year

    print("\nDate range: {} ~ {}".format(start_date_str, end_date_str))
    print("Year range: {} ~ {}".format(start_year, end_year))

    # Get trading calendar (for valuation)
    print("\nGetting trading calendar...")
    trading_days = get_trade_days(start_date_str, end_date_str)
    trading_days_list = [str(d) for d in trading_days]
    print("  Trading calendar: {} trading days".format(len(trading_days_list)))

    # Get stock pool
    print("\nGetting stock pool...")

    # For incremental mode, load existing stocks
    if incremental_days and Path(HDF5_FILE).exists():
        existing_stocks = load_existing_stocks()
        if existing_stocks:
            stock_pool = sorted(list(existing_stocks))
            print(
                "  Loaded {} existing stocks for incremental update".format(
                    len(stock_pool)
                )
            )
        else:
            stock_pool = []
    else:
        stock_pool = []

    # If not incremental or no existing stocks, fetch from API
    if not stock_pool:
        date_diff = (end_date - start_date).days
        sample_dates = [
            start_date,
            start_date + timedelta(days=date_diff // 4),
            start_date + timedelta(days=date_diff // 2),
            start_date + timedelta(days=date_diff * 3 // 4),
            end_date,
        ]

        all_stocks_set = set()
        for date_obj in sample_dates:
            date_str = date_obj.strftime("%Y%m%d")
            try:
                stocks = get_Ashares(date_str)
                if stocks:
                    all_stocks_set.update(stocks)
            except Exception as e:
                logger.error("Get stock pool failed: {} - {}".format(date_str, str(e)))

        stock_pool = sorted(list(all_stocks_set))
        print("  Stock pool: {} stocks".format(len(stock_pool)))

    if len(stock_pool) == 0:
        print("Error: stock pool is empty")
        return

    # Load already downloaded stocks
    existing_stocks = load_existing_stocks()
    need_download = [s for s in stock_pool if s not in existing_stocks]

    # Use unified batch size (ensure checkpoint batch boundaries consistent)
    batches = [
        need_download[i : i + BATCH_SIZE]
        for i in range(0, len(need_download), BATCH_SIZE)
    ]

    # Estimate API calls
    valuation_calls = len(batches) * len(trading_days_list)
    fundamental_calls = len(batches) * 4  # 4 tables
    total_calls = valuation_calls + fundamental_calls

    print("\nDownload statistics:")
    print("  Already done: {} stocks".format(len(existing_stocks)))
    print("  Need download: {} stocks".format(len(need_download)))
    print("  Batch count: {} batches".format(len(batches)))
    print("  Estimated API calls:")
    print("    Valuation: {} calls".format(valuation_calls))
    print("    Fundamentals: {} calls".format(fundamental_calls))
    print("    Total: {} calls".format(total_calls))
    print("  Estimated time: {:.1f} minutes".format(total_calls / 90 / 60))

    if len(need_download) == 0:
        print("\nAll stocks already downloaded!")
        return

    # Start download
    all_data = {}  # {stock: {'valuation': df, 'fundamentals': {field: {date: value}}}}
    success_count = 0
    fail_count = 0
    start_time = datetime.now()

    # Global progress tracking (based on API calls)
    total_batches = len(stock_pool) // BATCH_SIZE + (
        1 if len(stock_pool) % BATCH_SIZE else 0
    )
    total_api_calls = total_batches * len(trading_days_list) + total_batches * 4

    # API calls for already completed stocks (for progress display)
    completed_batches = len(existing_stocks) // BATCH_SIZE
    already_completed_calls = (
        completed_batches * len(trading_days_list) + completed_batches * 4
    )

    # API calls completed in current run (for speed and remaining time calculation)
    current_run_completed = 0

    # Periodic save configuration
    SAVE_INTERVAL = 10  # Auto save every 10 batches

    print("\nStarting batch download...")
    print("Note: Auto save every {} batches, checkpoint support".format(SAVE_INTERVAL))

    for batch_idx, stock_batch in enumerate(batches):
        batch_success = 0
        batch_start_run_calls = (
            current_run_completed  # Current run completed at batch start
        )
        batch_data = {}  # Temporary data for this batch

        # 1. Download valuation (daily) - with progress callback
        def valuation_progress(completed, total):
            nonlocal current_run_completed

            # Each downloaded day increases 1 API call
            current_run_completed = batch_start_run_calls + completed

            # Total progress = already completed + current run completed
            total_completed = already_completed_calls + current_run_completed
            overall_progress = total_completed / total_api_calls

            bar_length = 50
            filled = int(bar_length * overall_progress)
            bar = "=" * filled + "-" * (bar_length - filled)

            # Calculate remaining time based on current run speed
            elapsed = (datetime.now() - start_time).total_seconds()
            if current_run_completed > 0:
                avg_time = elapsed / current_run_completed
                remaining_calls = total_api_calls - total_completed
                remaining = avg_time * remaining_calls
                time_info = " | Elapsed {:.1f}min Remaining {:.1f}min".format(
                    elapsed / 60, remaining / 60
                )
            else:
                time_info = ""

            print(
                "\r[Valuation] [{}] {}/{} API ({:.1f}%){}".format(
                    bar,
                    total_completed,
                    total_api_calls,
                    overall_progress * 100,
                    time_info,
                ),
                end="",
            )

        valuation_success = False
        try:
            valuation_result = download_valuation_batch(
                stock_batch, trading_days_list, progress_callback=valuation_progress
            )

            for stock, df in valuation_result.items():
                if stock not in batch_data:
                    batch_data[stock] = {}
                batch_data[stock]["valuation"] = df

            current_run_completed = batch_start_run_calls + len(trading_days_list)
            valuation_success = True

        except Exception as e:
            logger.error(
                "Valuation batch download failed: batch {} - {}".format(
                    batch_idx, str(e)
                )
            )
            current_run_completed = batch_start_run_calls + len(trading_days_list)

        # 2. Download fundamentals (quarterly)
        fundamentals_success = False
        try:
            fund_result = download_fundamentals_batch(
                stock_batch, str(start_year), str(end_year)
            )

            for stock, field_cache in fund_result.items():
                if stock not in batch_data:
                    batch_data[stock] = {}
                batch_data[stock]["fundamentals"] = field_cache

            current_run_completed += 4
            fundamentals_success = True

        except Exception as e:
            logger.error(
                "Fundamentals batch download failed: batch {} - {}".format(
                    batch_idx, str(e)
                )
            )
            current_run_completed += 4

        # Only save and count as success if both succeed
        if valuation_success and fundamentals_success:
            # Verify each stock has complete data
            for stock in stock_batch:
                if (
                    stock in batch_data
                    and "valuation" in batch_data[stock]
                    and "fundamentals" in batch_data[stock]
                ):
                    batch_success += 1
                    if stock not in all_data:
                        all_data[stock] = {}
                    all_data[stock] = batch_data[stock]

        success_count += batch_success
        fail_count += len(stock_batch) - batch_success

        # Summary after batch completion
        total_completed = already_completed_calls + current_run_completed
        overall_progress = total_completed / total_api_calls
        elapsed = (datetime.now() - start_time).total_seconds()
        avg_time = elapsed / current_run_completed if current_run_completed > 0 else 0
        remaining = avg_time * (total_api_calls - total_completed)

        print(
            "\rBatch {}/{} | API {}/{} ({:.1f}%) | Elapsed {:.1f}min Remaining {:.1f}min | Success:{} Failed:{} | Memory:{}stocks".format(
                batch_idx + 1,
                len(batches),
                total_completed,
                total_api_calls,
                overall_progress * 100,
                elapsed / 60,
                remaining / 60,
                success_count,
                fail_count,
                len(all_data),
            )
        )

        # Save each batch (checkpoint support)
        if all_data:
            save_batch_to_disk(all_data)
            all_data.clear()  # Clear memory
            if (batch_idx + 1) % 10 == 0:
                print("\nSaved {} batches".format(batch_idx + 1))

    print("\n\n" + "=" * 70)
    print("Download complete, starting HDF5 optimization...")
    print("=" * 70)

    # Final optimization: sort, type conversion, compression
    hdf5_path = Path(HDF5_FILE)
    if hdf5_path.exists():
        print("Reading original data...")
        temp_file = HDF5_FILE + ".optimized.tmp"

        # Read all data
        all_stocks = {}
        with pd.HDFStore(HDF5_FILE, mode="r") as old_store:
            val_keys = sorted(
                [k for k in old_store.keys() if k.startswith("/valuation/")]
            )
            fund_keys = sorted(
                [k for k in old_store.keys() if k.startswith("/fundamentals/")]
            )

            print("Reading valuation data: {} stocks".format(len(val_keys)))
            for key in val_keys:
                stock = key.split("/")[-1]
                if stock not in all_stocks:
                    all_stocks[stock] = {}
                all_stocks[stock]["valuation"] = old_store[key]

            print("Reading fundamentals data: {} stocks".format(len(fund_keys)))
            for key in fund_keys:
                stock = key.split("/")[-1]
                if stock not in all_stocks:
                    all_stocks[stock] = {}
                all_stocks[stock]["fundamentals"] = old_store[key]

        # Sort by stock code and rewrite (with compression)
        print("Sorting and compressing write...")
        sorted_stocks = sorted(all_stocks.keys())

        with pd.HDFStore(
            temp_file, mode="w", complevel=HDF5_COMPLEVEL, complib=HDF5_COMPLIB
        ) as new_store:
            for idx, stock in enumerate(sorted_stocks):
                stock_data = all_stocks[stock]

                # Save valuation (sort + type conversion)
                if "valuation" in stock_data:
                    df = stock_data["valuation"].sort_index()
                    for col in df.columns:
                        df[col] = df[col].astype("float64")
                    key = "valuation/{}".format(stock)
                    new_store.put(key, df, format="fixed")

                # Save fundamentals (sort + type conversion)
                if "fundamentals" in stock_data:
                    df = stock_data["fundamentals"].sort_index()
                    for col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    key = "fundamentals/{}".format(stock)
                    new_store.put(key, df, format="fixed")

                # Progress display
                if (idx + 1) % 100 == 0:
                    print(
                        "\r  Optimization progress: {}/{}".format(
                            idx + 1, len(sorted_stocks)
                        ),
                        end="",
                    )

        print("\r  Optimization complete: {} stocks".format(len(sorted_stocks)))

        # Replace original file
        import os
        import shutil

        old_size = os.path.getsize(HDF5_FILE) / (1024 * 1024)
        shutil.move(temp_file, HDF5_FILE)
        new_size = os.path.getsize(HDF5_FILE) / (1024 * 1024)

        print("\nOptimization result:")
        print("  Original size: {:.1f} MB".format(old_size))
        print("  After compression: {:.1f} MB".format(new_size))
        print(
            "  Compression ratio: {:.1f}%".format(
                (1 - new_size / old_size) * 100 if old_size > 0 else 0
            )
        )

    print("\n" + "=" * 70)
    print("All complete")
    print("=" * 70)

    # Display file size and statistics
    hdf5_path = Path(HDF5_FILE)
    if hdf5_path.exists():
        file_size = hdf5_path.stat().st_size / (1024 * 1024)
        print("Data file: {}".format(HDF5_FILE))
        print("File size: {:.1f} MB".format(file_size))
        print("\nData structure:")
        print("  /valuation/{{stock}} - daily valuation data")
        print("  /fundamentals/{{stock}} - quarterly financial indicators")

        # Count actually saved stocks
        with pd.HDFStore(HDF5_FILE, mode="r") as store:
            val_count = len([k for k in store.keys() if k.startswith("/valuation/")])
            fund_count = len(
                [k for k in store.keys() if k.startswith("/fundamentals/")]
            )
            print("\nActually saved:")
            print("  Valuation: {} stocks".format(val_count))
            print("  Fundamentals: {} stocks".format(fund_count))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Download PTrade fundamentals HDF5 data"
    )
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
