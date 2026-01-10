# -*- coding: utf-8 -*-
"""
Efficient BaoStock data download program

This program optimizes BaoStock API usage by:
1. Fetching market + valuation + status data in ONE API call
2. Splitting data in memory and routing to different HDF5 files
3. Reducing API calls by 33% compared to the original program

Key optimization: query_history_k_data_plus is called once per stock
instead of 3 times (market, valuation, status separately).
"""

import fcntl
import json
import logging
import os
import sys
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import baostock as bs
import pandas as pd
from tables import NaturalNameWarning
from tqdm import tqdm

# Import core components
from simtradedata.fetchers.baostock_fetcher import BaoStockFetcher
from simtradedata.fetchers.unified_fetcher import UnifiedDataFetcher
from simtradedata.processors.data_splitter import DataSplitter
from simtradedata.writers.h5_writer import HDF5Writer
from simtradedata.config.field_mappings import BENCHMARK_CONFIG
from simtradedata.utils.download_progress import DownloadProgress

warnings.filterwarnings("ignore", category=NaturalNameWarning)

# Configuration
OUTPUT_DIR = "data"
LOG_FILE = "data/download_efficient.log"
LOCK_FILE = "data/.download.lock"

# Date range configuration
START_DATE = "2017-01-01"  # Full historical data from 2017
END_DATE = None  # None means use current date
INCREMENTAL_DAYS = None  # Set to N to only update last N days

# Batch configuration
BATCH_SIZE = 20  # Number of stocks per batch
# Note: BaoStock does not support multi-threading, so downloads are sequential

# Logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w",
)
logger = logging.getLogger(__name__)


class ProcessLock:
    """
    Process lock to prevent multiple instances from running simultaneously

    Uses fcntl.flock for file-based locking. The lock is automatically
    released when the process exits (even on crash).
    """

    def __init__(self, lock_file: str):
        self.lock_file = Path(lock_file)
        self.lock_fd = None

    def __enter__(self):
        # Ensure lock file directory exists
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)

        # Open lock file
        self.lock_fd = open(self.lock_file, 'w')

        # Try to acquire exclusive lock (non-blocking)
        try:
            fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            # Write current PID to lock file for debugging
            self.lock_fd.write(str(os.getpid()))
            self.lock_fd.flush()
        except IOError:
            # Lock already held by another process
            print("\n错误: 检测到其他下载进程正在运行")
            print(f"锁文件: {self.lock_file}")
            print("\n如果确认没有其他进程在运行，请删除锁文件后重试:")
            print(f"  rm {self.lock_file}")
            print("\n或者检查正在运行的进程:")
            print("  ps aux | grep download_efficient")
            sys.exit(1)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Release lock and close file
        if self.lock_fd:
            try:
                fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_UN)
                self.lock_fd.close()
            except Exception:
                pass  # Ignore errors during cleanup

            # Remove lock file
            try:
                self.lock_file.unlink(missing_ok=True)
            except Exception:
                pass


class EfficientBaoStockDownloader:
    """
    Efficient BaoStock data downloader
    
    This downloader optimizes API usage by fetching multiple data types
    in a single call and routing them to appropriate HDF5 structures.
    """
    
    def __init__(self, output_dir: str = ".", skip_fundamentals: bool = False, skip_metadata: bool = False, progress_tracker: DownloadProgress = None):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize components
        self.unified_fetcher = UnifiedDataFetcher()
        self.standard_fetcher = BaoStockFetcher()  # For metadata and other data
        self.data_splitter = DataSplitter()
        self.writer = HDF5Writer(output_dir=output_dir)

        # Configuration
        self.skip_fundamentals = skip_fundamentals
        self.skip_metadata = skip_metadata

        # Progress tracker for resume capability
        self.progress = progress_tracker

        # Cache for status data (used to build stock_status_history)
        self.status_cache = {}

        # Track failed stocks for retry
        self.failed_stocks = []
        self.timeout_stocks = []
    
    def download_stock_data(
        self, symbol: str, start_date: str, end_date: str
    ) -> dict:
        """
        Download all data for a single stock

        Optimization: Uses unified fetcher to get market + valuation + status
        in ONE API call instead of three separate calls.

        Args:
            symbol: Stock code in PTrade format
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            Dict with metadata information
        """
        try:
            # === 1. Fetch unified daily data (ONE API call) ===
            logger.info(f"Starting download for {symbol}")
            unified_df = self.unified_fetcher.fetch_unified_daily_data(
                symbol, start_date, end_date
            )

            if unified_df.empty:
                logger.warning(f"No data for {symbol}")
                return None

            # === 2. Split data in memory ===
            split_data = self.data_splitter.split_data(unified_df)

            # === 3. Write to different HDF5 files ===
            # 3.1 Market data -> ptrade_data.h5/stock_data/{symbol}
            if 'market' in split_data:
                self.writer.write_market_data(symbol, split_data['market'], mode='a')

            # 3.2 Extract valuation data (will be written later after market cap calculation)
            # Note: Valuation data (PE, PB, PS, etc.) is obtained from daily history
            # and will be written even if skip_fundamentals=True
            valuation_data = split_data.get('valuation')

            # 3.3 Cache status data for later processing
            if 'status' in split_data:
                self.status_cache[symbol] = split_data['status']

            # === 4. Download other data (cannot be merged) ===
            # 4.1 Adjust factor
            try:
                adj_factor = self.standard_fetcher.fetch_adjust_factor(
                    symbol, start_date, end_date
                )
                if not adj_factor.empty:
                    # Extract backward adjust factor
                    adj_series = adj_factor.set_index('date')['backAdjustFactor']
                    self.writer.write_adjust_factor(symbol, adj_series, mode='a')
            except Exception as e:
                logger.warning(f"Failed to fetch adjust factor for {symbol}: {e}")
            
            # 4.2 Stock basic info (skip for incremental updates to save time)
            basic_info = {}
            if not self.skip_metadata:
                try:
                    basic_df = self.standard_fetcher.fetch_stock_basic(symbol)
                    if not basic_df.empty:
                        basic_info = {
                            'status': basic_df['status'].values[0],
                            'ipoDate': basic_df['ipoDate'].values[0],
                            'outDate': basic_df['outDate'].values[0],
                            'type': basic_df['type'].values[0],
                            'code_name': basic_df['code_name'].values[0]
                        }
                except Exception as e:
                    logger.warning(f"Failed to fetch basic info for {symbol}: {e}")

            # 4.3 Industry classification (skip for incremental updates to save time)
            industry_info = {}
            if not self.skip_metadata:
                try:
                    industry_df = self.standard_fetcher.fetch_stock_industry(symbol)
                    if not industry_df.empty:
                        industry_info = {
                            'industry': industry_df['industry'].values[0],
                            'industryClassification': industry_df['industryClassification'].values[0]
                        }
                except Exception as e:
                    logger.warning(f"Failed to fetch industry for {symbol}: {e}")
            
            # === 4.4 Quarterly fundamentals data (only if skip_fundamentals=False) ===
            # Note: This downloads quarterly financial statements (balance sheet, income, cash flow)
            # Valuation data from step 3.2 is independent and will be written regardless
            fundamental_df = pd.DataFrame()
            if not self.skip_fundamentals:
                try:
                    from simtradedata.utils.ttm_calculator import (
                        get_quarters_in_range,
                        calculate_ttm_indicators
                    )

                    # Get quarters in date range
                    quarters = get_quarters_in_range(start_date, end_date)

                    if quarters:
                        all_fundamentals = []

                        # Add progress bar for quarterly fundamentals
                        quarter_desc = f"{symbol} fundamentals"
                        for year, quarter in tqdm(quarters, desc=quarter_desc, leave=False, position=2):
                            fund_df = self.standard_fetcher.fetch_quarterly_fundamentals(
                                symbol, year, quarter
                            )
                            if not fund_df.empty:
                                all_fundamentals.append(fund_df)

                        if all_fundamentals:
                            # Combine all quarters
                            combined_df = pd.concat(all_fundamentals, ignore_index=True)

                            # Sort by end_date
                            if 'end_date' in combined_df.columns:
                                combined_df = combined_df.sort_values('end_date')
                                combined_df = combined_df.set_index('end_date')

                            # Calculate TTM indicators
                            combined_df = calculate_ttm_indicators(combined_df)

                            # Save for market cap calculation
                            fundamental_df = combined_df

                            # Write to HDF5
                            self.writer.write_fundamentals(symbol, combined_df, mode='a')

                            logger.info(
                                f"Saved fundamentals for {symbol}: {len(combined_df)} quarters"
                            )
                except Exception as e:
                    logger.warning(f"Failed to fetch fundamentals for {symbol}: {e}")

            # === 4.5 Write valuation data (always written, regardless of skip_fundamentals) ===
            # Valuation data includes: PE, PB, PS, PCF, turnover rate, close price
            # These are obtained from daily history API and are always available
            if valuation_data is not None and not valuation_data.empty:
                try:
                    from simtradedata.utils.market_cap_calculator import calculate_market_cap

                    # Calculate market cap using fundamental data (if available)
                    # If skip_fundamentals=True, fundamental_df is empty, market_cap will be NaN
                    valuation_with_cap = calculate_market_cap(
                        valuation_data, fundamental_df, symbol
                    )

                    # Write valuation data to ptrade_fundamentals.h5/valuation/{symbol}
                    self.writer.write_valuation(symbol, valuation_with_cap, mode='a')
                except Exception as e:
                    logger.error(f"Failed to calculate/write valuation for {symbol}: {e}")
                    # Fallback: write valuation without market cap
                    self.writer.write_valuation(symbol, valuation_data, mode='a')

            
            return {
                'stock_code': symbol,
                'stock_name': basic_info.get('code_name', ''),
                'listed_date': basic_info.get('ipoDate', ''),
                'de_listed_date': basic_info.get('outDate', ''),
                'blocks': json.dumps(industry_info, ensure_ascii=False) if industry_info else None,
                'has_info': bool(basic_info)
            }

        except TimeoutError as e:
            logger.error(f"Timeout downloading {symbol}: {e}")
            self.timeout_stocks.append(symbol)
            if self.progress:
                self.progress.mark_failed(symbol, "timeout")
            return None
        except Exception as e:
            logger.error(f"Failed to download {symbol}: {e}")
            self.failed_stocks.append(symbol)
            if self.progress:
                self.progress.mark_failed(symbol, str(e))
            return None
    
    def download_batch(
        self, stock_batch: list, start_date: str, end_date: str
    ) -> list:
        """
        Download data for a batch of stocks sequentially

        Note: BaoStock does not support multi-threading, so we process
        stocks sequentially instead of using ThreadPoolExecutor.

        Args:
            stock_batch: List of stock codes
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            List of metadata dicts
        """
        metadata_list = []

        # Sequential processing (BaoStock doesn't support multi-threading)
        # Add progress bar for stocks in current batch
        for stock in tqdm(stock_batch, desc="Batch stocks", leave=False, position=1):
            try:
                metadata = self.download_stock_data(stock, start_date, end_date)
                if metadata:
                    metadata_list.append(metadata)
                    # Mark as completed in progress tracker
                    if self.progress:
                        self.progress.mark_completed(stock)
            except Exception as e:
                logger.error(f"Exception downloading {stock}: {e}")

        return metadata_list


def download_all_data(incremental_days=None, skip_fundamentals=False, skip_metadata=False, start_date=None, resume=True):
    """
    Main download function

    Args:
        incremental_days: If set, only update last N days for existing stocks
        skip_fundamentals: If True, skip quarterly fundamentals download
        skip_metadata: If True, skip stock_basic and stock_industry (faster for incremental updates)
        start_date: Override default start date (YYYY-MM-DD)
        resume: If True, resume from previous progress (default: True)
    """
    # Acquire process lock to prevent multiple instances
    with ProcessLock(LOCK_FILE):
        print("=" * 70)
        print("Efficient BaoStock Data Download Program")
        print("=" * 70)
        if incremental_days:
            print(f"Mode: Incremental update (last {incremental_days} days)")
            # Auto-enable skip_metadata for incremental updates
            if not skip_metadata:
                skip_metadata = True
                print("  Auto-enabled: Skip metadata refresh (faster)")
        else:
            print("Mode: Full download")

        if skip_fundamentals:
            print("Fundamentals: Quarterly financial statements skipped")
            print("             (Valuation data like PE, PB will still be saved)")
        else:
            print("Fundamentals: Quarterly financial statements enabled")

        if skip_metadata:
            print("Metadata: Stock basic info and industry classification skipped (faster)")

        print("=" * 70)
        
        # Date range
        end_date = (
            datetime.now().date()
            if END_DATE is None
            else datetime.strptime(END_DATE, "%Y-%m-%d").date()
        )
        
        start_date = datetime.strptime(START_DATE, "%Y-%m-%d").date()
        if incremental_days:
            start_date = end_date - timedelta(days=incremental_days)
        
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        print(f"\nDate range: {start_date_str} ~ {end_date_str}")

        # Initialize progress tracker
        progress = DownloadProgress() if resume else None
        if progress and not resume:
            progress.reset()

        if progress:
            print(f"\nResume mode: {'Enabled' if resume else 'Disabled (fresh start)'}")
            if resume and progress.data.get('completed'):
                progress.print_summary()

            # Save configuration
            progress.update_config({
                'start_date': start_date_str,
                'end_date': end_date_str,
                'incremental_days': incremental_days,
                'skip_fundamentals': skip_fundamentals,
                'skip_metadata': skip_metadata
            })

        # Initialize downloader with progress tracker
        downloader = EfficientBaoStockDownloader(
            output_dir=OUTPUT_DIR,
            skip_fundamentals=skip_fundamentals,
            skip_metadata=skip_metadata,
            progress_tracker=progress
        )
        downloader.unified_fetcher.login()
        downloader.standard_fetcher.login()
        
        try:
            # === 1. Get stock pool (use cached if resuming) ===
            cached_pool = progress.get_stock_pool() if progress else []
            completed_count = len(progress.data.get("completed", [])) if progress else 0

            # Use cached pool only if it's larger than completed stocks
            # (indicating it's a complete pool, not just downloaded stocks)
            if resume and cached_pool and len(cached_pool) > completed_count:
                stock_pool = cached_pool
                print(f"\nUsing cached stock pool: {len(stock_pool)} stocks")
            else:
                # Fetch stock pool from BaoStock
                if resume and cached_pool:
                    print(f"\nCached stock pool incomplete ({len(cached_pool)} stocks), re-fetching...")

                print("\nGetting stock pool...")
                full_start_date = datetime.strptime(START_DATE, "%Y-%m-%d").date()
                sample_dates = pd.date_range(
                    start=full_start_date, end=end_date, freq="QS"
                ).to_pydatetime().tolist()

                if end_date not in [d.date() for d in sample_dates]:
                    sample_dates.append(datetime.combine(end_date, datetime.min.time()))

                all_stocks = set()
                for date_obj in tqdm(sample_dates, desc="Sampling stock pool"):
                    date_str = date_obj.strftime("%Y-%m-%d")
                    try:
                        # Use query_all_stock instead of fetch_stock_list_by_date
                        rs = bs.query_all_stock(day=date_str)
                        if rs.error_code == "0":
                            stocks_df = rs.get_data()
                            if not stocks_df.empty:
                                # Convert BaoStock codes to PTrade format
                                from simtradedata.utils.code_utils import convert_to_ptrade_code
                                ptrade_codes = [
                                    convert_to_ptrade_code(code, "baostock")
                                    for code in stocks_df['code'].tolist()
                                ]
                                all_stocks.update(ptrade_codes)
                        else:
                            logger.warning(f"Failed to get stock pool for {date_str}: {rs.error_msg}")
                    except Exception as e:
                        logger.error(f"Failed to get stock pool for {date_str}: {e}")

                stock_pool = sorted(list(all_stocks))
                print(f"  Total stocks: {len(stock_pool)}")

                # Save stock pool for future resume
                if progress:
                    progress.save_stock_pool(stock_pool)

            # Update progress total
            if progress:
                progress.set_total(len(stock_pool))

            # === 2. Determine stocks to download ===
            if incremental_days:
                # Incremental mode: update all existing stocks
                existing_stocks = set(downloader.writer.get_existing_stocks(file_type="market"))
                need_to_download = sorted(list(existing_stocks))
                print(f"\nIncremental mode: updating {len(need_to_download)} existing stocks")
            elif progress and resume:
                # Resume mode: get pending stocks from progress tracker
                need_to_download = progress.get_pending_stocks(stock_pool)
                print(f"\nResume mode: {len(need_to_download)} stocks pending")
            else:
                # Full mode: download new stocks only
                existing_stocks = set(downloader.writer.get_existing_stocks(file_type="market"))
                need_to_download = [s for s in stock_pool if s not in existing_stocks]
                print(f"\nFull mode: {len(existing_stocks)} stocks exist")
                print(f"  Need to download: {len(need_to_download)} new stocks")
            
            # === 3. Download stocks in batches ===
            if not need_to_download:
                print("\nAll stocks already downloaded! Skipping stock download...")
                all_metadata = []
                success = 0
                fail = 0
            else:
                batches = [
                    need_to_download[i:i+BATCH_SIZE]
                    for i in range(0, len(need_to_download), BATCH_SIZE)
                ]

                print(f"\nDownloading {len(need_to_download)} stocks in {len(batches)} batches...")
                print(f"Batch size: {BATCH_SIZE} (sequential processing)")
                print("Note: BaoStock does not support concurrent downloads\n")

                all_metadata = []
                success = 0
                fail = 0

                for batch_idx, batch in enumerate(tqdm(batches, desc="Downloading batches", position=0)):
                    try:
                        metadata_list = downloader.download_batch(batch, start_date_str, end_date_str)
                        all_metadata.extend(metadata_list)
                        success += len(metadata_list)
                        fail += len(batch) - len(metadata_list)
                    except Exception as e:
                        logger.error(f"Batch {batch_idx} failed: {e}")
                        fail += len(batch)

                print(f"\nDownload complete: {success} success, {fail} failed")

            # Save final progress
            if progress:
                progress.save()
                progress.print_summary()

            # === 4. Save metadata ===
            if all_metadata:
                print("\nSaving stock metadata...")
                meta_df = pd.DataFrame(all_metadata)
                meta_df.set_index("stock_code", inplace=True)
                meta_df = meta_df.sort_index()

                # Read existing metadata and merge to avoid duplicates
                try:
                    with pd.HDFStore(downloader.writer.ptrade_data_path, mode='r') as store:
                        if 'stock_metadata' in store:
                            existing_meta = store['stock_metadata']
                            # Combine: new metadata overwrites old for same stocks
                            meta_df = pd.concat([existing_meta, meta_df])
                            meta_df = meta_df[~meta_df.index.duplicated(keep='last')]
                except (FileNotFoundError, KeyError):
                    pass  # No existing metadata, use new data only

                downloader.writer.write_stock_metadata(meta_df, mode='w')
            
            # === 5. Download global data ===
            print("\nDownloading global data...")
            
            # 5.1 Trading calendar
            try:
                new_trade_days = downloader.standard_fetcher.fetch_trade_calendar(
                    start_date_str, end_date_str
                )
                if not new_trade_days.empty:
                    new_trade_days = new_trade_days[new_trade_days['is_trading_day'] == '1']
                    new_trade_days['trade_date'] = pd.to_datetime(new_trade_days['calendar_date'])
                    new_trade_days = new_trade_days[['trade_date']].set_index('trade_date')

                    # Use refactored merge method
                    downloader.writer.merge_and_write_global_data(
                        '/trade_days',
                        new_trade_days,
                        downloader.writer.write_trade_days
                    )
                    print(f"  Trading calendar: {len(new_trade_days)} days fetched")
            except Exception as e:
                logger.error(f"Failed to download trading calendar: {e}")

            # 5.2 Benchmark index data
            # Use configured benchmark index (default: CSI300)
            BENCHMARK_INDEX = BENCHMARK_CONFIG['default_index']
            try:
                print(f"  Downloading benchmark index ({BENCHMARK_INDEX})...")
                benchmark_df = downloader.unified_fetcher.fetch_index_data(
                    BENCHMARK_INDEX, start_date_str, end_date_str
                )

                if not benchmark_df.empty:
                    # Use refactored merge method
                    downloader.writer.merge_and_write_global_data(
                        '/benchmark',
                        benchmark_df,
                        downloader.writer.write_benchmark
                    )
                    print(f"  Benchmark index: {len(benchmark_df)} days fetched")
            except Exception as e:
                logger.error(f"Failed to download benchmark data: {e}")

            # 5.3 Index constituents
            index_constituents = {}
            for date_obj in tqdm(sample_dates, desc="Downloading index constituents"):
                date_str = date_obj.strftime("%Y%m%d")
                index_constituents[date_str] = {}
                
                for index_code in ['000016.SS', '000300.SS', '000905.SS']:
                    try:
                        stocks_df = downloader.standard_fetcher.fetch_index_stocks(
                            index_code, date_obj.strftime("%Y-%m-%d")
                        )
                        if not stocks_df.empty:
                            from simtradedata.utils.code_utils import convert_to_ptrade_code
                            ptrade_codes = [
                                convert_to_ptrade_code(code, "baostock")
                                for code in stocks_df['code'].tolist()
                            ]
                            index_constituents[date_str][index_code] = ptrade_codes
                    except Exception as e:
                        logger.error(f"Failed to get index {index_code} for {date_str}: {e}")

            # 5.4 Save global metadata
            # Read existing metadata to preserve historical information
            actual_start_date = start_date_str
            existing_index_constituents = {}

            try:
                with pd.HDFStore(downloader.writer.ptrade_data_path, mode='r') as store:
                    if 'metadata' in store:
                        existing_meta = store['metadata']
                        # Preserve original start_date (earliest date)
                        if 'start_date' in existing_meta.index:
                            existing_start = existing_meta['start_date']
                            if existing_start and existing_start < start_date_str:
                                actual_start_date = existing_start

                        # Merge index constituents
                        if 'index_constituents' in existing_meta.index:
                            try:
                                existing_index_constituents = json.loads(existing_meta['index_constituents'])
                            except:
                                pass
            except (FileNotFoundError, KeyError):
                pass  # No existing metadata

            # Merge index constituents: new dates overwrite old
            merged_constituents = existing_index_constituents.copy()
            merged_constituents.update(index_constituents)

            global_meta = pd.Series({
                'download_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'start_date': actual_start_date,  # Preserve earliest start date
                'end_date': end_date_str,
                'stock_count': len(stock_pool),
                'sample_count': len(sample_dates),
                'format_version': 3,
                'index_constituents': json.dumps(merged_constituents, ensure_ascii=False),
                'stock_status_history': json.dumps({}, ensure_ascii=False)  # TODO: Build from status_cache
            })
            downloader.writer.write_global_metadata(global_meta, mode='w')
            
        finally:
            downloader.unified_fetcher.logout()
            downloader.standard_fetcher.logout()
        
        # === 6. Summary ===
        print("\n" + "=" * 70)
        print("Download Complete!")
        print("=" * 70)

        output_dir = Path(OUTPUT_DIR)
        ptrade_data_path = output_dir / "ptrade_data.h5"
        fundamentals_path = output_dir / "ptrade_fundamentals.h5"
        adj_path = output_dir / "ptrade_adj_pre.h5"

        ptrade_data_size = ptrade_data_path.stat().st_size / (1024 * 1024) if ptrade_data_path.exists() else 0
        fundamentals_size = fundamentals_path.stat().st_size / (1024 * 1024) if fundamentals_path.exists() else 0
        adj_size = adj_path.stat().st_size / (1024 * 1024) if adj_path.exists() else 0

        print(f"\nOutput directory: {output_dir.absolute()}")
        print(f"ptrade_data.h5: {ptrade_data_size:.1f} MB")
        print(f"ptrade_fundamentals.h5: {fundamentals_size:.1f} MB")
        print(f"ptrade_adj_pre.h5: {adj_size:.1f} MB")
        print(f"Total: {ptrade_data_size + fundamentals_size + adj_size:.1f} MB")
        
        print(f"\nOptimization: Reduced API calls by 33%")
        print(f"  Traditional: 6 calls/stock × {success} stocks = {6 * success} calls")
        print(f"  Optimized: 4 calls/stock × {success} stocks = {4 * success} calls")
        print(f"  Saved: {2 * success} API calls")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Efficient BaoStock data download program with resume capability"
    )
    parser.add_argument(
        "--incremental",
        type=int,
        metavar="DAYS",
        help="Incremental update: only update last N days for existing stocks",
    )
    parser.add_argument(
        "--skip-fundamentals",
        action="store_true",
        help="Skip quarterly fundamentals download (financial statements). "
             "Note: Valuation data (PE, PB, PS, etc.) from daily history will still be saved.",
    )
    parser.add_argument(
        "--skip-metadata",
        action="store_true",
        help="Skip stock basic info and industry classification (faster, auto-enabled for incremental mode)",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Disable resume from previous progress (start fresh)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset progress file and start fresh download",
    )

    args = parser.parse_args()

    # Handle reset flag
    if args.reset:
        progress = DownloadProgress()
        progress.reset()
        print("Progress reset. Starting fresh download...")

    incremental = args.incremental or INCREMENTAL_DAYS
    resume = not args.no_resume and not args.reset

    download_all_data(
        incremental_days=incremental,
        skip_fundamentals=args.skip_fundamentals,
        skip_metadata=args.skip_metadata,
        resume=resume
    )
