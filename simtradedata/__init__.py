"""
SimTradeData - Download market data and store in DuckDB with Parquet export

This package fetches data from BaoStock, converts it to PTrade-compatible
format, stores in DuckDB for incremental updates, and exports to Parquet.
"""

__version__ = "0.2.0"

from simtradedata.converters.data_converter import DataConverter
from simtradedata.fetchers.baostock_fetcher import BaoStockFetcher
from simtradedata.writers.duckdb_writer import DuckDBWriter

__all__ = [
    "BaoStockFetcher",
    "DataConverter",
    "DuckDBWriter",
]
