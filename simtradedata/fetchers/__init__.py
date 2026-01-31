"""Fetchers package"""

from simtradedata.fetchers.baostock_fetcher import BaoStockFetcher
from simtradedata.fetchers.mootdx_affair_fetcher import MootdxAffairFetcher
from simtradedata.fetchers.mootdx_fetcher import MootdxFetcher
from simtradedata.fetchers.mootdx_unified_fetcher import MootdxUnifiedFetcher

__all__ = [
    "BaoStockFetcher",
    "MootdxFetcher",
    "MootdxAffairFetcher",
    "MootdxUnifiedFetcher",
]
