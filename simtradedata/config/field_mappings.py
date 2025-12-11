"""
Centralized field mapping configurations

This module contains all field mapping definitions from various data sources
(BaoStock, QStock, etc.) to PTrade format. Centralizing mappings here ensures
consistency and makes maintenance easier.
"""

# BaoStock -> PTrade field mappings

MARKET_FIELD_MAP = {
    "date": "date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "amount": "money",  # BaoStock 'amount' -> PTrade 'money'
}

VALUATION_FIELD_MAP = {
    "peTTM": "pe_ttm",
    "pbMRQ": "pb",
    "psTTM": "ps_ttm",
    "pcfNcfTTM": "pcf",
    "turn": "turnover_rate",
}

FUNDAMENTAL_FIELD_MAP = {
    # From profit data
    "roeAvg": "roe",
    "roa": "roa",
    "npMargin": "net_profit_ratio",
    "gpMargin": "gross_income_ratio",
    # From balance data
    "currentRatio": "current_ratio",
    "quickRatio": "quick_ratio",
    "liabilityToAsset": "debt_equity_ratio",
    # From operation data
    "ARTurnRatio": "accounts_receivables_turnover_rate",
    "INVTurnRatio": "inventory_turnover_rate",
    "TATurnRatio": "total_asset_turnover_rate",
    "CATurnRatio": "current_assets_turnover_rate",
    # From growth data
    "YOYORev": "operating_revenue_grow_rate",
    "YOYNI": "net_profit_grow_rate",
    "YOYAsset": "total_asset_grow_rate",
    "YOYEPSBasic": "basic_eps_yoy",
    "YOYPNI": "np_parent_company_yoy",
    # From cash flow data
    "ebitToInterest": "interest_cover",
}

# Data routing configuration for DataSplitter
DATA_ROUTING = {
    'market': {
        'target_file': 'ptrade_data.h5',
        'target_path': 'stock_data/{symbol}',
        'fields': ['date', 'open', 'high', 'low', 'close', 'volume', 'amount'],
        'rename': {'amount': 'money'}  # Use MARKET_FIELD_MAP
    },
    'valuation': {
        'target_file': 'ptrade_fundamentals.h5',
        'target_path': 'valuation/{symbol}',
        'fields': ['date', 'close', 'peTTM', 'pbMRQ', 'psTTM', 'pcfNcfTTM', 'turn'],
        'rename': {  # Use VALUATION_FIELD_MAP
            'peTTM': 'pe_ttm',
            'pbMRQ': 'pb',
            'psTTM': 'ps_ttm',
            'pcfNcfTTM': 'pcf',
            'turn': 'turnover_rate'
        }
    },
    'status': {
        'target_file': 'memory',  # Stored in memory for building stock_status_history
        'target_path': None,
        'fields': ['date', 'isST', 'tradestatus'],
        'rename': {}
    }
}

# Benchmark configuration
BENCHMARK_CONFIG = {
    'default_index': '000300.SS',  # CSI300 (沪深300)
    'alternatives': {
        '000016.SS': 'SSE50',      # 上证50
        '000905.SS': 'CSI500',     # 中证500
        '000001.SS': 'SSE',        # 上证指数
        '399001.SZ': 'SZSE',       # 深证成指
        '399006.SZ': 'CYBZ',       # 创业板指
    }
}
