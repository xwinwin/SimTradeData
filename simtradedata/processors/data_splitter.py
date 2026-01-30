"""
Data splitter for routing unified data to different storage targets

This module splits unified data fetched from BaoStock into separate
DataFrames for different target tables.
"""

import logging
from typing import Dict

import pandas as pd

from simtradedata.config.field_mappings import DATA_ROUTING

logger = logging.getLogger(__name__)


class DataSplitter:
    """
    Split unified data into separate DataFrames for different targets

    This class takes a unified DataFrame containing market data, valuation data,
    and status data, and splits it into separate DataFrames that can be written
    to different DuckDB tables.
    """
    
    def __init__(self, routing_config: Dict = None):
        """
        Initialize data splitter
        
        Args:
            routing_config: Optional custom routing configuration.
                           If None, uses default DATA_ROUTING.
        """
        self.routing_config = routing_config or DATA_ROUTING
    
    def split_data(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Split unified DataFrame into separate DataFrames
        
        Args:
            df: Unified DataFrame with all fields
        
        Returns:
            Dict mapping data type to DataFrame:
            {
                'market': DataFrame with OHLCV data,
                'valuation': DataFrame with valuation indicators,
                'status': DataFrame with ST/HALT status
            }
        """
        if df.empty:
            logger.warning("Empty DataFrame provided to split_data")
            return {}
        
        result = {}
        
        for data_type, config in self.routing_config.items():
            fields = config['fields']
            
            # Check which fields are available in the DataFrame
            available_fields = [f for f in fields if f in df.columns]
            
            if not available_fields:
                logger.warning(
                    f"No fields available for {data_type} data type. "
                    f"Expected: {fields}, Available: {list(df.columns)}"
                )
                continue
            
            # Extract subset of data
            subset = df[available_fields].copy()
            
            # Rename fields to match PTrade format
            if config.get('rename'):
                subset = subset.rename(columns=config['rename'])
            
            # Set date as index (except for status data which keeps date as column)
            if 'date' in subset.columns and data_type != 'status':
                subset = subset.set_index('date')
            
            result[data_type] = subset
            
            logger.debug(
                f"Split {data_type} data: {len(subset)} rows, "
                f"{len(subset.columns)} columns"
            )
        
        logger.info(
            f"Data split complete: {len(result)} data types "
            f"({', '.join(result.keys())})"
        )

        return result
