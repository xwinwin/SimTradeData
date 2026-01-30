"""
Data validators for ensuring data quality before writing to storage

This module provides validation functions to check data integrity,
field completeness, and value ranges before persisting data.
"""

import logging
from typing import List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when data quality validation fails"""
    pass


class MarketDataValidator:
    """Validator for market data (OHLCV)"""

    REQUIRED_FIELDS = ["open", "high", "low", "close", "volume", "money"]

    @staticmethod
    def validate(df: pd.DataFrame, symbol: str, strict: bool = False) -> bool:
        """
        Validate market data quality

        Args:
            df: Market data DataFrame
            symbol: Stock code for logging
            strict: If True, raise exception on validation failure

        Returns:
            True if validation passes

        Raises:
            DataQualityError: If strict=True and validation fails
        """
        if df.empty:
            msg = f"{symbol}: Empty DataFrame"
            if strict:
                raise DataQualityError(msg)
            logger.warning(msg)
            return False

        # 1. Check required fields
        missing = set(MarketDataValidator.REQUIRED_FIELDS) - set(df.columns)
        if missing:
            msg = f"{symbol}: Missing required fields {missing}"
            if strict:
                raise DataQualityError(msg)
            logger.error(msg)
            return False

        # 2. Check index type
        if not isinstance(df.index, pd.DatetimeIndex):
            msg = f"{symbol}: Index must be DatetimeIndex, got {type(df.index).__name__}"
            if strict:
                raise DataQualityError(msg)
            logger.error(msg)
            return False

        # 3. Check for duplicate dates
        if df.index.has_duplicates:
            dups = df.index[df.index.duplicated()].unique()
            msg = f"{symbol}: Duplicate dates in index: {dups.tolist()[:5]}"
            if strict:
                raise DataQualityError(msg)
            logger.error(msg)
            return False

        # 4. Value range checks
        issues = []

        # Close price should be positive
        if (df["close"] <= 0).any():
            invalid_count = (df["close"] <= 0).sum()
            issues.append(f"{invalid_count} non-positive close prices")

        # High >= Low
        if (df["high"] < df["low"]).any():
            invalid_count = (df["high"] < df["low"]).sum()
            issues.append(f"{invalid_count} rows where high < low")

        # Close should be between high and low
        out_of_range = (df["close"] > df["high"]) | (df["close"] < df["low"])
        if out_of_range.any():
            invalid_count = out_of_range.sum()
            issues.append(f"{invalid_count} rows where close not in [low, high]")

        # Volume should be non-negative
        if (df["volume"] < 0).any():
            invalid_count = (df["volume"] < 0).sum()
            issues.append(f"{invalid_count} negative volume values")

        if issues:
            msg = f"{symbol}: Data range issues: {'; '.join(issues)}"
            if strict:
                raise DataQualityError(msg)
            logger.warning(msg)
            return False

        # 5. Check for excessive NaN values
        nan_pct = df.isna().sum() / len(df) * 100
        high_nan_fields = nan_pct[nan_pct > 10].to_dict()
        if high_nan_fields:
            msg = (
                f"{symbol}: High NaN percentage: "
                f"{', '.join(f'{k}={v:.1f}%' for k, v in high_nan_fields.items())}"
            )
            logger.warning(msg)
            # Don't fail on high NaN, just warn

        logger.debug(f"{symbol}: Market data validation passed")
        return True


class ValuationDataValidator:
    """Validator for valuation data"""

    REQUIRED_FIELDS = ["pe_ttm", "pb", "ps_ttm", "pcf", "turnover_rate"]

    @staticmethod
    def validate(df: pd.DataFrame, symbol: str, strict: bool = False) -> bool:
        """
        Validate valuation data quality

        Args:
            df: Valuation data DataFrame
            symbol: Stock code for logging
            strict: If True, raise exception on validation failure

        Returns:
            True if validation passes
        """
        if df.empty:
            msg = f"{symbol}: Empty valuation DataFrame"
            if strict:
                raise DataQualityError(msg)
            logger.warning(msg)
            return False

        # 1. Check required fields (at least some should exist)
        available = set(ValuationDataValidator.REQUIRED_FIELDS) & set(df.columns)
        if len(available) == 0:
            msg = f"{symbol}: No valuation fields available"
            if strict:
                raise DataQualityError(msg)
            logger.error(msg)
            return False

        # 2. Check index type
        if not isinstance(df.index, pd.DatetimeIndex):
            msg = f"{symbol}: Index must be DatetimeIndex"
            if strict:
                raise DataQualityError(msg)
            logger.error(msg)
            return False

        # 3. Value range checks
        issues = []

        # PE, PB, PS, PCF should generally be positive (negative means loss)
        for field in ["pb", "pcf"]:
            if field in df.columns and (df[field] < 0).any():
                invalid_count = (df[field] < 0).sum()
                issues.append(f"{invalid_count} negative {field} values")

        # Turnover rate should be in reasonable range [0, 100]%
        if "turnover_rate" in df.columns:
            invalid = (df["turnover_rate"] < 0) | (df["turnover_rate"] > 100)
            if invalid.any():
                invalid_count = invalid.sum()
                issues.append(f"{invalid_count} turnover_rate out of [0, 100] range")

        if issues:
            msg = f"{symbol}: Valuation data issues: {'; '.join(issues)}"
            logger.warning(msg)
            # Don't fail on valuation issues, just warn

        logger.debug(f"{symbol}: Valuation data validation passed")
        return True


class FundamentalDataValidator:
    """Validator for fundamental data (quarterly)"""

    @staticmethod
    def validate(df: pd.DataFrame, symbol: str, strict: bool = False) -> bool:
        """
        Validate fundamental data quality

        Args:
            df: Fundamental data DataFrame
            symbol: Stock code for logging
            strict: If True, raise exception on validation failure

        Returns:
            True if validation passes
        """
        if df.empty:
            msg = f"{symbol}: Empty fundamental DataFrame"
            if strict:
                raise DataQualityError(msg)
            logger.warning(msg)
            return False

        # 1. Check index type
        if not isinstance(df.index, pd.DatetimeIndex):
            msg = f"{symbol}: Index must be DatetimeIndex (end_date)"
            if strict:
                raise DataQualityError(msg)
            logger.error(msg)
            return False

        # 2. Check for duplicate quarters
        if df.index.has_duplicates:
            dups = df.index[df.index.duplicated()].unique()
            msg = f"{symbol}: Duplicate quarters in index: {dups.tolist()}"
            if strict:
                raise DataQualityError(msg)
            logger.error(msg)
            return False

        # 3. Check for at least some non-NaN data
        non_nan_count = df.notna().sum().sum()
        total_count = df.size
        if non_nan_count == 0:
            msg = f"{symbol}: All fundamental data is NaN"
            if strict:
                raise DataQualityError(msg)
            logger.error(msg)
            return False

        # 4. Check percentage of available data
        data_pct = (non_nan_count / total_count) * 100
        if data_pct < 20:
            msg = f"{symbol}: Only {data_pct:.1f}% of fundamental data available"
            logger.warning(msg)

        logger.debug(f"{symbol}: Fundamental data validation passed")
        return True


def validate_before_write(
    data: pd.DataFrame,
    data_type: str,
    symbol: str,
    strict: bool = False
) -> bool:
    """
    Validate data before writing to storage

    Args:
        data: DataFrame to validate
        data_type: Type of data ("market", "valuation", "fundamental")
        symbol: Stock code for logging
        strict: If True, raise exception on validation failure

    Returns:
        True if validation passes

    Raises:
        DataQualityError: If strict=True and validation fails
    """
    if data_type == "market":
        return MarketDataValidator.validate(data, symbol, strict)
    elif data_type == "valuation":
        return ValuationDataValidator.validate(data, symbol, strict)
    elif data_type == "fundamental":
        return FundamentalDataValidator.validate(data, symbol, strict)
    else:
        logger.warning(f"Unknown data type: {data_type}, skipping validation")
        return True
