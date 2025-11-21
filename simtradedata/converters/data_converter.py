"""
Data converter to transform fetched data into PTrade-compatible format
"""

import logging
from typing import Dict

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class DataConverter:
    """
    Convert data from various sources to PTrade format

    This handles:
    - Field name mapping
    - Data type conversion
    - Index formatting
    - Data structure reorganization
    """

    # Field mapping for market data
    MARKET_FIELD_MAP = {
        "date": "date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
        "amount": "money",  # BaoStock 'amount' -> PTrade 'money'
    }

    # Field mapping for valuation data
    VALUATION_FIELD_MAP = {
        "peTTM": "pe_ttm",
        "pbMRQ": "pb",
        "psTTM": "ps_ttm",
        "pcfNcfTTM": "pcf",
        "turn": "turnover_rate",
    }

    # Field mapping for fundamentals (BaoStock -> PTrade)
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

    def convert_market_data(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Convert market data to PTrade format

        Args:
            df: Raw market data from data source
            symbol: Stock code in PTrade format

        Returns:
            DataFrame in PTrade format with columns:
            [open, high, low, close, volume, money]
        """
        if df.empty:
            return df

        # Strict validation: ensure we have expected raw fields
        expected_fields = ["date", "open", "high", "low", "close", "volume", "amount"]
        missing_fields = [f for f in expected_fields if f not in df.columns]
        if missing_fields:
            raise ValueError(
                f"Missing expected market data fields: {missing_fields}. "
                f"Got columns: {list(df.columns)}"
            )

        # Ensure datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            if "date" in df.columns:
                df.set_index("date", inplace=True)
            df.index = pd.to_datetime(df.index)

        # Normalize datetime to remove time component (SimTradeLab uses date only)
        df.index = df.index.normalize()

        # Select and rename required columns
        result = pd.DataFrame(index=df.index)

        for src_field, tgt_field in self.MARKET_FIELD_MAP.items():
            if src_field in df.columns and src_field != "date":
                result[tgt_field] = df[src_field]
            elif src_field == "amount" and "amount" in df.columns:
                result["money"] = df["amount"]

        # Ensure column order matches SimTradeLab format
        # SimTradeLab uses: close, open, high, low, volume, money
        column_order = ["close", "open", "high", "low", "volume", "money"]
        result = result[[col for col in column_order if col in result.columns]]

        # Convert to appropriate data types
        for col in result.columns:
            result[col] = pd.to_numeric(result[col], errors="coerce")

        logger.info(
            f"Converted market data for {symbol}: {len(result)} rows, "
            f"{len(result.columns)} columns"
        )

        return result

    def convert_valuation_data(
        self, df: pd.DataFrame, market_df: pd.DataFrame, symbol: str
    ) -> pd.DataFrame:
        """
        Convert valuation data to PTrade format

        PTrade valuation includes: float_value, pb, pcf, pe_ttm, ps_ttm,
        total_shares, total_value, turnover_rate

        Args:
            df: Raw valuation data from BaoStock
            market_df: Market data (needed for calculating market cap)
            symbol: Stock code

        Returns:
            DataFrame with valuation indicators
        """
        if df.empty:
            return df

        # Strict validation: ensure we have the expected raw fields from BaoStock
        expected_fields = ["peTTM", "pbMRQ", "psTTM", "pcfNcfTTM", "turn"]
        missing_fields = [f for f in expected_fields if f not in df.columns]
        if missing_fields:
            raise ValueError(
                f"Missing expected valuation fields: {missing_fields}. "
                f"Got columns: {list(df.columns)}"
            )

        # Ensure datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            if "date" in df.columns:
                df.set_index("date", inplace=True)
            else:
                raise ValueError(
                    f"Valuation data must have 'date' column or DatetimeIndex. "
                    f"Got columns: {list(df.columns)}, index: {type(df.index).__name__}"
                )
            df.index = pd.to_datetime(df.index)

        # Rename columns
        result = df.rename(columns=self.VALUATION_FIELD_MAP)

        # Select PTrade fields
        ptrade_fields = ["pe_ttm", "pb", "ps_ttm", "pcf", "turnover_rate"]
        result = result[[col for col in ptrade_fields if col in result.columns]]

        # Calculate market value fields (需要股本数据,这里先填充 NaN)
        # TODO: Add total_shares, total_value, float_value calculation
        result["total_shares"] = np.nan
        result["total_value"] = np.nan
        result["float_value"] = np.nan

        logger.info(f"Converted valuation data for {symbol}: {len(result)} rows")

        return result

    def convert_fundamentals(
        self,
        profit_df: pd.DataFrame,
        operation_df: pd.DataFrame,
        growth_df: pd.DataFrame,
        balance_df: pd.DataFrame,
        cash_flow_df: pd.DataFrame,
        symbol: str,
    ) -> pd.DataFrame:
        """
        Merge and convert fundamental data from multiple BaoStock APIs

        PTrade fundamentals has 23 fields, sourced from:
        - profit data (盈利能力)
        - operation data (营运能力)
        - growth data (成长能力)
        - balance data (偿债能力)
        - cash flow data (现金流量)

        Args:
            profit_df: Profit ability data
            operation_df: Operation ability data
            growth_df: Growth ability data
            balance_df: Balance ability data
            cash_flow_df: Cash flow data
            symbol: Stock code

        Returns:
            DataFrame with 23 fundamental indicators
        """
        # Merge all dataframes on statDate (quarter end date)
        dfs = []

        for df in [profit_df, operation_df, growth_df, balance_df, cash_flow_df]:
            if not df.empty and "statDate" in df.columns:
                df = df.copy()
                df["end_date"] = pd.to_datetime(df["statDate"])
                df.set_index("end_date", inplace=True)
                dfs.append(df)

        if not dfs:
            return pd.DataFrame()

        # Merge all DataFrames
        result = dfs[0]
        for df in dfs[1:]:
            result = result.join(df, how="outer", rsuffix="_dup")

        # Map fields to PTrade names
        mapped_result = pd.DataFrame(index=result.index)

        for src_field, tgt_field in self.FUNDAMENTAL_FIELD_MAP.items():
            if src_field in result.columns:
                mapped_result[tgt_field] = result[src_field]

        # Add TTM fields (calculated from quarterly data)
        # TODO: Implement TTM calculations for fields ending with _ttm
        ttm_fields = [
            "roe_ttm",
            "roa_ttm",
            "gross_income_ratio_ttm",
            "net_profit_ratio_ttm",
            "roa_ebit_ttm",
        ]
        for field in ttm_fields:
            if field not in mapped_result.columns:
                mapped_result[field] = np.nan

        # Add missing fields with NaN
        ptrade_fields = [
            "accounts_receivables_turnover_rate",
            "basic_eps_yoy",
            "current_assets_turnover_rate",
            "current_ratio",
            "debt_equity_ratio",
            "gross_income_ratio",
            "gross_income_ratio_ttm",
            "interest_cover",
            "inventory_turnover_rate",
            "net_profit_grow_rate",
            "net_profit_ratio",
            "net_profit_ratio_ttm",
            "np_parent_company_yoy",
            "operating_revenue_grow_rate",
            "quick_ratio",
            "roa",
            "roa_ebit_ttm",
            "roa_ttm",
            "roe",
            "roe_ttm",
            "roic",
            "total_asset_grow_rate",
            "total_asset_turnover_rate",
        ]

        for field in ptrade_fields:
            if field not in mapped_result.columns:
                mapped_result[field] = np.nan

        # Select PTrade fields in order
        mapped_result = mapped_result[
            [col for col in ptrade_fields if col in mapped_result.columns]
        ]

        logger.info(
            f"Converted fundamentals for {symbol}: {len(mapped_result)} quarters, "
            f"{len(mapped_result.columns)} indicators"
        )

        return mapped_result

    def convert_adjust_factor(self, df: pd.DataFrame, symbol: str) -> pd.Series:
        """
        Convert adjust factor data to PTrade format

        PTrade uses backward adjust factor as a Series named 'backward_a'

        Args:
            df: DataFrame with foreAdjustFactor and backAdjustFactor
            symbol: Stock code

        Returns:
            Series with backward adjust factor
        """
        if df.empty:
            return pd.Series(dtype=float)

        # Strict validation
        expected_fields = ["date", "foreAdjustFactor", "backAdjustFactor"]
        missing_fields = [f for f in expected_fields if f not in df.columns]
        if missing_fields:
            raise ValueError(
                f"Missing expected adjust factor fields: {missing_fields}. "
                f"Got columns: {list(df.columns)}"
            )

        # Set date as index if it's a column
        if not isinstance(df.index, pd.DatetimeIndex):
            if "date" in df.columns:
                df = df.set_index("date")
            df.index = pd.to_datetime(df.index)

        # Extract backward adjust factor
        if "backAdjustFactor" in df.columns:
            result = df["backAdjustFactor"].astype(np.float32)
            result.name = "backward_a"
        else:
            result = pd.Series(dtype=np.float32, name="backward_a")

        logger.info(f"Converted adjust factor for {symbol}: {len(result)} days")

        return result

    def convert_exrights_data(
        self, dividend_df: pd.DataFrame, adjust_df: pd.DataFrame, symbol: str
    ) -> pd.DataFrame:
        """
        Convert dividend and adjust factor data to PTrade exrights format

        PTrade exrights fields:
        - allotted_ps: 配股比例
        - rationed_ps: 配股价格比例
        - rationed_px: 配股价格
        - bonus_ps: 送股比例
        - exer_forward_a, exer_forward_b: 前复权因子
        - exer_backward_a, exer_backward_b: 后复权因子

        Args:
            dividend_df: Dividend data from BaoStock
            adjust_df: Adjust factor data from BaoStock
            symbol: Stock code

        Returns:
            DataFrame with exrights information
        """
        if dividend_df.empty:
            return pd.DataFrame()

        result = pd.DataFrame()

        # Map dividend fields
        if "dividOperateDate" in dividend_df.columns:
            result["date"] = pd.to_datetime(
                dividend_df["dividOperateDate"], format="%Y-%m-%d", errors="coerce"
            )
            result["date"] = result["date"].dt.strftime("%Y%m%d").astype(int)

        result["allotted_ps"] = dividend_df.get("allotmentRatio", 0.0)
        result["rationed_ps"] = 0.0  # Not directly available
        result["rationed_px"] = dividend_df.get("allotmentPrice", 0.0)
        result["bonus_ps"] = dividend_df.get("perShareDivRatio", 0.0)

        # Merge adjust factors
        if not adjust_df.empty:
            # adjust_df now has 'date' as column (not index)
            adjust_df_copy = adjust_df.copy()
            adjust_df_copy["date"] = (
                adjust_df_copy["date"].dt.strftime("%Y%m%d").astype(int)
            )

            result = result.merge(
                adjust_df_copy[["date", "foreAdjustFactor", "backAdjustFactor"]],
                on="date",
                how="left",
            )

            result["exer_forward_a"] = result.get("foreAdjustFactor", np.nan)
            result["exer_forward_b"] = np.nan  # Needs calculation
            result["exer_backward_a"] = result.get("backAdjustFactor", np.nan)
            result["exer_backward_b"] = np.nan  # Needs calculation
        else:
            result["exer_forward_a"] = np.nan
            result["exer_forward_b"] = np.nan
            result["exer_backward_a"] = np.nan
            result["exer_backward_b"] = np.nan

        # Set date as index
        if "date" in result.columns:
            result.set_index("date", inplace=True)

        # Select PTrade fields
        ptrade_fields = [
            "allotted_ps",
            "rationed_ps",
            "rationed_px",
            "bonus_ps",
            "exer_forward_a",
            "exer_forward_b",
            "exer_backward_a",
            "exer_backward_b",
        ]
        result = result[[col for col in ptrade_fields if col in result.columns]]

        logger.info(f"Converted exrights data for {symbol}: {len(result)} records")

        return result

    def convert_stock_metadata(self, basic_df: pd.DataFrame, symbol: str) -> Dict:
        """
        Convert stock basic info to PTrade metadata format

        PTrade stock_metadata fields:
        - blocks: JSON string with block/concept info
        - de_listed_date: Delisting date
        - has_info: Boolean flag
        - listed_date: IPO date
        - stock_name: Stock name

        Args:
            basic_df: Stock basic info from BaoStock
            symbol: Stock code

        Returns:
            Dictionary with metadata
        """
        if basic_df.empty:
            return {}

        row = basic_df.iloc[0]

        metadata = {
            "stock_name": row.get("code_name", ""),
            "listed_date": pd.to_datetime(row.get("ipoDate", ""), errors="coerce"),
            "de_listed_date": pd.to_datetime(row.get("outDate", ""), errors="coerce"),
            "has_info": True,
            "blocks": "{}",  # TODO: Fetch industry classification
        }

        logger.info(f"Converted metadata for {symbol}")

        return metadata
