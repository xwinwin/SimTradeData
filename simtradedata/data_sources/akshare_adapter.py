"""
AkShare数据源适配器

提供AkShare数据源的统一接口实现。
"""

import logging
import os
from datetime import date
from typing import Any, Dict, List, Optional, Union

# 禁用AkShare的进度条
os.environ["TQDM_DISABLE"] = "1"


from .base import BaseDataSource, DataSourceDataError

logger = logging.getLogger(__name__)


class AkShareAdapter(BaseDataSource):
    """AkShare数据源适配器"""

    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化AkShare适配器

        Args:
            config: 配置参数
        """
        super().__init__("akshare", config)
        self._akshare = None

        # AkShare特定配置
        self.tool = self.config.get("tool", "pandas")  # pandas/numpy
        self.timeout = self.config.get("timeout", 10)

    def connect(self) -> bool:
        """连接AkShare"""
        import akshare as ak

        self._akshare = ak
        self._connected = True
        return True

    def disconnect(self):
        """断开AkShare连接"""
        self._akshare = None
        self._connected = False
        logger.info("AkShare连接已断开")

    def is_connected(self) -> bool:
        """检查连接状态"""
        return self._connected and self._akshare is not None

    def get_daily_data(
        self,
        symbol: str,
        start_date: Union[str, date],
        end_date: Union[str, date] = None,
    ) -> Dict[str, Any]:
        """
        获取日线数据

        Args:
            symbol: 股票代码 (如: 000001.SZ)
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            Dict[str, Any]: 日线数据
        """
        if not self.is_connected():
            self.connect()

        symbol = self._normalize_symbol(symbol)
        start_date = self._normalize_date(start_date)
        end_date = self._normalize_date(end_date) if end_date else start_date

        def _fetch_data():
            # 转换为AkShare格式
            ak_symbol = self._convert_to_akshare_symbol(symbol)

            # 获取日线数据
            df = self._akshare.stock_zh_a_hist(
                symbol=ak_symbol,
                period="daily",
                start_date=start_date.replace("-", ""),
                end_date=end_date.replace("-", ""),
                adjust="qfq",  # 前复权
            )

            return self._convert_daily_data(df, symbol)

        return self._retry_request(_fetch_data)

    def get_minute_data(
        self, symbol: str, trade_date: Union[str, date], frequency: str = "5m"
    ) -> Dict[str, Any]:
        """
        获取分钟线数据

        Args:
            symbol: 股票代码
            trade_date: 交易日期
            frequency: 频率 (1m/5m/15m/30m/60m)

        Returns:
            Dict[str, Any]: 分钟线数据
        """
        if not self.is_connected():
            self.connect()

        symbol = self._normalize_symbol(symbol)
        trade_date = self._normalize_date(trade_date)
        frequency = self._validate_frequency(frequency)

        def _fetch_data():
            try:
                # 转换为AkShare格式
                ak_symbol = self._convert_to_akshare_symbol(symbol)

                # 转换频率
                period_map = {
                    "1m": "1",
                    "5m": "5",
                    "15m": "15",
                    "30m": "30",
                    "60m": "60",
                }

                if frequency not in period_map:
                    raise DataSourceDataError(f"AkShare不支持频率: {frequency}")

                period = period_map[frequency]

                # 获取分钟线数据
                df = self._akshare.stock_zh_a_hist_min_em(
                    symbol=ak_symbol,
                    period=period,
                    start_date=trade_date + " 09:00:00",
                    end_date=trade_date + " 15:30:00",
                    adjust="qfq",
                )

                if df.empty:
                    raise DataSourceDataError(
                        f"未获取到分钟线数据: {symbol} {trade_date}"
                    )

                # 转换为标准格式
                return self._convert_minute_data(df, symbol, trade_date, frequency)

            except Exception as e:
                logger.error(f"AkShare获取分钟线数据失败 {symbol} {trade_date}: {e}")
                raise DataSourceDataError(f"获取分钟线数据失败: {e}")

        return self._retry_request(_fetch_data)

    def get_stock_info(
        self, symbol: str = None
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        获取股票基础信息

        Args:
            symbol: 股票代码，为None时返回所有股票

        Returns:
            Union[Dict, List[Dict]]: 股票信息
        """
        if not self.is_connected():
            self.connect()

        def _fetch_data():
            try:
                if symbol:
                    # 获取单个股票信息
                    symbol_norm = self._normalize_symbol(symbol)
                    ak_symbol = self._convert_to_akshare_symbol(symbol_norm)

                    # 获取股票基本信息
                    try:
                        df = self._akshare.stock_individual_info_em(symbol=ak_symbol)
                        return self._convert_stock_detail_data(df, symbol_norm)
                    except Exception as e:
                        logger.warning(f"获取单个股票信息失败 {symbol}: {e}")
                        # 尝试从股票列表中获取基本信息
                        all_stocks = self._akshare.stock_info_a_code_name()
                        if not all_stocks.empty:
                            stock_row = all_stocks[all_stocks["代码"] == ak_symbol]
                            if not stock_row.empty:
                                return self._convert_basic_stock_data(
                                    stock_row.iloc[0], symbol_norm
                                )
                        return {"success": False, "data": None, "error": str(e)}
                else:
                    # 获取所有股票列表
                    df = self._akshare.stock_info_a_code_name()
                    return self._convert_stock_list_data(df)

            except Exception as e:
                logger.error(f"获取股票信息失败: {e}")
                return {"success": False, "data": None, "error": str(e)}

        return self._retry_request(_fetch_data)

    def _convert_stock_list_data(self, df) -> Dict[str, Any]:
        """转换股票列表数据格式"""
        if df is None or df.empty:
            return {"success": False, "data": None, "error": "股票列表为空"}

        try:
            # 转换为标准格式
            stocks = []
            for _, row in df.iterrows():
                code = str(row.get("代码", ""))
                name = str(row.get("名称", ""))

                if code and name and len(code) == 6:  # 确保是6位代码
                    stock_data = {
                        "symbol": code,  # 不带后缀的代码
                        "name": name.strip(),
                        "market": self._determine_market_from_code(code),
                    }
                    stocks.append(stock_data)

            return {"success": True, "data": stocks, "count": len(stocks)}

        except Exception as e:
            logger.error(f"转换股票列表数据失败: {e}")
            return {"success": False, "data": None, "error": str(e)}

    def _convert_stock_detail_data(self, df, symbol: str) -> Dict[str, Any]:
        """转换股票详细信息数据格式"""
        if df is None or df.empty:
            return {"success": False, "data": None, "error": "股票详细信息为空"}

        try:
            # 将Series转换为字典
            if hasattr(df, "to_dict"):
                data_dict = df.to_dict()
            else:
                data_dict = dict(df)

            # 提取关键信息
            detail_data = {
                "symbol": symbol,
                "name": self._safe_str(data_dict.get("股票简称", "")),
                "total_shares": self._extract_shares(data_dict.get("总股本", 0)),
                "float_shares": self._extract_shares(data_dict.get("流通股", 0)),
                "list_date": self._extract_date(data_dict.get("上市日期", "")),
                "industry": self._safe_str(data_dict.get("所属行业", "")),
                "market_cap": self._extract_number(data_dict.get("总市值", 0)),
            }

            return {"success": True, "data": detail_data}

        except Exception as e:
            logger.error(f"转换股票详细信息失败 {symbol}: {e}")
            return {"success": False, "data": None, "error": str(e)}

    def _convert_basic_stock_data(self, row, symbol: str) -> Dict[str, Any]:
        """转换基本股票数据格式"""
        try:
            basic_data = {
                "symbol": symbol,
                "name": self._safe_str(row.get("名称", "")),
                "market": self._determine_market_from_code(
                    self._convert_to_akshare_symbol(symbol)
                ),
            }

            return {"success": True, "data": basic_data}

        except Exception as e:
            logger.error(f"转换基本股票数据失败 {symbol}: {e}")
            return {"success": False, "data": None, "error": str(e)}

    def _determine_market_from_code(self, code: str) -> str:
        """从股票代码确定市场"""
        if code.startswith("0") or code.startswith("3"):
            return "SZ"
        elif code.startswith("6") or code.startswith("9"):
            return "SS"
        elif code.startswith("8"):
            return "BJ"
        else:
            return "SZ"

    def _extract_shares(self, value) -> Optional[float]:
        """提取股本数据（单位：股）"""
        try:
            if value is None or str(value).lower() in ["nan", "", "--"]:
                return None

            str_value = str(value).replace(",", "").strip()

            # 处理带单位的数据
            if "万股" in str_value:
                num_value = float(str_value.replace("万股", ""))
                return num_value * 10000
            elif "亿股" in str_value:
                num_value = float(str_value.replace("亿股", ""))
                return num_value * 100000000
            elif "股" in str_value:
                return float(str_value.replace("股", ""))
            else:
                # 假设已经是股数
                return float(str_value)

        except (ValueError, TypeError):
            return None

    def _extract_number(self, value) -> Optional[float]:
        """提取数值"""
        try:
            if value is None or str(value).lower() in ["nan", "", "--"]:
                return None
            return float(str(value).replace(",", ""))
        except (ValueError, TypeError):
            return None

    def _extract_date(self, value) -> Optional[str]:
        """提取日期"""
        try:
            if value is None or str(value).lower() in ["nan", "", "--"]:
                return None

            import re

            str_value = str(value).strip()

            # YYYY-MM-DD 格式
            if re.match(r"\d{4}-\d{2}-\d{2}", str_value):
                return str_value[:10]
            # YYYYMMDD 格式
            elif re.match(r"\d{8}", str_value):
                return f"{str_value[:4]}-{str_value[4:6]}-{str_value[6:8]}"
            else:
                return None

        except Exception:
            return None

    def _safe_str(self, value) -> str:
        """安全字符串转换"""
        if value is None or str(value).lower() == "nan":
            return ""
        return str(value).strip()

    def get_fundamentals(
        self, symbol: str, report_date: Union[str, date], report_type: str = "Q4"
    ) -> Dict[str, Any]:
        """
        获取财务数据

        Args:
            symbol: 股票代码
            report_date: 报告期
            report_type: 报告类型

        Returns:
            Dict[str, Any]: 财务数据
        """
        if not self.is_connected():
            self.connect()

        symbol = self._normalize_symbol(symbol)
        report_date = self._normalize_date(report_date)

        def _fetch_data():
            ak_symbol = self._convert_to_akshare_symbol(symbol)

            # 获取财务数据
            df = self._akshare.stock_financial_abstract_ths(symbol=ak_symbol)

            return self._convert_financial_data(df, symbol)

        return self._retry_request(_fetch_data)

    def get_valuation_data(
        self, symbol: str, trade_date: Union[str, date]
    ) -> Dict[str, Any]:
        """获取估值数据"""
        if not self.is_connected():
            self.connect()

        symbol = self._normalize_symbol(symbol)
        trade_date = self._normalize_date(trade_date)

        def _fetch_data():
            try:
                ak_symbol = self._convert_to_akshare_symbol(symbol)

                # 获取实时数据 (包含估值指标)
                df = self._akshare.stock_zh_a_spot_em()
                stock_data = df[df["代码"] == ak_symbol]

                if stock_data.empty:
                    raise DataSourceDataError(f"未获取到估值数据: {symbol}")

                return {
                    "success": True,
                    "data": self._convert_valuation_data(
                        stock_data.iloc[0], symbol, trade_date
                    ),
                }

            except Exception as e:
                logger.error(f"AkShare获取估值数据失败 {symbol}: {e}")
                raise DataSourceDataError(f"获取估值数据失败: {e}")

        return self._retry_request(_fetch_data)

    def _convert_valuation_data(
        self, data, symbol: str, trade_date: str
    ) -> Dict[str, Any]:
        """转换估值数据格式"""

        def safe_float(value, default=0.0):
            """安全的浮点数转换"""
            if pd.isna(value) or value == "" or value is None:
                return default
            try:
                return float(value)
            except (ValueError, TypeError):
                return default

        return {
            "symbol": symbol,
            "date": trade_date,
            "pe_ratio": safe_float(data.get("市盈率-动态", 0)),
            "pb_ratio": safe_float(data.get("市净率", 0)),
            "ps_ratio": safe_float(data.get("市销率", 0)),
            "market_cap": safe_float(data.get("总市值", 0)) * 100000000,  # 转换为元
            "circulating_cap": safe_float(data.get("流通市值", 0))
            * 100000000,  # 转换为元
            "source": "akshare",
        }

    def _convert_daily_data(self, df, symbol: str) -> Dict[str, Any]:
        """转换日线数据格式"""
        if df is None or df.empty:
            return {"success": False, "data": None, "error": "数据为空"}

        try:
            # 转换DataFrame为标准格式
            records = []
            for _, row in df.iterrows():
                record = {
                    "symbol": symbol,
                    "date": str(row.get("日期", "")),
                    "open": float(row.get("开盘", 0) or 0),
                    "high": float(row.get("最高", 0) or 0),
                    "low": float(row.get("最低", 0) or 0),
                    "close": float(row.get("收盘", 0) or 0),
                    "volume": float(row.get("成交量", 0) or 0),
                    "amount": float(row.get("成交额", 0) or 0),
                }

                # 验证数据有效性
                if (
                    record["open"] > 0
                    and record["high"] > 0
                    and record["low"] > 0
                    and record["close"] > 0
                ):
                    records.append(record)

            return {"success": True, "data": records, "count": len(records)}

        except Exception as e:
            logger.error(f"转换日线数据失败: {e}")
            return {"success": False, "data": None, "error": str(e)}

    def _convert_financial_data(self, df, symbol: str) -> Dict[str, Any]:
        """转换财务数据格式"""
        if df is None or df.empty:
            return {"success": False, "data": None, "error": "财务数据为空"}

        try:
            # 提取第一行数据（最新财务数据）
            if len(df) > 0:
                row = df.iloc[0]
                financial_data = {
                    "revenue": self._safe_float(row.get("营业收入", 0)),
                    "net_profit": self._safe_float(row.get("净利润", 0)),
                    "total_assets": self._safe_float(row.get("总资产", 0)),
                    "operating_profit": self._safe_float(row.get("营业利润", 0)),
                    "eps": self._safe_float(row.get("每股收益", 0)),
                }

                return {"success": True, "data": financial_data}
            else:
                return {"success": False, "data": None, "error": "无财务数据"}

        except Exception as e:
            logger.error(f"转换财务数据失败: {e}")
            return {"success": False, "data": None, "error": str(e)}

    def _safe_float(self, value, default=0.0):
        """安全的浮点数转换"""
        try:
            import pandas as pd

            if pd.isna(value) or value == "" or value is None:
                return default
            return float(value)
        except (ValueError, TypeError):
            return default

    def _convert_to_akshare_symbol(self, symbol: str) -> str:
        """转换为AkShare股票代码格式"""
        # 移除市场后缀
        if "." in symbol:
            return symbol.split(".")[0]
        return symbol

    def get_stock_concepts(self, symbol: str) -> List[str]:
        """
        获取股票所属概念板块

        Args:
            symbol: 股票代码

        Returns:
            List[str]: 概念列表
        """
        if not self.is_connected():
            self.connect()

        symbol = self._normalize_symbol(symbol)
        ak_symbol = self._convert_to_akshare_symbol(symbol)

        def _fetch_concepts():
            try:
                concepts = []

                # 获取所有概念板块
                all_concepts = self._akshare.stock_board_concept_name_em()

                # 对于每个概念板块，检查股票是否属于该概念
                for _, concept_row in all_concepts.iterrows():
                    concept_name = concept_row.get("板块名称", "")
                    if concept_name:
                        try:
                            # 获取概念成分股
                            concept_stocks = self._akshare.stock_board_concept_cons_em(
                                symbol=concept_name
                            )

                            if not concept_stocks.empty:
                                # 检查股票是否在成分股中
                                matching_stocks = concept_stocks[
                                    concept_stocks["代码"] == ak_symbol
                                ]
                                if not matching_stocks.empty:
                                    concepts.append(concept_name)

                        except Exception:
                            # 某些概念板块可能无法获取成分股，跳过
                            continue

                return {"success": True, "data": concepts}

            except Exception as e:
                logger.error(f"获取股票概念失败 {symbol}: {e}")
                return {"success": False, "data": [], "error": str(e)}

        return self._retry_request(_fetch_concepts)

    def get_all_concepts(self) -> List[Dict[str, Any]]:
        """
        获取所有概念板块列表

        Returns:
            List[Dict]: 概念板块信息
        """
        if not self.is_connected():
            self.connect()

        def _fetch_all_concepts():
            try:
                # 获取所有概念板块
                concepts_df = self._akshare.stock_board_concept_name_em()

                if concepts_df.empty:
                    return {"success": True, "data": []}

                concepts = []
                for _, row in concepts_df.iterrows():
                    concept_info = {
                        "concept_name": row.get("板块名称", ""),
                        "concept_code": row.get("板块代码", ""),
                        "stock_count": row.get("成分股数量", 0),  # 如果有的话
                    }
                    concepts.append(concept_info)

                return {"success": True, "data": concepts}

            except Exception as e:
                logger.error(f"获取所有概念板块失败: {e}")
                return {"success": False, "data": [], "error": str(e)}

        return self._retry_request(_fetch_all_concepts)
