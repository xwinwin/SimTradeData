"""
测试数据同步系统

验证增量同步、缺口检测、数据验证和同步管理功能。
"""

import logging
from datetime import date
from unittest.mock import Mock

import pytest

from simtradedata.config import Config
from simtradedata.data_sources.manager import DataSourceManager
from simtradedata.database.manager import DatabaseManager
from simtradedata.preprocessor.engine import DataProcessingEngine
from simtradedata.sync.gap_detector import GapDetector
from simtradedata.sync.incremental import IncrementalSync
from simtradedata.sync.manager import SyncManager
from simtradedata.sync.validator import DataValidator

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestIncrementalSync:
    """测试增量同步器"""

    @pytest.fixture
    def mock_components(self):
        """模拟组件"""
        db_manager = Mock(spec=DatabaseManager)
        data_source_manager = Mock(spec=DataSourceManager)
        preprocessor = Mock(spec=DataProcessingEngine)
        config = Config()

        return db_manager, data_source_manager, preprocessor, config

    def test_initialization(self, mock_components):
        """测试初始化"""
        db_manager, data_source_manager, preprocessor, config = mock_components

        sync = IncrementalSync(db_manager, data_source_manager, preprocessor, config)

        assert sync.db_manager is db_manager
        assert sync.data_source_manager is data_source_manager
        assert sync.processing_engine is preprocessor
        assert sync.config is config

        logger.info("✅ 增量同步器初始化测试通过")

    def test_get_last_data_date(self, mock_components):
        """测试获取最后数据日期"""
        db_manager, data_source_manager, preprocessor, config = mock_components
        sync = IncrementalSync(db_manager, data_source_manager, preprocessor, config)

        # 模拟数据库返回
        db_manager.fetchone.return_value = {"last_date": "2024-01-19"}

        last_date = sync.get_last_data_date("000001.SZ", "1d")

        assert last_date == date(2024, 1, 19)
        db_manager.fetchone.assert_called_once()

        logger.info("✅ 获取最后数据日期测试通过")

    def test_calculate_sync_range(self, mock_components):
        """测试计算同步范围"""
        db_manager, data_source_manager, preprocessor, config = mock_components
        sync = IncrementalSync(db_manager, data_source_manager, preprocessor, config)

        # 模拟有历史数据的情况
        db_manager.fetchone.return_value = {"last_date": "2024-01-19"}

        start_date, end_date = sync.calculate_sync_range("000001.SZ", date(2024, 1, 20))

        assert start_date == date(2024, 1, 20)  # 最后日期的下一天
        assert end_date == date(2024, 1, 20)

        # 模拟无历史数据的情况
        db_manager.fetchone.return_value = None

        start_date, end_date = sync.calculate_sync_range("000001.SZ", date(2024, 1, 20))

        assert start_date is not None
        assert end_date == date(2024, 1, 20)

        logger.info("✅ 计算同步范围测试通过")


class TestGapDetector:
    """测试缺口检测器"""

    @pytest.fixture
    def mock_db_manager(self):
        """模拟数据库管理器"""
        db_manager = Mock(spec=DatabaseManager)

        # 模拟交易日历
        trading_days = [
            {"trade_date": "2024-01-18"},
            {"trade_date": "2024-01-19"},
            {"trade_date": "2024-01-22"},
            {"trade_date": "2024-01-23"},
        ]

        # 模拟已有数据日期 (缺少2024-01-22)
        existing_dates = [
            {"trade_date": "2024-01-18"},
            {"trade_date": "2024-01-19"},
            {"trade_date": "2024-01-23"},
        ]

        # 模拟活跃股票
        active_symbols = [
            {"symbol": "000001.SZ"},
            {"symbol": "600000.SS"},
        ]

        def fetchall_side_effect(sql, params=None):
            if "ptrade_calendar" in sql:
                return trading_days
            elif "DISTINCT trade_date" in sql:
                return existing_dates
            elif "ptrade_stock_info" in sql:
                return active_symbols
            else:
                return []

        db_manager.fetchall.side_effect = fetchall_side_effect

        return db_manager

    def test_initialization(self, mock_db_manager):
        """测试初始化"""
        config = Config()
        detector = GapDetector(mock_db_manager, config)

        assert detector.db_manager is mock_db_manager
        assert detector.config is config

        logger.info("✅ 缺口检测器初始化测试通过")

    def test_detect_symbol_gaps(self, mock_db_manager):
        """测试检测单个股票缺口"""
        detector = GapDetector(mock_db_manager)

        # 模拟交易日历数据
        mock_db_manager.fetchall.side_effect = [
            # 交易日历查询
            [
                {"date": "2024-01-18"},
                {"date": "2024-01-19"},
                {"date": "2024-01-22"},
                {"date": "2024-01-23"},
            ],
            # 已有数据查询
            [
                {"date": "2024-01-18"},
                {"date": "2024-01-19"},
                {"date": "2024-01-23"},
                # 缺少2024-01-22
            ],
        ]

        gaps = detector.detect_symbol_gaps(
            "000001.SZ", date(2024, 1, 18), date(2024, 1, 23), "1d"
        )

        # 应该检测到2024-01-22的缺口
        assert len(gaps) >= 1

        # 检查缺口类型
        date_gaps = [gap for gap in gaps if gap["gap_type"] == "date_missing"]
        assert len(date_gaps) >= 1

        logger.info("✅ 检测单个股票缺口测试通过")


class TestDataValidator:
    """测试数据验证器"""

    @pytest.fixture
    def mock_db_manager(self):
        """模拟数据库管理器"""
        db_manager = Mock(spec=DatabaseManager)

        # 模拟股票数据
        mock_data = [
            {
                "symbol": "000001.SZ",
                "trade_date": "2024-01-20",
                "open": 10.0,
                "high": 10.5,
                "low": 9.8,
                "close": 10.2,
                "volume": 1000000,
                "preclose": 10.0,
                "quality_score": 95,
            },
            {
                "symbol": "000001.SZ",
                "trade_date": "2024-01-21",
                "open": 0.0,  # 异常数据
                "high": 10.3,
                "low": 9.9,
                "close": 10.1,
                "volume": 800000,
                "preclose": 10.2,
                "quality_score": 50,  # 低质量
            },
        ]

        # 模拟活跃股票
        active_symbols = [
            {"symbol": "000001.SZ"},
            {"symbol": "600000.SS"},
        ]

        def fetchall_side_effect(sql, params=None):
            if "ptrade_stock_info" in sql:
                return active_symbols
            elif "market_data" in sql:
                return mock_data
            else:
                return []

        db_manager.fetchall.side_effect = fetchall_side_effect

        return db_manager

    def test_initialization(self, mock_db_manager):
        """测试初始化"""
        config = Config()
        validator = DataValidator(mock_db_manager, config)

        assert validator.db_manager is mock_db_manager
        assert validator.config is config

        logger.info("✅ 数据验证器初始化测试通过")

    def test_validate_symbol_data(self, mock_db_manager):
        """测试验证单个股票数据"""
        validator = DataValidator(mock_db_manager)

        result = validator.validate_symbol_data(
            "000001.SZ", date(2024, 1, 20), date(2024, 1, 21), "1d"
        )

        assert result["symbol"] == "000001.SZ"
        assert result["total_records"] == 2
        assert result["valid_records"] >= 0
        assert result["invalid_records"] >= 0
        assert len(result["issues"]) >= 0

        # 检查是否检测到异常数据
        invalid_price_issues = [
            issue
            for issue in result["issues"]
            if issue["issue_type"] == "invalid_price"
        ]
        assert len(invalid_price_issues) >= 1

        logger.info("✅ 验证单个股票数据测试通过")


class TestSyncManager:
    """测试同步管理器"""

    @pytest.fixture
    def mock_components(self):
        """模拟组件"""
        db_manager = Mock(spec=DatabaseManager)
        data_source_manager = Mock(spec=DataSourceManager)
        preprocessor = Mock(spec=DataProcessingEngine)
        config = Config()

        # 模拟同步状态查询
        db_manager.fetchall.return_value = []
        db_manager.fetchone.return_value = {
            "total_records": 1000,
            "total_symbols": 50,
            "total_dates": 20,
            "avg_quality": 85.5,
        }

        return db_manager, data_source_manager, preprocessor, config

    def test_initialization(self, mock_components):
        """测试初始化"""
        db_manager, data_source_manager, preprocessor, config = mock_components

        manager = SyncManager(db_manager, data_source_manager, preprocessor, config)

        assert manager.db_manager is db_manager
        assert manager.data_source_manager is data_source_manager
        assert manager.processing_engine is preprocessor
        assert manager.incremental_sync is not None
        assert manager.gap_detector is not None
        assert manager.validator is not None

        logger.info("✅ 同步管理器初始化测试通过")

    def test_get_sync_status(self, mock_components):
        """测试获取同步状态"""
        db_manager, data_source_manager, preprocessor, config = mock_components
        manager = SyncManager(db_manager, data_source_manager, preprocessor, config)

        status = manager.get_sync_status()

        # 新架构返回字典格式，需要检查success字段和data字段
        assert status["success"] == True
        assert "recent_syncs" in status["data"]
        assert "data_stats" in status["data"]
        assert "components" in status["data"]
        assert "config" in status["data"]

        # 检查数据统计
        data_stats = status["data"]["data_stats"]
        assert data_stats["total_records"] == 1000
        assert data_stats["total_symbols"] == 50

        logger.info("✅ 获取同步状态测试通过")


def test_sync_system_integration():
    """数据同步系统集成测试"""
    logger.info("🚀 开始数据同步系统集成测试...")

    # 创建模拟组件
    config = Config()
    db_manager = Mock(spec=DatabaseManager)
    data_source_manager = Mock(spec=DataSourceManager)
    preprocessor = Mock(spec=DataProcessingEngine)

    # 模拟数据库返回
    db_manager.fetchone.return_value = {"last_date": "2024-01-19"}
    db_manager.fetchall.return_value = [
        {"symbol": "000001.SZ"},
        {"symbol": "600000.SS"},
    ]

    # 模拟预处理器返回
    preprocessor.process_symbol_data.return_value = True

    # 测试增量同步器
    incremental_sync = IncrementalSync(
        db_manager, data_source_manager, preprocessor, config
    )

    # 测试获取最后数据日期
    last_date = incremental_sync.get_last_data_date("000001.SZ", "1d")
    assert last_date == date(2024, 1, 19)

    # 测试计算同步范围
    start_date, end_date = incremental_sync.calculate_sync_range(
        "000001.SZ", date(2024, 1, 20)
    )
    assert start_date == date(2024, 1, 20)
    assert end_date == date(2024, 1, 20)

    # 测试缺口检测器
    gap_detector = GapDetector(db_manager, config)

    # 模拟交易日历和已有数据
    def fetchall_side_effect(sql, params=None):
        if "trading_calendar" in sql:
            return [
                {"date": "2024-01-18"},
                {"date": "2024-01-19"},
                {"date": "2024-01-22"},
            ]
        elif "DISTINCT date" in sql:
            return [
                {"date": "2024-01-18"},
                {"date": "2024-01-19"},
                # 缺少2024-01-22
            ]
        else:
            return [{"symbol": "000001.SZ"}]

    db_manager.fetchall.side_effect = fetchall_side_effect

    gaps = gap_detector.detect_symbol_gaps(
        "000001.SZ", date(2024, 1, 18), date(2024, 1, 22)
    )
    assert len(gaps) >= 1

    # 测试数据验证器
    validator = DataValidator(db_manager, config)

    # 模拟数据验证
    mock_data = [
        {
            "symbol": "000001.SZ",
            "trade_date": "2024-01-20",
            "open": 10.0,
            "high": 10.5,
            "low": 9.8,
            "close": 10.2,
            "volume": 1000000,
            "quality_score": 95,
        }
    ]

    db_manager.fetchall.return_value = mock_data

    validation_result = validator.validate_symbol_data(
        "000001.SZ", date(2024, 1, 20), date(2024, 1, 20)
    )
    assert validation_result["symbol"] == "000001.SZ"
    assert validation_result["total_records"] == 1

    # 测试同步管理器
    db_manager.fetchone.return_value = {
        "total_records": 1000,
        "total_symbols": 50,
        "total_dates": 20,
        "avg_quality": 85.5,
    }

    sync_manager = SyncManager(db_manager, data_source_manager, preprocessor, config)

    # 测试获取同步状态
    status = sync_manager.get_sync_status()
    assert status["success"] == True
    assert "data_stats" in status["data"]
    assert status["data"]["data_stats"]["total_records"] == 1000

    # 测试报告生成
    mock_full_result = {
        "target_date": "2024-01-20",
        "start_time": "2024-01-20T10:00:00",
        "end_time": "2024-01-20T10:05:00",
        "duration_seconds": 300,
        "summary": {"total_phases": 3, "successful_phases": 2, "failed_phases": 1},
        "phases": {
            "incremental_sync": {
                "status": "completed",
                "result": {
                    "total_symbols": 50,
                    "success_count": 45,
                    "error_count": 5,
                    "skipped_count": 0,
                },
            }
        },
    }

    report = sync_manager.generate_sync_report(mock_full_result)
    assert "数据同步报告" in report
    assert "增量同步" in report

    logger.info("🎉 数据同步系统集成测试通过!")


if __name__ == "__main__":
    # 运行集成测试
    test_sync_system_integration()

    # 运行pytest测试
    pytest.main([__file__, "-v"])
