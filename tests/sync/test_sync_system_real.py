"""
测试数据同步系统 - 使用真实对象

验证增量同步、缺口检测、数据验证和同步管理功能。
使用真实组件进行集成测试，不使用mock。
"""

import logging
import tempfile
from datetime import date
from pathlib import Path

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


@pytest.fixture
def real_components():
    """创建真实组件用于测试"""
    # 创建临时数据库
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    config = Config()
    db_manager = DatabaseManager(db_path, config=config)

    # 创建必要的测试表
    _create_test_tables(db_manager)

    # 插入基础测试数据
    _insert_test_data(db_manager)

    # 创建其他组件
    data_source_manager = DataSourceManager(config)
    preprocessor = DataProcessingEngine(db_manager, config)

    yield db_manager, data_source_manager, preprocessor, config

    # 清理
    db_manager.close()
    Path(db_path).unlink(missing_ok=True)


def _create_test_tables(db_manager):
    """创建测试表"""
    # 创建股票信息表 - 使用正确的表名
    db_manager.execute(
        """
        CREATE TABLE IF NOT EXISTS stocks (
            symbol TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            market TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            list_date DATE,
            industry_l1 TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    # 创建市场数据表 - 使用正确的字段名
    db_manager.execute(
        """
        CREATE TABLE IF NOT EXISTS market_data (
            symbol TEXT NOT NULL,
            date DATE NOT NULL,
            frequency TEXT NOT NULL DEFAULT '1d',
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            amount REAL,
            prev_close REAL,
            change_percent REAL,
            turnover_rate REAL,
            quality_score INTEGER DEFAULT 100,
            source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, date, frequency)
        )
    """
    )

    # 创建交易日历表 - 使用正确的表名
    db_manager.execute(
        """
        CREATE TABLE IF NOT EXISTS trading_calendar (
            date DATE NOT NULL,
            market TEXT NOT NULL,
            is_trading BOOLEAN NOT NULL,
            PRIMARY KEY (date, market)
        )
    """
    )

    # 创建同步状态表 - 使用正确的字段名
    db_manager.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_status (
            symbol TEXT NOT NULL,
            frequency TEXT NOT NULL,
            last_sync_date DATE,
            last_data_date DATE,
            status TEXT DEFAULT 'pending',
            error_message TEXT,
            total_records INTEGER DEFAULT 0,
            last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, frequency)
        )
    """
    )


def _insert_test_data(db_manager):
    """插入测试数据"""
    # 插入股票信息 - 使用正确的表名和字段名
    stocks = [
        ("000001.SZ", "平安银行", "SZ", "active", "1991-04-03", "银行"),
        ("000002.SZ", "万科A", "SZ", "active", "1991-01-29", "房地产"),
        ("600000.SS", "浦发银行", "SS", "active", "1999-11-10", "银行"),
    ]

    db_manager.executemany(
        "INSERT OR REPLACE INTO stocks (symbol, name, market, status, list_date, industry_l1) VALUES (?, ?, ?, ?, ?, ?)",
        stocks,
    )

    # 插入交易日历（简化版本：工作日为交易日）
    import datetime

    start_date = date(2024, 1, 1)
    for i in range(30):
        current_date = start_date + datetime.timedelta(days=i)
        is_trading = current_date.weekday() < 5
        db_manager.execute(
            "INSERT OR REPLACE INTO trading_calendar (date, market, is_trading) VALUES (?, ?, ?)",
            (str(current_date), "CN", is_trading),
        )

    # 插入一些历史数据（有意留一些缺口）- 使用正确的字段名
    market_data = [
        (
            "000001.SZ",
            "2024-01-15",
            "1d",
            10.0,
            10.5,
            9.8,
            10.2,
            1000000,
            10200000,
            10.0,
            2.0,
            5.5,
            95,
            "test",
        ),
        (
            "000001.SZ",
            "2024-01-16",
            "1d",
            10.2,
            10.8,
            10.0,
            10.5,
            1200000,
            12600000,
            10.2,
            2.9,
            6.2,
            90,
            "test",
        ),
        # 故意跳过 2024-01-17 创建缺口
        (
            "000001.SZ",
            "2024-01-18",
            "1d",
            10.5,
            11.0,
            10.3,
            10.8,
            1500000,
            16200000,
            10.5,
            2.9,
            7.8,
            85,
            "test",
        ),
        (
            "000002.SZ",
            "2024-01-15",
            "1d",
            8.0,
            8.3,
            7.8,
            8.1,
            800000,
            6480000,
            8.0,
            1.3,
            4.2,
            92,
            "test",
        ),
        (
            "000002.SZ",
            "2024-01-16",
            "1d",
            8.1,
            8.5,
            7.9,
            8.3,
            900000,
            7470000,
            8.1,
            2.5,
            4.8,
            88,
            "test",
        ),
        # 插入一些异常数据用于验证器测试
        (
            "600000.SS",
            "2024-01-15",
            "1d",
            0.0,
            12.5,
            11.8,
            12.2,
            2000000,
            24400000,
            12.0,
            1.7,
            4.5,
            30,
            "test",
        ),  # 开盘价异常
    ]

    db_manager.executemany(
        "INSERT OR REPLACE INTO market_data (symbol, date, frequency, open, high, low, close, volume, amount, prev_close, change_percent, turnover_rate, quality_score, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        market_data,
    )


class TestIncrementalSyncReal:
    """测试增量同步器 - 真实组件"""

    def test_initialization(self, real_components):
        """测试初始化"""
        db_manager, data_source_manager, preprocessor, config = real_components

        sync = IncrementalSync(db_manager, data_source_manager, preprocessor, config)

        assert sync.db_manager is db_manager
        assert sync.data_source_manager is data_source_manager
        assert sync.processing_engine is preprocessor
        assert sync.config is config

        logger.info("✅ 增量同步器初始化测试通过")

    def test_get_last_data_date(self, real_components):
        """测试获取最后数据日期"""
        db_manager, data_source_manager, preprocessor, config = real_components
        sync = IncrementalSync(db_manager, data_source_manager, preprocessor, config)

        # 测试有数据的情况
        last_date = sync.get_last_data_date("000001.SZ", "1d")
        assert last_date == date(2024, 1, 18)  # 最新的数据日期

        # 测试无数据的情况
        last_date = sync.get_last_data_date("999999.SZ", "1d")
        assert last_date is None

        logger.info("✅ 获取最后数据日期测试通过")

    def test_calculate_sync_range(self, real_components):
        """测试计算同步范围"""
        db_manager, data_source_manager, preprocessor, config = real_components
        sync = IncrementalSync(db_manager, data_source_manager, preprocessor, config)

        # 测试有历史数据的情况
        start_date, end_date = sync.calculate_sync_range("000001.SZ", date(2024, 1, 20))

        assert start_date == date(2024, 1, 19)  # 最后日期的下一天
        assert end_date == date(2024, 1, 20)

        # 测试无历史数据的情况
        start_date, end_date = sync.calculate_sync_range("999999.SZ", date(2024, 1, 20))

        assert start_date is not None  # 会有默认开始日期
        assert end_date == date(2024, 1, 20)

        logger.info("✅ 计算同步范围测试通过")


class TestGapDetectorReal:
    """测试缺口检测器 - 真实组件"""

    def test_initialization(self, real_components):
        """测试初始化"""
        db_manager, _, _, config = real_components

        detector = GapDetector(db_manager, config)

        assert detector.db_manager is db_manager
        assert detector.config is config

        logger.info("✅ 缺口检测器初始化测试通过")

    def test_detect_symbol_gaps(self, real_components):
        """测试检测单个股票缺口"""
        db_manager, _, _, config = real_components
        detector = GapDetector(db_manager, config)

        # 检测000001.SZ的缺口（我们故意跳过了2024-01-17）
        gaps = detector.detect_symbol_gaps(
            "000001.SZ", date(2024, 1, 15), date(2024, 1, 18), "1d"
        )

        # 应该检测到2024-01-17的缺口
        assert len(gaps) > 0

        # 检查缺口类型
        date_gaps = [gap for gap in gaps if gap["gap_type"] == "date_missing"]
        assert len(date_gaps) > 0

        # 检查具体的缺口日期 - 修复字段访问
        gap_found = False
        for gap in date_gaps:
            if "2024-01-17" in gap["start_date"] or "2024-01-17" in gap["end_date"]:
                gap_found = True
                break

        assert gap_found, f"未找到2024-01-17的缺口，实际缺口: {date_gaps}"

        logger.info("✅ 检测单个股票缺口测试通过")


class TestDataValidatorReal:
    """测试数据验证器 - 真实组件"""

    def test_initialization(self, real_components):
        """测试初始化"""
        db_manager, _, _, config = real_components

        validator = DataValidator(db_manager, config)

        assert validator.db_manager is db_manager
        assert validator.config is config

        logger.info("✅ 数据验证器初始化测试通过")

    def test_validate_symbol_data(self, real_components):
        """测试验证单个股票数据"""
        db_manager, _, _, config = real_components
        validator = DataValidator(db_manager, config)

        # 验证600000.SS的数据（我们插入了异常的开盘价0.0）
        result = validator.validate_symbol_data(
            "600000.SS", date(2024, 1, 15), date(2024, 1, 15), "1d"
        )

        assert result["symbol"] == "600000.SS"
        assert result["total_records"] == 1
        assert result["invalid_records"] > 0  # 应该检测到异常数据
        assert len(result["issues"]) > 0

        # 检查是否检测到价格异常
        price_issues = [
            issue
            for issue in result["issues"]
            if issue["issue_type"] == "invalid_price"
        ]
        assert len(price_issues) > 0

        logger.info("✅ 验证单个股票数据测试通过")


class TestSyncManagerReal:
    """测试同步管理器 - 真实组件"""

    def test_initialization(self, real_components):
        """测试初始化"""
        db_manager, data_source_manager, preprocessor, config = real_components

        manager = SyncManager(db_manager, data_source_manager, preprocessor, config)

        assert manager.db_manager is db_manager
        assert manager.data_source_manager is data_source_manager
        assert manager.processing_engine is preprocessor
        assert manager.incremental_sync is not None
        assert manager.gap_detector is not None
        assert manager.validator is not None

        logger.info("✅ 同步管理器初始化测试通过")

    def test_get_sync_status(self, real_components):
        """测试获取同步状态"""
        db_manager, data_source_manager, preprocessor, config = real_components
        manager = SyncManager(db_manager, data_source_manager, preprocessor, config)

        status = manager.get_sync_status()

        # 检查返回格式
        assert status["success"] == True
        assert "recent_syncs" in status["data"]
        assert "data_stats" in status["data"]
        assert "components" in status["data"]
        assert "config" in status["data"]

        # 检查数据统计
        data_stats = status["data"]["data_stats"]
        assert data_stats["total_records"] > 0  # 我们插入了测试数据
        assert data_stats["total_symbols"] >= 3  # 至少有3个股票

        logger.info("✅ 获取同步状态测试通过")


@pytest.mark.integration
def test_sync_system_real_integration():
    """数据同步系统真实集成测试"""
    logger.info("🚀 开始数据同步系统真实集成测试...")

    # 创建临时数据库
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        config = Config()
        db_manager = DatabaseManager(db_path, config=config)

        # 创建测试表和数据
        _create_test_tables(db_manager)
        _insert_test_data(db_manager)

        # 创建真实组件
        data_source_manager = DataSourceManager(config)
        preprocessor = DataProcessingEngine(db_manager, config)

        # 测试增量同步器
        incremental_sync = IncrementalSync(
            db_manager, data_source_manager, preprocessor, config
        )

        # 测试获取最后数据日期
        last_date = incremental_sync.get_last_data_date("000001.SZ", "1d")
        assert last_date == date(2024, 1, 18)

        # 测试计算同步范围
        start_date, end_date = incremental_sync.calculate_sync_range(
            "000001.SZ", date(2024, 1, 20)
        )
        assert start_date == date(2024, 1, 19)
        assert end_date == date(2024, 1, 20)

        # 测试缺口检测器
        gap_detector = GapDetector(db_manager, config)
        gaps = gap_detector.detect_symbol_gaps(
            "000001.SZ", date(2024, 1, 15), date(2024, 1, 18), "1d"
        )
        assert len(gaps) > 0  # 应该检测到2024-01-17的缺口

        # 测试数据验证器
        validator = DataValidator(db_manager, config)
        validation_result = validator.validate_symbol_data(
            "600000.SS", date(2024, 1, 15), date(2024, 1, 15), "1d"
        )
        assert validation_result["symbol"] == "600000.SS"
        assert validation_result["invalid_records"] > 0  # 异常数据

        # 测试同步管理器
        sync_manager = SyncManager(
            db_manager, data_source_manager, preprocessor, config
        )

        # 测试获取同步状态
        status = sync_manager.get_sync_status()
        assert status["success"] == True
        assert status["data"]["data_stats"]["total_records"] > 0

        # 测试生成报告
        mock_result = {
            "target_date": "2024-01-20",
            "start_time": "2024-01-20T10:00:00",
            "end_time": "2024-01-20T10:05:00",
            "duration_seconds": 300,
            "summary": {"total_phases": 3, "successful_phases": 2, "failed_phases": 1},
            "phases": {
                "incremental_sync": {
                    "status": "completed",
                    "result": {
                        "total_symbols": 3,
                        "success_count": 2,
                        "error_count": 1,
                        "skipped_count": 0,
                    },
                }
            },
        }

        report = sync_manager.generate_sync_report(mock_result)
        assert "数据同步报告" in report
        assert "增量同步" in report

        logger.info("🎉 数据同步系统真实集成测试通过!")

    finally:
        # 清理
        if "db_manager" in locals():
            db_manager.close()
        Path(db_path).unlink(missing_ok=True)


if __name__ == "__main__":
    # 运行集成测试
    test_sync_system_real_integration()

    # 运行pytest测试
    pytest.main([__file__, "-v"])
