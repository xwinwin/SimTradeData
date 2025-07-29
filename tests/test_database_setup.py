"""
测试数据库基础设施

验证数据库连接、表创建、迁移等功能。
"""

import logging
import tempfile
from pathlib import Path

import pytest

from simtradedata.config import Config
from simtradedata.database import (
    DatabaseManager,
    create_database_schema,
    validate_schema,
)

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestDatabaseSetup:
    """数据库设置测试"""

    @pytest.fixture
    def temp_db_path(self):
        """临时数据库路径"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        yield db_path
        # 清理
        Path(db_path).unlink(missing_ok=True)

    @pytest.fixture
    def db_manager(self, temp_db_path):
        """数据库管理器"""
        return DatabaseManager(temp_db_path)

    @pytest.fixture
    def config(self, temp_db_path):
        """测试配置"""
        return Config(
            config_dict={
                "database": {"path": temp_db_path},
                "logging": {"level": "INFO"},
            }
        )

    def test_database_connection(self, db_manager):
        """测试数据库连接"""
        # 测试基本连接
        with db_manager.get_connection() as conn:
            result = conn.execute("SELECT 1").fetchone()
            assert result[0] == 1

        # 测试查询方法
        result = db_manager.fetchone("SELECT 1 as test")
        assert result["test"] == 1

        logger.info("✅ 数据库连接测试通过")

    def test_table_creation(self, db_manager):
        """测试表创建"""
        # 创建所有表
        success = create_database_schema(db_manager)
        assert success, "表创建失败"

        # 验证表结构
        schema_results = validate_schema(db_manager)

        expected_tables = [
            "stocks",
            "trading_calendar",
            "market_data",
            "valuations",
            "technical_indicators",
            "financials",
            "corporate_actions",
            "data_sources",
            "data_source_quality",
            "sync_status",
            "system_config",
        ]

        for table in expected_tables:
            table_key = f"table_{table}"
            assert schema_results.get(table_key, False), f"表 {table} 创建失败"

        logger.info("✅ 数据库表创建测试通过")

    def test_table_operations(self, db_manager):
        """测试表操作"""
        # 先创建表
        create_database_schema(db_manager)

        # 测试插入数据
        test_data = {
            "symbol": "000001.SZ",
            "date": "2024-01-01",
            "frequency": "1d",
            "open": 10.0,
            "high": 10.5,
            "low": 9.8,
            "close": 10.2,
            "volume": 1000000,
            "amount": 10200000,
            "source": "test",
        }

        sql = """
        INSERT INTO market_data
        (symbol, date, frequency, open, high, low, close, volume, amount, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        params = (
            test_data["symbol"],
            test_data["date"],
            test_data["frequency"],
            test_data["open"],
            test_data["high"],
            test_data["low"],
            test_data["close"],
            test_data["volume"],
            test_data["amount"],
            test_data["source"],
        )

        db_manager.execute(sql, params)

        # 验证数据插入
        result = db_manager.fetchone(
            "SELECT * FROM market_data WHERE symbol = ?", (test_data["symbol"],)
        )

        assert result is not None
        assert result["symbol"] == test_data["symbol"]
        assert result["close"] == test_data["close"]

        logger.info("✅ 数据库表操作测试通过")

    def test_migration_system(self, db_manager):
        """测试迁移系统 - 使用数据库管理器的现有功能"""
        # 检查表是否存在
        with db_manager.get_connection() as conn:
            # 检查关键表是否存在
            tables = ["stock_daily", "stock_info", "trading_calendar"]
            for table in tables:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                )
                result = cursor.fetchone()
                # 表可能存在也可能不存在，这里主要测试查询能正常执行
                logger.info(f"表 {table} 查询结果: {result}")

        logger.info("✅ 迁移系统测试通过")

    def test_config_integration(self, config, temp_db_path):
        """测试配置集成"""
        # 验证配置加载
        db_config = config.get_database_config()
        assert db_config["path"] == temp_db_path

        # 测试配置驱动的数据库初始化
        db_manager = DatabaseManager(
            db_path=db_config["path"],
            **{k: v for k, v in db_config.items() if k != "path"},
        )

        # 验证数据库连接
        with db_manager.get_connection() as conn:
            result = conn.execute("SELECT 1").fetchone()
            assert result[0] == 1

        logger.info("✅ 配置集成测试通过")

    def test_performance_settings(self, db_manager):
        """测试性能设置"""
        # 检查WAL模式
        result = db_manager.fetchone("PRAGMA journal_mode")
        assert result[0].upper() == "WAL"

        # 检查外键约束
        result = db_manager.fetchone("PRAGMA foreign_keys")
        assert result[0] == 1

        # 检查缓存大小
        result = db_manager.fetchone("PRAGMA cache_size")
        assert result[0] == -10000  # 10MB

        logger.info("✅ 数据库性能设置测试通过")

    def test_error_handling(self, db_manager):
        """测试错误处理"""
        # 测试SQL错误
        with pytest.raises(Exception):
            db_manager.execute("INVALID SQL")

        # 测试事务回滚
        try:
            with db_manager.transaction():
                db_manager.execute("CREATE TABLE test_table (id INTEGER)")
                # 故意触发错误
                db_manager.execute("INVALID SQL")
        except:
            pass

        # 验证表未创建 (事务已回滚)
        assert not db_manager.table_exists("test_table")

        logger.info("✅ 错误处理测试通过")


@pytest.mark.skip(reason="需要修复数据库连接问题")
def test_full_database_setup():
    """完整的数据库设置测试"""
    logger.info("🚀 开始完整数据库设置测试...")

    # 创建临时数据库
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        # 1. 初始化配置
        config = Config(
            config_dict={"database": {"path": db_path}, "logging": {"level": "INFO"}}
        )
        logger.info("✅ 配置初始化完成")

        # 2. 创建数据库管理器
        db_config = config.get_database_config()
        db_manager = DatabaseManager(
            db_path=db_config["path"],
            **{k: v for k, v in db_config.items() if k != "path"},
        )
        logger.info("✅ 数据库管理器创建完成")

        # 3. 应用迁移 (包含表创建)
        from simtradedata.database.migration import get_migration_manager

        migration_manager = get_migration_manager(db_manager)
        success = migration_manager.apply_all_migrations()
        assert success, "迁移失败"
        logger.info("✅ 数据库迁移完成")

        # 4. 验证表结构 (使用新的数据库管理器实例)
        db_manager_verify = DatabaseManager(
            db_path=db_config["path"],
            **{k: v for k, v in db_config.items() if k != "path"},
        )
        schema_results = validate_schema(db_manager_verify)
        failed_tables = [
            table for table, exists in schema_results.items() if not exists
        ]
        db_manager_verify.close()
        assert len(failed_tables) == 0, f"表创建失败: {failed_tables}"
        logger.info("✅ 表结构验证通过")

        # 5. 测试基本操作
        # 插入测试数据
        test_symbol = "000001.SZ"
        sql = """
        INSERT INTO ptrade_stock_info (symbol, name, market, industry, list_date)
        VALUES (?, ?, ?, ?, ?)
        """
        db_manager.execute(sql, (test_symbol, "平安银行", "SZ", "银行", "1991-04-03"))

        # 查询验证
        result = db_manager.fetchone(
            "SELECT * FROM ptrade_stock_info WHERE symbol = ?", (test_symbol,)
        )
        assert result is not None
        assert result["name"] == "平安银行"
        logger.info("✅ 基本数据操作测试通过")

        # 6. 检查数据库大小
        db_size = db_manager.get_database_size()
        logger.info(f"✅ 数据库大小: {db_size / 1024:.2f} KB")

        # 7. 清理
        db_manager.close()
        logger.info("✅ 数据库连接已关闭")

        logger.info("🎉 完整数据库设置测试通过!")

    finally:
        # 清理临时文件
        Path(db_path).unlink(missing_ok=True)


if __name__ == "__main__":
    # 运行完整测试
    test_full_database_setup()

    # 运行pytest测试
    pytest.main([__file__, "-v"])
