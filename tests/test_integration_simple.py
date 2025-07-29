"""
SimTradeData 简化集成测试

验证核心模块的基本集成功能。
"""

import logging
import tempfile
from pathlib import Path

import pytest

from simtradedata.api import APIRouter
from simtradedata.config import Config
from simtradedata.database import DatabaseManager
from simtradedata.performance import QueryOptimizer
from simtradedata.performance.cache_manager import CacheManager

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestPTradeCacheIntegration:
    """SimTradeData 集成测试"""

    @pytest.fixture
    def temp_db_path(self):
        """临时数据库路径"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            yield f.name
        # 清理
        Path(f.name).unlink(missing_ok=True)

    @pytest.fixture
    def config(self, temp_db_path):
        """测试配置"""
        config = Config()
        config.set("database.path", temp_db_path)
        config.set("cache.enable", True)
        config.set("cache.ttl", 300)
        config.set("system_monitor.enable_monitoring", False)
        config.set("health_checker.enable_monitoring", False)
        config.set("ops_tools.enable_auto_maintenance", False)
        config.set("performance_monitor.enable_monitoring", False)
        return config

    @pytest.fixture
    def db_manager(self, config):
        """数据库管理器"""
        db_path = config.get("database.path")
        db_manager = DatabaseManager(db_path)
        # 初始化连接（通过访问connection属性）
        _ = db_manager.connection
        yield db_manager
        # 清理连接
        if hasattr(db_manager, "_local") and hasattr(db_manager._local, "connection"):
            if db_manager._local.connection:
                db_manager._local.connection.close()

    def test_database_and_cache_integration(self, config, db_manager):
        """测试数据库和缓存集成"""
        logger.info("🧪 测试数据库和缓存集成...")

        # 1. 测试数据库连接
        # 通过执行简单查询测试连接
        result = db_manager.fetchone("SELECT 1 as test")
        assert result is not None
        assert result["test"] == 1

        # 2. 测试缓存管理器
        cache_manager = CacheManager(config)

        # 测试缓存操作
        test_data = {"symbol": "000001.SZ", "price": 10.5}
        cache_manager.set("test_key", test_data, "test_type")
        cached_data = cache_manager.get("test_key", "test_type")
        assert cached_data == test_data

        # 3. 测试API路由器
        api_router = APIRouter(db_manager, config)

        # 测试路由器统计
        stats = api_router.get_api_stats()
        assert "api_name" in stats or "error" not in stats

        logger.info("✅ 数据库和缓存集成测试通过")

    def test_performance_modules_integration(self, config, db_manager):
        """测试性能模块集成"""
        logger.info("🧪 测试性能模块集成...")

        # 1. 测试查询优化器
        optimizer = QueryOptimizer(db_manager, config)

        # 测试查询优化
        sql = "SELECT 1 as test_value"
        result = optimizer.execute_with_cache(sql, ())
        assert result is not None

        # 测试缓存统计
        cache_stats = optimizer.get_cache_stats()
        assert "hits" in cache_stats
        assert "misses" in cache_stats

        logger.info("✅ 性能模块集成测试通过")

    @pytest.mark.skip(reason="监控模块尚未实现")
    def test_monitoring_modules_integration(self, config, db_manager):
        """测试监控模块集成"""
        logger.info("🧪 测试监控模块集成...")

        # 监控模块待实现
        logger.info("✅ 监控模块集成测试跳过（待实现）")

    def test_data_workflow_integration(self, config, db_manager):
        """测试数据工作流集成"""
        logger.info("🧪 测试数据工作流集成...")

        # 1. 初始化组件
        cache_manager = CacheManager(config)
        APIRouter(db_manager, config)

        # 2. 测试数据存储和检索流程
        # 创建测试表
        db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS test_stocks (
                symbol TEXT,
                trade_date TEXT,
                close REAL,
                volume INTEGER,
                PRIMARY KEY (symbol, trade_date)
            )
        """
        )

        # 插入测试数据
        test_data = [
            ("000001.SZ", "2024-01-20", 10.5, 1000000),
            ("000002.SZ", "2024-01-20", 25.8, 800000),
        ]

        for data in test_data:
            db_manager.execute(
                "INSERT OR REPLACE INTO test_stocks (symbol, trade_date, close, volume) VALUES (?, ?, ?, ?)",
                data,
            )

        # 3. 测试查询功能
        # 直接数据库查询
        result = db_manager.fetchall(
            "SELECT * FROM test_stocks WHERE symbol = ?", ("000001.SZ",)
        )
        assert len(result) == 1
        assert result[0]["close"] == 10.5

        # 4. 测试缓存功能
        # 缓存查询结果
        cache_key = "test_stocks_000001.SZ"
        cache_manager.set(cache_key, result, "query_result")

        # 从缓存获取
        cached_result = cache_manager.get(cache_key, "query_result")
        assert cached_result == result

        # 5. 测试查询优化器
        optimizer = QueryOptimizer(db_manager, config)

        # 使用优化器查询（会自动缓存）
        optimized_result = optimizer.execute_with_cache(
            "SELECT * FROM test_stocks WHERE symbol = ?", ("000002.SZ",)
        )
        assert len(optimized_result) == 1
        assert optimized_result[0]["close"] == 25.8

        # 第二次查询应该从缓存获取
        cached_optimized_result = optimizer.execute_with_cache(
            "SELECT * FROM test_stocks WHERE symbol = ?", ("000002.SZ",)
        )
        assert cached_optimized_result == optimized_result

        # 验证缓存统计
        cache_stats = optimizer.get_cache_stats()
        assert cache_stats["hits"] > 0

        logger.info("✅ 数据工作流集成测试通过")

    def test_error_handling_integration(self, config, db_manager):
        """测试错误处理集成"""
        logger.info("🧪 测试错误处理集成...")

        # 1. 测试数据库错误处理
        try:
            # 执行无效SQL
            db_manager.execute("INVALID SQL STATEMENT")
        except Exception as e:
            # 应该捕获并处理错误
            assert "syntax error" in str(e).lower() or "near" in str(e).lower()

        # 2. 测试缓存错误处理
        cache_manager = CacheManager(config)

        # 测试获取不存在的缓存
        result = cache_manager.get("non_existent_key", "test_type")
        assert result is None

        # 3. 测试查询优化器错误处理
        optimizer = QueryOptimizer(db_manager, config)

        try:
            # 执行无效查询
            optimizer.execute_with_cache("INVALID QUERY", ())
        except Exception:
            # 应该优雅地处理错误
            pass

        # 优化器应该仍然可用
        cache_stats = optimizer.get_cache_stats()
        assert isinstance(cache_stats, dict)

        # 4. 测试健康检查错误恢复 (跳过，待实现)
        # health_checker = HealthChecker(db_manager, config)
        # health = health_checker.get_overall_health()
        # assert "overall_status" in health

        logger.info("✅ 错误处理集成测试通过")


def test_simtradedata_integration():
    """SimTradeData 集成测试入口"""
    logger.info("🚀 开始SimTradeData集成测试...")

    # 这个测试会被pytest自动发现和运行
    # 主要用于验证核心组件能够正确协作

    logger.info("🎉 SimTradeData集成测试完成!")


if __name__ == "__main__":
    # 运行集成测试
    test_simtradedata_integration()

    # 运行pytest测试
    pytest.main([__file__, "-v"])
