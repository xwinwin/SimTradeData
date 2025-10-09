"""
性能基准测试 - Full Sync

使用 pytest-benchmark 对批量同步、大规模同步和内存使用进行基准测试。
运行方式：pytest tests/sync/test_full_sync_benchmark.py --benchmark-only -v

性能目标：
1. 同步速度 >500 条/秒
2. 批量模式比逐个模式快 >3 倍
3. 大规模同步（5000+ 股票）能稳定完成
"""

import gc
import tracemalloc
from datetime import date
from typing import Dict, List
from unittest.mock import patch

import pytest

from simtradedata.sync.manager import SyncManager
from tests.conftest import BaseTestClass


@pytest.mark.performance
@pytest.mark.benchmark
class TestSyncPerformanceBenchmark(BaseTestClass):
    """同步性能基准测试"""

    def _create_mock_financial_data(self, symbol: str) -> Dict:
        """创建模拟财务数据"""
        return {
            "revenue": 1000000.0 + hash(symbol) % 1000000,
            "net_profit": 50000.0 + hash(symbol) % 50000,
            "total_assets": 5000000.0 + hash(symbol) % 5000000,
            "total_liabilities": 2000000.0,
            "shareholders_equity": 3000000.0,
            "operating_cash_flow": 100000.0,
            "eps": 1.5,
            "roe": 7.5,
        }

    def _create_mock_valuation_data(self, symbol: str) -> List[Dict]:
        """创建模拟估值数据"""
        return [
            {
                "date": "2025-01-24",
                "pe_ratio": 15.0 + hash(symbol) % 10,
                "pb_ratio": 2.0 + (hash(symbol) % 5) * 0.1,
                "ps_ratio": 3.0,
                "pcf_ratio": 10.0,
            }
        ]

    def _setup_test_environment(self, db_manager, symbol_count: int) -> tuple:
        """设置测试环境"""
        # 生成测试股票代码
        if symbol_count <= 1000:
            test_symbols = [f"60{1000 + i:04d}.SS" for i in range(symbol_count)]
        else:
            test_symbols = []
            for i in range(symbol_count):
                if i % 2 == 0:
                    test_symbols.append(f"60{(i // 2) % 10000:04d}.SS")
                else:
                    test_symbols.append(f"00{(i // 2) % 10000:04d}.SZ")

        # 插入股票基础信息
        for symbol in test_symbols:
            db_manager.execute(
                """
                INSERT OR IGNORE INTO stocks (symbol, name, market, status)
                VALUES (?, ?, ?, ?)
                """,
                (symbol, f"测试股票{symbol}", symbol.split(".")[1], "active"),
            )

        # 清理历史数据
        for symbol in test_symbols:
            db_manager.execute(
                "DELETE FROM extended_sync_status WHERE symbol = ?", (symbol,)
            )
            db_manager.execute("DELETE FROM financials WHERE symbol = ?", (symbol,))
            db_manager.execute("DELETE FROM valuations WHERE symbol = ?", (symbol,))

        target_date = date(2025, 1, 24)
        return test_symbols, target_date

    def test_benchmark_individual_mode_100_stocks(
        self, benchmark, db_manager, data_source_manager, processing_engine, config
    ):
        """基准测试：逐个模式同步 100 只股票"""
        test_symbols, target_date = self._setup_test_environment(db_manager, 100)

        def fundamentals_mock(symbol, *args, **kwargs):
            return {
                "success": True,
                "data": self._create_mock_financial_data(symbol),
            }

        def valuation_mock(symbol, *args, **kwargs):
            return {
                "success": True,
                "data": self._create_mock_valuation_data(symbol),
            }

        sync_manager = SyncManager(
            db_manager, data_source_manager, processing_engine, config
        )

        # 强制使用逐个模式
        with (
            patch.object(
                data_source_manager,
                "batch_import_financial_data",
                return_value={"success": False},
            ),
            patch.object(
                data_source_manager, "get_fundamentals", side_effect=fundamentals_mock
            ),
            patch.object(
                data_source_manager, "get_valuation_data", side_effect=valuation_mock
            ),
        ):

            def sync_individual():
                return sync_manager._sync_extended_data(test_symbols, target_date, None)

            result = benchmark(sync_individual)

            assert result["processed_symbols"] == 100
            assert result["batch_mode"] is False

            if benchmark.stats["mean"] > 0:
                throughput = 100 / benchmark.stats["mean"]
                print(f"\n逐个模式同步速度: {throughput:.2f} 条/秒")

        # 清理
        for symbol in test_symbols:
            db_manager.execute(
                "DELETE FROM extended_sync_status WHERE symbol = ?", (symbol,)
            )
            db_manager.execute("DELETE FROM financials WHERE symbol = ?", (symbol,))
            db_manager.execute("DELETE FROM valuations WHERE symbol = ?", (symbol,))
            db_manager.execute("DELETE FROM stocks WHERE symbol = ?", (symbol,))

    def test_benchmark_batch_mode_100_stocks(
        self, benchmark, db_manager, data_source_manager, processing_engine, config
    ):
        """基准测试：批量模式同步 100 只股票"""
        test_symbols, target_date = self._setup_test_environment(db_manager, 100)

        # 插入足够股票触发批量模式 (减少到100只加速测试)
        for i in range(100):
            symbol = f"99{i:04d}.SZ"
            db_manager.execute(
                """
                INSERT OR IGNORE INTO stocks (symbol, name, market, status)
                VALUES (?, ?, ?, ?)
                """,
                (symbol, f"填充股票{i}", "SZ", "active"),
            )

        def batch_mock(year, report_type):
            batch_data = []
            for symbol in test_symbols:
                batch_data.append(
                    {
                        "symbol": symbol,
                        "report_date": f"{year}-12-31",
                        "report_type": report_type,
                        "data": self._create_mock_financial_data(symbol),
                    }
                )
            return {"success": True, "data": {"data": batch_data}}

        def valuation_mock(symbol, *args, **kwargs):
            return {
                "success": True,
                "data": self._create_mock_valuation_data(symbol),
            }

        sync_manager = SyncManager(
            db_manager, data_source_manager, processing_engine, config
        )

        with (
            patch.object(
                data_source_manager,
                "batch_import_financial_data",
                side_effect=batch_mock,
            ),
            patch.object(
                data_source_manager, "get_valuation_data", side_effect=valuation_mock
            ),
        ):

            def sync_batch():
                return sync_manager._sync_extended_data(test_symbols, target_date, None)

            result = benchmark(sync_batch)

            assert result["processed_symbols"] == 100
            assert result["batch_mode"] is True

            if benchmark.stats["mean"] > 0:
                throughput = 100 / benchmark.stats["mean"]
                print(f"\n批量模式同步速度: {throughput:.2f} 条/秒")

        # 清理
        for symbol in test_symbols:
            db_manager.execute(
                "DELETE FROM extended_sync_status WHERE symbol = ?", (symbol,)
            )
            db_manager.execute("DELETE FROM financials WHERE symbol = ?", (symbol,))
            db_manager.execute("DELETE FROM valuations WHERE symbol = ?", (symbol,))
            db_manager.execute("DELETE FROM stocks WHERE symbol = ?", (symbol,))

        for i in range(100):  # 减少清理范围
            symbol = f"99{i:04d}.SZ"
            db_manager.execute("DELETE FROM stocks WHERE symbol = ?", (symbol,))

    @pytest.mark.slow
    def test_benchmark_batch_mode_500_stocks(
        self, benchmark, db_manager, data_source_manager, processing_engine, config
    ):
        """基准测试：批量模式同步 500 只股票（慢速测试）"""
        test_symbols, target_date = self._setup_test_environment(db_manager, 500)

        # 插入足够股票触发批量模式
        for i in range(100):
            symbol = f"99{i:04d}.SZ"
            db_manager.execute(
                """
                INSERT OR IGNORE INTO stocks (symbol, name, market, status)
                VALUES (?, ?, ?, ?)
                """,
                (symbol, f"填充股票{i}", "SZ", "active"),
            )

        def batch_mock(year, report_type):
            batch_data = []
            for symbol in test_symbols:
                batch_data.append(
                    {
                        "symbol": symbol,
                        "report_date": f"{year}-12-31",
                        "report_type": report_type,
                        "data": self._create_mock_financial_data(symbol),
                    }
                )
            return {"success": True, "data": {"data": batch_data}}

        def valuation_mock(symbol, *args, **kwargs):
            return {
                "success": True,
                "data": self._create_mock_valuation_data(symbol),
            }

        sync_manager = SyncManager(
            db_manager, data_source_manager, processing_engine, config
        )

        with (
            patch.object(
                data_source_manager,
                "batch_import_financial_data",
                side_effect=batch_mock,
            ),
            patch.object(
                data_source_manager, "get_valuation_data", side_effect=valuation_mock
            ),
        ):

            def sync_batch():
                return sync_manager._sync_extended_data(test_symbols, target_date, None)

            result = benchmark(sync_batch)

            assert result["processed_symbols"] == 500
            assert result["batch_mode"] is True

            if benchmark.stats["mean"] > 0:
                throughput = 500 / benchmark.stats["mean"]
                print(f"\n批量模式同步 500 只股票速度: {throughput:.2f} 条/秒")

        # 清理
        for symbol in test_symbols:
            db_manager.execute(
                "DELETE FROM extended_sync_status WHERE symbol = ?", (symbol,)
            )
            db_manager.execute("DELETE FROM financials WHERE symbol = ?", (symbol,))
            db_manager.execute("DELETE FROM valuations WHERE symbol = ?", (symbol,))
            db_manager.execute("DELETE FROM stocks WHERE symbol = ?", (symbol,))

        for i in range(100):
            symbol = f"99{i:04d}.SZ"
            db_manager.execute("DELETE FROM stocks WHERE symbol = ?", (symbol,))

    @pytest.mark.slow
    def test_benchmark_batch_mode_1000_stocks(
        self, benchmark, db_manager, data_source_manager, processing_engine, config
    ):
        """基准测试：批量模式同步 1000 只股票（慢速测试）"""
        test_symbols, target_date = self._setup_test_environment(db_manager, 1000)

        def batch_mock(year, report_type):
            batch_data = []
            for symbol in test_symbols:
                batch_data.append(
                    {
                        "symbol": symbol,
                        "report_date": f"{year}-12-31",
                        "report_type": report_type,
                        "data": self._create_mock_financial_data(symbol),
                    }
                )
            return {"success": True, "data": {"data": batch_data}}

        def valuation_mock(symbol, *args, **kwargs):
            return {
                "success": True,
                "data": self._create_mock_valuation_data(symbol),
            }

        sync_manager = SyncManager(
            db_manager, data_source_manager, processing_engine, config
        )

        with (
            patch.object(
                data_source_manager,
                "batch_import_financial_data",
                side_effect=batch_mock,
            ),
            patch.object(
                data_source_manager, "get_valuation_data", side_effect=valuation_mock
            ),
        ):

            def sync_batch():
                return sync_manager._sync_extended_data(test_symbols, target_date, None)

            result = benchmark(sync_batch)

            assert result["processed_symbols"] == 1000
            assert result["batch_mode"] is True

            if benchmark.stats["mean"] > 0:
                throughput = 1000 / benchmark.stats["mean"]
                print(f"\n批量模式同步 1000 只股票速度: {throughput:.2f} 条/秒")

        # 清理
        for symbol in test_symbols:
            db_manager.execute(
                "DELETE FROM extended_sync_status WHERE symbol = ?", (symbol,)
            )
            db_manager.execute("DELETE FROM financials WHERE symbol = ?", (symbol,))
            db_manager.execute("DELETE FROM valuations WHERE symbol = ?", (symbol,))
            db_manager.execute("DELETE FROM stocks WHERE symbol = ?", (symbol,))


@pytest.mark.performance
@pytest.mark.benchmark
@pytest.mark.slow
class TestLargeScaleSyncBenchmark(BaseTestClass):
    """大规模同步性能基准测试（5000+ 股票）"""

    def test_benchmark_large_scale_5000_stocks(
        self, benchmark, db_manager, data_source_manager, processing_engine, config
    ):
        """基准测试：大规模同步 5000 只股票"""
        # 生成大规模测试数据集
        test_symbols = []
        for i in range(5000):
            if i % 2 == 0:
                symbol = f"60{(i // 2) % 10000:04d}.SS"
            else:
                symbol = f"00{(i // 2) % 10000:04d}.SZ"
            test_symbols.append(symbol)

            db_manager.execute(
                """
                INSERT OR IGNORE INTO stocks (symbol, name, market, status)
                VALUES (?, ?, ?, ?)
                """,
                (symbol, f"测试股票{symbol}", symbol.split(".")[1], "active"),
            )

        target_date = date(2025, 1, 24)

        def batch_mock(year, report_type):
            batch_data = []
            for symbol in test_symbols[:1000]:  # 每次批量1000只
                batch_data.append(
                    {
                        "symbol": symbol,
                        "report_date": f"{year}-12-31",
                        "report_type": report_type,
                        "data": {"revenue": 1000000.0, "net_profit": 50000.0},
                    }
                )
            return {"success": True, "data": {"data": batch_data}}

        def valuation_mock(symbol, *args, **kwargs):
            return {
                "success": True,
                "data": [{"date": "2025-01-24", "pe_ratio": 15.0}],
            }

        sync_manager = SyncManager(
            db_manager, data_source_manager, processing_engine, config
        )

        with (
            patch.object(
                data_source_manager,
                "batch_import_financial_data",
                side_effect=batch_mock,
            ),
            patch.object(
                data_source_manager, "get_valuation_data", side_effect=valuation_mock
            ),
        ):

            def sync_large_scale():
                return sync_manager._sync_extended_data(test_symbols, target_date, None)

            result = benchmark.pedantic(sync_large_scale, rounds=1, iterations=1)

            assert result["processed_symbols"] == 5000
            print(f"\n大规模同步完成: 处理了 {result['processed_symbols']} 只股票")
            print(f"批量模式: {result['batch_mode']}")
            print(f"成功: {result['success_count']}, 失败: {result['failed_symbols']}")

            if benchmark.stats["mean"] > 0:
                throughput = 5000 / benchmark.stats["mean"]
                print(f"大规模同步速度: {throughput:.2f} 条/秒")
                print(f"总耗时: {benchmark.stats['mean']:.2f} 秒")

        # 清理
        for symbol in test_symbols:
            db_manager.execute(
                "DELETE FROM extended_sync_status WHERE symbol = ?", (symbol,)
            )
            db_manager.execute("DELETE FROM financials WHERE symbol = ?", (symbol,))
            db_manager.execute("DELETE FROM valuations WHERE symbol = ?", (symbol,))
            db_manager.execute("DELETE FROM stocks WHERE symbol = ?", (symbol,))


@pytest.mark.performance
@pytest.mark.slow
class TestMemoryUsageBenchmark(BaseTestClass):
    """内存使用性能测试（慢速测试）"""

    def test_memory_usage_batch_mode(
        self, db_manager, data_source_manager, processing_engine, config
    ):
        """测试批量模式内存使用"""
        test_symbols = [
            f"60{1000 + i:04d}.SS" for i in range(50)
        ]  # 减少到50只用于快速测试
        target_date = date(2025, 1, 24)

        for symbol in test_symbols:
            db_manager.execute(
                """
                INSERT OR IGNORE INTO stocks (symbol, name, market, status)
                VALUES (?, ?, ?, ?)
                """,
                (symbol, f"测试股票{symbol}", "SS", "active"),
            )
            db_manager.execute(
                "DELETE FROM extended_sync_status WHERE symbol = ?", (symbol,)
            )
            db_manager.execute("DELETE FROM financials WHERE symbol = ?", (symbol,))

        def batch_mock(year, report_type):
            batch_data = []
            for symbol in test_symbols:
                batch_data.append(
                    {
                        "symbol": symbol,
                        "report_date": f"{year}-12-31",
                        "report_type": report_type,
                        "data": {
                            "revenue": 1000000.0,
                            "net_profit": 50000.0,
                            "total_assets": 5000000.0,
                        },
                    }
                )
            return {"success": True, "data": {"data": batch_data}}

        def valuation_mock(symbol, *args, **kwargs):
            return {
                "success": True,
                "data": [{"date": "2025-01-24", "pe_ratio": 15.0}],
            }

        gc.collect()
        tracemalloc.start()

        sync_manager = SyncManager(
            db_manager, data_source_manager, processing_engine, config
        )

        with (
            patch.object(
                data_source_manager,
                "batch_import_financial_data",
                side_effect=batch_mock,
            ),
            patch.object(
                data_source_manager, "get_valuation_data", side_effect=valuation_mock
            ),
        ):
            result = sync_manager._sync_extended_data(test_symbols, target_date, None)

        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        assert result["processed_symbols"] == 50
        assert result["batch_mode"] is True

        print(f"\n批量模式内存使用（50 只股票）:")
        print(f"  当前内存: {current / 1024 / 1024:.2f} MB")
        print(f"  峰值内存: {peak / 1024 / 1024:.2f} MB")
        print(f"  平均每只股票: {peak / len(test_symbols) / 1024:.2f} KB")

        max_memory_mb = 500
        assert (
            peak / 1024 / 1024 < max_memory_mb
        ), f"内存使用应该 <{max_memory_mb}MB，实际: {peak / 1024 / 1024:.2f}MB"

        # 清理
        for symbol in test_symbols:
            db_manager.execute(
                "DELETE FROM extended_sync_status WHERE symbol = ?", (symbol,)
            )
            db_manager.execute("DELETE FROM financials WHERE symbol = ?", (symbol,))
            db_manager.execute("DELETE FROM stocks WHERE symbol = ?", (symbol,))
