# SimTradeData - 高效量化交易数据下载工具

> **BaoStock + Mootdx 双数据源** | **PTrade格式兼容** | **DuckDB + Parquet存储**

**SimTradeData** 是为 [SimTradeLab](https://github.com/kay-ou/SimTradeLab) 设计的高效数据下载工具。支持 BaoStock 和 Mootdx（通达信）双数据源，采用 DuckDB 作为中间存储，导出为 Parquet 格式，支持高效的增量更新和数据查询。

---

<div align="center">

### 推荐组合：SimTradeData + SimTradeLab

**完全兼容PTrade | 回测速度提升20倍以上**

[![SimTradeLab](https://img.shields.io/badge/SimTradeLab-量化回测框架-blue?style=for-the-badge)](https://github.com/kay-ou/SimTradeLab)

**无需修改PTrade策略代码** | **极速本地回测** | **零成本解决方案**

</div>

---

## 核心特性

### 高效存储架构
- **DuckDB 中间存储**: 高性能列式数据库，支持 SQL 查询和增量更新
- **Parquet 导出格式**: 压缩高效，跨平台兼容，适合大规模数据分析
- **自动增量更新**: 智能识别已下载数据，仅更新增量部分

### 数据完整性
- **市场数据**: OHLCV 日线数据，含涨跌停价、前收盘价
- **估值指标**: PE/PB/PS/PCF/换手率/总股本/流通股
- **财务数据**: 23个季度财务指标 + TTM指标自动计算
- **除权除息**: 分红、送股、配股数据
- **复权因子**: 前复权/后复权因子
- **元数据**: 股票信息、交易日历、指数成分股、ST/停牌状态

### 数据质量保障
- **自动验证**: 写入前自动验证数据完整性
- **导出时计算**: 涨跌停价、TTM指标等在导出时计算，确保数据一致性
- **详细日志**: 完整的错误日志和警告信息

## 生成的数据结构

```
data/
├── simtradedata.duckdb          # DuckDB 数据库（下载时使用）
└── parquet/                     # 导出的 Parquet 文件
    ├── stocks/                  # 股票日线行情（每股票一个文件）
    │   ├── 000001.SZ.parquet
    │   └── 600000.SS.parquet
    ├── exrights/                # 除权除息事件
    ├── fundamentals/            # 季度财务数据（含TTM）
    ├── valuation/               # 估值指标（日频）
    ├── metadata/                # 元数据
    │   ├── stock_metadata.parquet
    │   ├── benchmark.parquet
    │   ├── trade_days.parquet
    │   ├── index_constituents.parquet
    │   ├── stock_status.parquet
    │   └── version.parquet
    ├── ptrade_adj_pre.parquet   # 前复权因子
    ├── ptrade_adj_post.parquet  # 后复权因子
    └── manifest.json            # 数据包清单
```

## 快速开始

### 方式一：直接下载现成数据（推荐）

已导出的 Parquet 数据包（2025年数据），可直接用于 SimTradeLab 回测：

> 夸克网盘分享：[simtradelab_data_2025_parquet.zip](https://pan.quark.cn/s/52c14827a6c5)
>
> 提取码：5DdN

```bash
# 解压到 SimTradeLab 数据目录
unzip simtradelab_data_2025_parquet.zip -d /path/to/SimTradeLab/data/
```

### 方式二：自行下载数据

#### 1. 安装依赖

```bash
# 克隆项目
git clone https://github.com/kay-ou/SimTradeData.git
cd SimTradeData

# 安装依赖
poetry install

# 激活虚拟环境
poetry shell
```

#### 2. 下载数据

支持两种数据源，任选其一：

**数据源 A：BaoStock（免费，无需安装客户端）**

```bash
# 首次下载：下载全部数据（2017至今）
poetry run python scripts/download_efficient.py

# 跳过财务数据（更快）
poetry run python scripts/download_efficient.py --skip-fundamentals

# 指定起始日期
poetry run python scripts/download_efficient.py --start-date 2020-01-01
```

**数据源 B：Mootdx（通达信API，速度更快）**

```bash
# 首次下载：下载全部数据（2015至今）
poetry run python scripts/download_mootdx.py

# 跳过财务数据（更快）
poetry run python scripts/download_mootdx.py --skip-fundamentals

# 指定起始日期
poetry run python scripts/download_mootdx.py --start-date 2020-01-01

# 指定财务数据ZIP下载目录
poetry run python scripts/download_mootdx.py --download-dir /tmp/tdx_data
```

**数据源 C：TDX官方数据包（推荐，最快）**

自动下载通达信官方沪深京日线完整包（~500MB），包含完整历史数据：

```bash
# 自动下载并导入（增量更新）
poetry run python scripts/download_tdx_day.py

# 强制重新下载
poetry run python scripts/download_tdx_day.py --force-download

# 完全重新导入
poetry run python scripts/download_tdx_day.py --full

# 仅下载不导入
poetry run python scripts/download_tdx_day.py --download-only

# 使用已下载的文件
poetry run python scripts/download_tdx_day.py --file hsjday.zip
```

#### 3. 导出为 Parquet

```bash
# 导出为 PTrade 兼容的 Parquet 格式
poetry run python scripts/export_parquet.py

# 指定输出目录
poetry run python scripts/export_parquet.py --output data/parquet
```

#### 4. 在 SimTradeLab 中使用

```bash
# 复制 Parquet 文件到 SimTradeLab 数据目录
cp -r data/parquet/* /path/to/SimTradeLab/data/
```

## 项目架构

```
SimTradeData/
├── scripts/
│   ├── download_efficient.py    # BaoStock 下载脚本
│   ├── download_mootdx.py       # Mootdx（通达信API）下载脚本
│   ├── download_tdx_day.py      # TDX 官方日线数据包下载导入脚本
│   ├── import_tdx_day.py        # TDX .day 文件导入脚本
│   └── export_parquet.py        # Parquet 导出脚本
├── simtradedata/
│   ├── fetchers/
│   │   ├── base_fetcher.py      # 基础 Fetcher 类
│   │   ├── baostock_fetcher.py  # BaoStock 数据获取
│   │   ├── unified_fetcher.py   # BaoStock 统一数据获取（优化版）
│   │   ├── mootdx_fetcher.py    # Mootdx 基础数据获取
│   │   ├── mootdx_unified_fetcher.py  # Mootdx 统一数据获取
│   │   └── mootdx_affair_fetcher.py   # Mootdx 财务数据获取
│   ├── processors/
│   │   └── data_splitter.py     # 数据分流处理
│   ├── writers/
│   │   └── duckdb_writer.py     # DuckDB 写入和导出
│   ├── validators/
│   │   └── data_validator.py    # 数据质量验证
│   ├── config/
│   │   ├── field_mappings.py    # 字段映射配置
│   │   └── mootdx_finvalue_map.py  # Mootdx 财务字段映射
│   └── utils/
│       ├── code_utils.py        # 股票代码转换
│       └── ttm_calculator.py    # 季度范围计算
├── data/                        # 数据目录
└── docs/                        # 文档
    ├── PTRADE_PARQUET_FORMAT.md # Parquet 格式规范
    └── PTrade_API_mini_Reference.md
```

### 核心模块

**1. UnifiedDataFetcher** - 统一数据获取
- 一次 API 调用获取行情、估值、状态数据
- 减少 API 调用次数 33%

**2. DuckDBWriter** - 数据存储和导出
- 高效的增量写入（upsert）
- 导出时计算涨跌停价、TTM指标
- Forward fill 季度数据到日频

**3. DataSplitter** - 数据分流
- 将统一数据按类型分流到不同表

## 数据字段说明

### stocks/ - 股票日线
| 字段 | 说明 |
|------|------|
| date | 交易日期 |
| open/high/low/close | OHLC价格 |
| high_limit/low_limit | 涨跌停价（导出时计算） |
| preclose | 前收盘价 |
| volume | 成交量（股） |
| money | 成交金额（元） |

### valuation/ - 估值指标（日频）
| 字段 | 说明 |
|------|------|
| pe_ttm/pb/ps_ttm/pcf | 估值比率 |
| roe/roe_ttm/roa/roa_ttm | 盈利指标（季报forward fill） |
| naps | 每股净资产（导出时计算） |
| total_shares/a_floats | 总股本/流通股 |
| turnover_rate | 换手率 |

### fundamentals/ - 财务数据（季频）
包含23个财务指标及其TTM版本，详见 [PTRADE_PARQUET_FORMAT.md](docs/PTRADE_PARQUET_FORMAT.md)

## 配置说明

编辑 `scripts/download_efficient.py`:

```python
# 日期范围
START_DATE = "2017-01-01"
END_DATE = None  # None = 当前日期

# 输出目录
OUTPUT_DIR = "data"

# 批次大小
BATCH_SIZE = 20
```

## 文档

| 文档 | 说明 |
|------|------|
| [PTRADE_PARQUET_FORMAT.md](docs/PTRADE_PARQUET_FORMAT.md) | Parquet 数据格式规范 |
| [PTrade_API_mini_Reference.md](docs/PTrade_API_mini_Reference.md) | PTrade API 参考 |

## 注意事项

### 数据源对比

| 特性 | BaoStock | Mootdx API | TDX 官方数据包 |
|------|----------|------------|---------------|
| 速度 | 较慢 | 快 | 最快（一次性下载） |
| 估值数据 | 有 (PE/PB/PS等) | 无 | 无 |
| 财务数据 | 有 | 有（批量ZIP） | 无 |
| 历史起始 | 2017年 | 2015年 | 完整历史 |
| 并发支持 | 不支持 | 支持 | N/A |
| 数据包大小 | - | - | ~500MB |

### BaoStock 限制
- 不支持并发下载
- 建议控制请求频率

### Mootdx 限制
- 不包含估值指标数据
- 财务数据通过下载ZIP文件获取

### TDX 官方数据包
- 仅包含日线行情数据（OHLCV）
- 不包含估值、财务数据
- 适合快速获取完整历史行情

### 数据质量
- 数据来自 BaoStock 免费数据源
- 仅供学习研究使用

## 版本历史

### v0.4.0 (2026-01-30) - DuckDB + Parquet 架构
- 存储格式从 HDF5 迁移到 DuckDB + Parquet
- 添加涨跌停价计算（导出时基于 preclose）
- 添加 TTM 指标计算（导出时用 SQL window function）
- 添加除权除息数据下载
- 添加股本数据（total_shares/a_floats）
- 优化增量更新逻辑
- 清理废弃代码和文档

### v0.3.0 (2025-11-24) - 质量与架构优化版
- 实现市值字段计算
- 修复 TTM 指标计算
- 添加数据验证器
- 提取 BaseFetcher 基类

### v0.2.0 (2025-11-22) - 性能优化版
- 实现统一数据获取，API 调用减少 33%
- 优化 HDF5 写入逻辑

### v0.1.0 (2024-11-14) - 初始版本
- 基础数据下载功能
- BaoStock 数据源集成

## 相关链接

- **SimTradeLab**: https://github.com/kay-ou/SimTradeLab
- **BaoStock**: http://baostock.com/
- **Mootdx**: https://github.com/mootdx/mootdx

## 许可证

本项目采用 AGPL-3.0 许可证。详见 [LICENSE](LICENSE) 文件。

---

**项目状态**: 生产就绪 | **当前版本**: v0.4.0 | **最后更新**: 2026-01-30
