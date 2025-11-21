# BaoStock 完整数据下载方案

## 概述

本文档定义完整的数据下载方案，涵盖所有PTrade兼容的数据类型。参考了：
- `/home/kay/dev/ptrade/data/download_unified_hdf5.py` - 日K线数据
- `/home/kay/dev/ptrade/data/download_fundamentals_hdf5.py` - 估值和财务数据
- `/home/kay/dev/ptrade/data/download_minute_hdf5.py` - 分钟级数据

## 数据类型总览

| 数据类型 | 频率 | PTrade文件 | BaoStock API | 优先级 |
|---------|------|-----------|--------------|-------|
| 日K线数据 | 日频 | `ptrade_data.h5` | `query_history_k_data_plus` | 🔴 P0 |
| 除权除息 | 事件 | `ptrade_data.h5` | `query_dividend_data` + `query_adjust_factor` | 🟠 P1 |
| 股票元数据 | 静态 | `ptrade_data.h5` | `query_stock_basic` + `query_stock_industry` | 🟠 P1 |
| 交易日历 | 日频 | `ptrade_data.h5` | `query_trade_dates` | 🟠 P1 |
| 指数成份股 | 季频采样 | `ptrade_data.h5/metadata` | ❌ 暂缺 | 🟡 P2 |
| 股票状态历史 | 季频采样 | `ptrade_data.h5/metadata` | `query_history_k_data_plus` + `query_stock_basic` | 🟠 P1 |
| 估值数据 | 日频 | `ptrade_fundamentals.h5` | `query_history_k_data_plus` | 🟢 P3 |
| 财务数据 | 季频 | `ptrade_fundamentals.h5` | `query_profit_data` 等 | 🟢 P3 |
| 分钟数据 | 分钟 | `minute_*.h5` (分片) | ❌ 不支持 | 🔵 P4 |
| 复权因子 | 日频 | `ptrade_adj_pre.h5` | `query_adjust_factor` | 🟡 P2 |

---

## 一、日K线数据 (`ptrade_data.h5`)

### 数据结构

```
ptrade_data.h5
├── /stock_data/{symbol}      - 日K线（4815只 × 250天 ≈ 120万行）
├── /exrights/{symbol}         - 除权除息
├── /stock_metadata            - 股票基本信息（DataFrame）
├── /benchmark                 - 基准指数（沪深300）
├── /trade_days                - 交易日历
└── /metadata                  - 全局元数据（Series）
    ├── download_date          - 下载时间
    ├── start_date             - 起始日期
    ├── end_date               - 结束日期
    ├── stock_count            - 股票数量
    ├── sample_count           - 采样点数量
    ├── format_version: 3      - 格式版本
    ├── index_constituents     - JSON: {日期: {指数: [股票]}}
    └── stock_status_history   - JSON: {股票: {basic: {...}, daily: {...}}}
```

### 1.1 日K线 (`/stock_data/{symbol}`)

**BaoStock API**: `query_history_k_data_plus`

```python
rs = bs.query_history_k_data_plus(
    code="sh.600000",
    fields="date,open,high,low,close,volume,amount",
    start_date="2024-01-01",
    end_date="2024-12-31",
    frequency="d",
    adjustflag="3"  # 3=不复权
)
df = rs.get_data()
```

**数据处理**:
```python
# 重命名列
df = df.rename(columns={'amount': 'money'})

# 设置索引
df['date'] = pd.to_datetime(df['date'])
df = df.set_index('date')

# 转换数值类型
for col in ['open', 'high', 'low', 'close', 'volume', 'money']:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# 只保留需要的列
df = df[['open', 'high', 'low', 'close', 'volume', 'money']]
```

### 1.2 除权除息 (`/exrights/{symbol}`)

**BaoStock API**: 综合使用两个API

#### 方案A: 分红送股数据

```python
rs = bs.query_dividend_data(
    code="sh.600000",
    year="2024",
    yearType="report"
)
df = rs.get_data()
```

**关键字段**:
- `dividOperateDate`: 除权除息日期（主键）
- `dividCashPsBeforeTax`: 每股股利(税前)
- `dividStocksPs`: 每股送股比例
- `dividReserveToStockPs`: 每股转增资本比例

#### 方案B: 复权因子

```python
rs = bs.query_adjust_factor(
    code="sh.600000",
    start_date="2024-01-01",
    end_date="2024-12-31"
)
df = rs.get_data()
```

**关键字段**:
- `date`: 日期（重命名为 `dividOperateDate`）
- `foreAdjustFactor`: 前复权因子
- `backAdjustFactor`: 后复权因子

**推荐**: 优先使用方案B（复权因子），更简洁直接。

### 1.3 股票元数据 (`/stock_metadata`)

**BaoStock API**: `query_stock_basic` + `query_stock_industry`

```python
# 基本信息
rs = bs.query_stock_basic(code="sh.600000")
basic_df = rs.get_data()

# 行业分类
rs = bs.query_stock_industry(code="sh.600000", date="2024-01-01")
industry_df = rs.get_data()
```

**字段映射**:

| PTrade字段 | BaoStock来源 | 获取方式 |
|-----------|------------|---------|
| stock_code | `code` | `query_stock_basic()` |
| stock_name | `code_name` | `query_stock_basic()` |
| listed_date | `ipoDate` | `query_stock_basic()` |
| de_listed_date | `outDate` | `query_stock_basic()` |
| blocks | `industry` + `industryClassification` | `query_stock_industry()` |
| has_info | - | 衍生字段 |

**数据处理**:
```python
metadata = {
    'stock_code': basic_df['code'].values[0],
    'stock_name': basic_df['code_name'].values[0],
    'listed_date': basic_df['ipoDate'].values[0],
    'de_listed_date': basic_df['outDate'].values[0] if basic_df['outDate'].values[0] else None,
    'blocks': json.dumps({
        'industry': industry_df['industry'].values[0],
        'industryClassification': industry_df['industryClassification'].values[0]
    }, ensure_ascii=False),
    'has_info': True
}
```

### 1.4 基准指数 (`/benchmark`)

**BaoStock API**: `query_history_k_data_plus`

```python
rs = bs.query_history_k_data_plus(
    code="sh.000300",  # 沪深300
    fields="date,open,high,low,close,volume,amount",
    start_date="2024-01-01",
    end_date="2024-12-31",
    frequency="d",
    adjustflag="3"
)
df = rs.get_data()
```

**代码转换**: `000300.SS` (PTrade) → `sh.000300` (BaoStock)

### 1.5 交易日历 (`/trade_days`)

**BaoStock API**: `query_trade_dates`

```python
rs = bs.query_trade_dates(start_date="2024-01-01", end_date="2024-12-31")
df = rs.get_data()

# 筛选交易日
df = df[df['is_trading_day'] == '1']
df['trade_date'] = pd.to_datetime(df['calendar_date'])
df = df[['trade_date']].set_index('trade_date')
```

### 1.6 指数成份股 (`/metadata['index_constituents']`)

**问题**: BaoStock 没有直接API

**解决方案**:
1. **方案A**: 使用 Mootdx (如果支持)
2. **方案B**: 使用 TuShare 或其他数据源
3. **方案C**: 暂时留空，后续补充

**数据结构** (JSON字符串):
```python
{
    "20240101": {
        "000300.SS": ["000001.SZ", "000002.SZ", ...],
        "000905.SS": ["000001.SZ", "000002.SZ", ...],
    },
    "20240401": {...}
}
```

### 1.7 股票状态历史 (`/metadata['stock_status_history']`)

**BaoStock API**: `query_history_k_data_plus` + `query_stock_basic`

#### 数据来源

**每日动态数据**:
```python
rs = bs.query_history_k_data_plus(
    code="sh.600000",
    fields="date,isST,tradestatus",
    start_date="2024-01-01",
    end_date="2024-12-31",
    frequency="d",
    adjustflag="3"
)
```

- `isST`: ST状态 (1=是, 0=否)
- `tradestatus`: 交易状态 (1=正常, 0=停牌)

**基本静态数据**:
```python
rs = bs.query_stock_basic(code="sh.600000")
```

- `status`: 上市状态 (1=上市, 0=退市)
- `ipoDate`: 上市日期
- `outDate`: 退市日期

#### 数据结构 (JSON字符串)

```python
{
    "000001.SZ": {
        "basic": {
            "status": "1",
            "ipo_date": "1991-04-03",
            "out_date": ""
        },
        "daily": {
            "20240101": {"is_st": "0", "trade_status": "1"},
            "20240102": {"is_st": "0", "trade_status": "0"},  # 停牌
            "20240103": {"is_st": "1", "trade_status": "1"},  # ST
        }
    }
}
```

#### PTrade API 映射

| PTrade query_type | 数据来源 | 判断逻辑 |
|------------------|---------|---------|
| `'ST'` | `daily[date]['is_st']` | `is_st == "1"` |
| `'HALT'` | `daily[date]['trade_status']` | `trade_status == "0"` |
| `'DELISTING'` | `basic['status']` + `basic['out_date']` | `status == "0"` 或 `date > out_date` |

#### 采样策略（季度采样）

```python
sample_dates = pd.date_range(start=start_date, end=end_date, freq='Q')

# 确保包含起始和结束日期
sample_dates_set = set(d.date() for d in sample_dates)
if start_date not in sample_dates_set:
    sample_dates.insert(0, start_date)
if end_date not in sample_dates_set:
    sample_dates.append(end_date)
```

---

## 二、估值与财务数据 (`ptrade_fundamentals.h5`)

参考 `download_fundamentals_hdf5.py`

### 数据结构

```
ptrade_fundamentals.h5
├── /valuation/{symbol}        - 日频估值数据（4815只 × 250天）
└── /fundamentals/{symbol}     - 季频财务指标（4815只 × 32季度）
```

### 2.1 估值数据 (`/valuation/{symbol}`)

**PTrade API**: `get_fundamentals(stocks, 'valuation', fields=[...], date)`

**BaoStock API**: `query_history_k_data_plus`

```python
rs = bs.query_history_k_data_plus(
    code="sh.600000",
    fields="date,peTTM,pbMRQ,psTTM,pcfNcfTTM,turn",
    start_date="2024-01-01",
    end_date="2024-12-31",
    frequency="d",
    adjustflag="3"
)
df = rs.get_data()
```

**字段映射**:

| PTrade字段 | BaoStock字段 | 说明 |
|-----------|-------------|------|
| pe_ttm | peTTM | 滚动市盈率 |
| pb | pbMRQ | 市净率 |
| ps_ttm | psTTM | 市销率TTM |
| pcf | pcfNcfTTM | 市现率 |
| turnover_rate | turn | 换手率 |

**注意**: BaoStock 缺少以下字段，需要计算或忽略：
- `total_value`: 总市值 = `close * total_shares`
- `float_value`: 流通市值
- `total_shares`: 总股本

**解决方案**:
- 总市值 = 收盘价 × 总股本（从 `query_stock_basic` 获取）
- 流通市值需要从其他数据源获取

### 2.2 财务数据 (`/fundamentals/{symbol}`)

**PTrade API**: `get_fundamentals(stocks, table, fields=[...], start_year, end_year)`

**BaoStock API**: 4个财务表

| PTrade表名 | BaoStock API | 说明 |
|----------|--------------|------|
| `profit_ability` | `query_profit_data` | 盈利能力 |
| `growth_ability` | `query_growth_data` | 成长能力 |
| `operating_ability` | `query_operation_data` | 营运能力 |
| `debt_paying_ability` | `query_balance_data` | 偿债能力 |

#### 盈利能力 (`profit_ability`)

```python
rs = bs.query_profit_data(
    code="sh.600000",
    year=2024,
    quarter=1
)
df = rs.get_data()
```

**字段映射**:

| PTrade字段 | BaoStock字段 | 说明 |
|-----------|-------------|------|
| roe | roeAvg | 净资产收益率ROE(平均) |
| roa | roa | 总资产净利率ROA |
| gross_income_ratio | grossProfitMargin | 销售毛利率 |
| net_profit_ratio | netProfitMargin | 销售净利率 |

#### 成长能力 (`growth_ability`)

```python
rs = bs.query_growth_data(
    code="sh.600000",
    year=2024,
    quarter=1
)
```

**字段映射**:

| PTrade字段 | BaoStock字段 |
|-----------|-------------|
| operating_revenue_grow_rate | ORPS | 营业收入同比增长率 |
| net_profit_grow_rate | NPGR | 归属母公司净利润同比增长率 |
| total_asset_grow_rate | TAGR | 总资产同比增长率 |

#### 营运能力 (`operating_ability`)

```python
rs = bs.query_operation_data(
    code="sh.600000",
    year=2024,
    quarter=1
)
```

**字段映射**:

| PTrade字段 | BaoStock字段 |
|-----------|-------------|
| total_asset_turnover_rate | assetTurnoverRate | 总资产周转率 |
| inventory_turnover_rate | inventoryTurnoverRate | 存货周转率 |
| accounts_receivables_turnover_rate | accountsReceivableTurnover | 应收账款周转率 |

#### 偿债能力 (`debt_paying_ability`)

```python
rs = bs.query_balance_data(
    code="sh.600000",
    year=2024,
    quarter=1
)
```

**字段映射**:

| PTrade字段 | BaoStock字段 |
|-----------|-------------|
| current_ratio | currentRatio | 流动比率 |
| quick_ratio | quickRatio | 速动比率 |
| debt_equity_ratio | debtEquityRatio | 产权比率 |

---

## 三、复权因子 (`ptrade_adj_pre.h5`)

### 数据结构

```
ptrade_adj_pre.h5
└── /{symbol}    - 复权因子（日频）
```

### BaoStock API

```python
rs = bs.query_adjust_factor(
    code="sh.600000",
    start_date="2024-01-01",
    end_date="2024-12-31"
)
df = rs.get_data()
```

**字段**:
- `date`: 日期
- `foreAdjustFactor`: 前复权因子
- `backAdjustFactor`: 后复权因子

---

## 四、分钟级数据 (暂不支持)

**问题**: BaoStock **不支持**分钟级数据

**备选方案**:
1. **Mootdx**: 支持分钟级数据（推荐）
2. **TuShare**: 支持分钟级数据（需要积分）
3. **AKShare**: 支持分钟级数据（免费）

**建议**: 优先级P4，暂不实现。

---

## 五、实现优先级

### P0 - 核心功能（必须实现）

1. ✅ 日K线数据下载
2. ✅ 代码格式转换
3. ✅ HDF5写入器

### P1 - 基础功能（应该实现）

1. 除权除息数据
2. 股票元数据（含行业分类）
3. 交易日历
4. 股票状态历史（ST/HALT/DELISTING）
5. 股票池获取（季度采样）

### P2 - 增强功能（可以实现）

1. 复权因子
2. 指数成份股（需外部数据源）
3. 增量更新逻辑

### P3 - 扩展功能（有时间实现）

1. 估值数据
2. 财务数据（4个表）
3. 数据验证与修复

### P4 - 未来功能（暂不实现）

1. 分钟级数据（使用Mootdx）
2. Tick数据
3. 龙虎榜数据

---

## 六、实现步骤

### Phase 1: 扩展 BaoStock Fetcher (P0 + P1)

**新增方法**:

1. `fetch_stock_list_by_date(date)` - 封装 `query_all_stock`
2. `fetch_trade_calendar(start, end)` - 封装 `query_trade_dates`
3. `fetch_stock_industry(symbol, date)` - 封装 `query_stock_industry`
4. `fetch_market_data_with_status(...)` - 添加 isST/tradestatus 字段
5. `fetch_valuation_data(...)` - 封装估值字段提取

**修改方法**:

1. `fetch_market_data()` - 支持额外字段参数
2. `fetch_stock_basic()` - 已实现 ✅

### Phase 2: 实现数据收集逻辑 (P0 + P1)

**新增模块**: `simtradedata/collectors/`

1. `stock_pool_collector.py` - 季度采样收集股票池
2. `status_collector.py` - 收集股票状态历史
3. `metadata_collector.py` - 收集并合并元数据

### Phase 3: 扩展 Pipeline (P0 + P1)

**修改 `pipeline.py`**:

1. 支持季度采样
2. 收集状态数据
3. 收集元数据
4. 构建 `stock_status_history`
5. 调用交易日历获取

### Phase 4: 扩展 HDF5Writer (P0 + P1)

**修改 `h5_writer.py`**:

1. `write_trade_calendar()` - 保存交易日历
2. `write_metadata()` - 支持 `stock_status_history` 和 `index_constituents`
3. 验证数据完整性

### Phase 5: 实现估值与财务数据 (P3)

**新增脚本**: `scripts/download_fundamentals.py`

1. 批量下载估值数据（日频）
2. 批量下载财务数据（季频）
3. 保存到 `ptrade_fundamentals.h5`

### Phase 6: 实现 PTrade API (P1)

**新增或修改**:

1. `get_stock_status(stocks, query_type, query_date)`
2. `get_Ashares(date)` - 从季度采样查询
3. `get_trade_days(start, end)` - 从交易日历查询

---

## 七、配置与参数

### 日期范围

```python
START_DATE = '2017-01-01'
END_DATE = None  # None = 当前日期
```

### 采样策略

```python
SAMPLING_FREQ = 'Q'  # 季度采样
```

### 批次大小

```python
BATCH_SIZE = 20  # BaoStock限制: 每批最多20只股票
```

### 限流控制

```python
API_RATE_LIMIT = 90  # 每秒最多90次调用
```

### HDF5压缩

```python
HDF5_COMPLEVEL = 9
HDF5_COMPLIB = 'blosc'
```

---

## 八、数据验证

### 验证清单

1. **数据完整性**:
   - [ ] 所有股票都有 stock_data
   - [ ] 所有股票都有 stock_metadata
   - [ ] 交易日历覆盖完整日期范围

2. **数据一致性**:
   - [ ] stock_status_history 的股票代码 = 股票池
   - [ ] exrights 的日期都在交易日内
   - [ ] 数值类型正确（float64）

3. **格式兼容性**:
   - [ ] HDF5文件结构与PTrade一致
   - [ ] 字段名称完全匹配
   - [ ] 索引类型正确（DatetimeIndex）

---

## 九、注意事项

### 1. BaoStock 限制

- 每秒最多100次API调用
- 季频数据需要逐季度查询
- 部分字段缺失需要计算或忽略

### 2. 代码转换

所有股票代码必须转换：
```python
from simtradedata.utils.code_utils import (
    convert_from_ptrade_code,  # PTrade → BaoStock
    convert_to_ptrade_code     # BaoStock → PTrade
)
```

### 3. 数据类型

BaoStock 返回字符串，需要转换：
```python
df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
df['date'] = pd.to_datetime(df['date'])
```

### 4. 缺失数据处理

- `outDate` 为空 = 未退市
- `blocks` 可能为空
- `index_constituents` 需外部数据源

### 5. 性能优化

- 使用 `rs.get_data()` 一次性获取（已优化 ✅）
- 批量下载减少API调用
- 季度采样减少数据量
- 断点续传支持

---

## 十、测试策略

### 单元测试

1. BaoStock API 字段提取
2. 代码格式转换
3. 数据类型转换
4. JSON序列化/反序列化

### 集成测试

1. 小规模测试（10只股票，1个月）
2. HDF5文件结构验证
3. PTrade API兼容性

### 完整测试

1. 完整股票池，完整日期范围
2. 文件大小基准测试
3. 与PTrade原始数据对比

---

## 附录A: BaoStock API 字段清单

### query_history_k_data_plus

```python
# 日K线 + 估值
fields = "date,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST,peTTM,pbMRQ,psTTM,pcfNcfTTM"
```

**推荐组合**:
- 日K线: `"date,open,high,low,close,volume,amount"`
- 日K线+状态: `"date,open,high,low,close,volume,amount,isST,tradestatus"`
- 估值数据: `"date,peTTM,pbMRQ,psTTM,pcfNcfTTM,turn"`

### 财务数据表

| BaoStock API | 财务类型 | 主要字段 |
|-------------|---------|---------|
| `query_profit_data` | 盈利能力 | roeAvg, roa, grossProfitMargin, netProfitMargin |
| `query_growth_data` | 成长能力 | ORPS, NPGR, TAGR |
| `query_operation_data` | 营运能力 | assetTurnoverRate, inventoryTurnoverRate |
| `query_balance_data` | 偿债能力 | currentRatio, quickRatio, debtEquityRatio |

---

## 附录B: 代码示例

### 完整的下载流程

```python
from simtradedata.fetchers.baostock_fetcher import BaoStockFetcher
from simtradedata.writers.h5_writer import HDF5Writer
import pandas as pd

# 初始化
fetcher = BaoStockFetcher()
writer = HDF5Writer(output_dir='data')

# 1. 获取股票池（季度采样）
sample_dates = pd.date_range(start='2024-01-01', end='2024-12-31', freq='Q')
all_stocks = set()

for date in sample_dates:
    stocks_df = fetcher.fetch_stock_list_by_date(date.strftime('%Y-%m-%d'))
    all_stocks.update(stocks_df['code'].tolist())

stock_pool = sorted(list(all_stocks))

# 2. 下载数据
for stock in stock_pool:
    # 日K线
    market_data = fetcher.fetch_market_data(stock, '2024-01-01', '2024-12-31')
    writer.write_market_data(stock, market_data)

    # 除权除息
    exrights = fetcher.fetch_adjust_factor(stock, '2024-01-01', '2024-12-31')
    writer.write_exrights(stock, exrights)

    # 元数据
    basic_info = fetcher.fetch_stock_basic(stock)
    # ... 收集元数据

# 3. 保存全局元数据
writer.write_metadata(
    start_date='2024-01-01',
    end_date='2024-12-31',
    stock_count=len(stock_pool),
    stock_status_history=status_dict,  # 需要构建
    index_constituents=index_dict      # 可选
)
```

---

## 附录C: 文件大小估算

基于4815只股票，2017-2025年（8年）:

| 文件 | 数据类型 | 估算大小 |
|-----|---------|---------|
| `ptrade_data.h5` | 日K线 + 元数据 | 800-1000 MB |
| `ptrade_fundamentals.h5` | 估值 + 财务 | 300-400 MB |
| `ptrade_adj_pre.h5` | 复权因子 | 50-100 MB |

**总计**: 约 1.5 GB（压缩后）
