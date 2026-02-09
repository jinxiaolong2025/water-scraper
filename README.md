# 全国水质数据爬虫 (National Water Quality Scraper)

基于 Playwright + Python 的全国水质监测数据采集工具，数据存储于 SQLite。支持自动化部署，内置去重机制。

## 数据来源

- **目标网站**: 国家地表水水质自动监测实时数据发布系统
- **数据内容**: 省份、城市、流域、河流、断面名称、监测时间、水质类别、各项水质指标（水温、pH、溶解氧、电导率、浊度、高锰酸盐指数、氨氮、总磷、总氮等）

## 特性

- ✅ **城市级别采集**: 自动遍历所有省份下的城市，确保城市字段正确采集
- ✅ **智能去重**: 基于 `(station_id, observed_at)` 唯一索引，避免重复数据
- ✅ **增量更新**: 新站点自动添加，已有站点新数据自动追加
- ✅ **定时部署**: 支持服务器定时任务（如每 2 小时自动采集一次）

## 快速开始

```bash
# 创建虚拟环境
python -m venv .venv
source .venv/bin/activate

# 安装依赖
pip install -r requirements.txt
playwright install

# 运行采集
python run_once.py
```

数据默认存储在 `data/water_quality.db`，采集日志输出到 stdout。

## 配置

- `config/settings.yaml`: 数据库路径、时区（默认 `Asia/Shanghai`）、Playwright 选项
- `scraper/selectors.py`: 目标 URL、iframe 选择器、表格选择器、分页策略

## 数据库结构

### stations 表（监测站点）
| 字段 | 说明 |
|------|------|
| id | 主键 |
| province | 省份 |
| city | 城市 |
| basin | 流域 |
| river | 河流 |
| station_name | 断面名称 |
| station_code | 断面编码 |

### readings 表（监测数据）
| 字段 | 说明 |
|------|------|
| id | 主键 |
| station_id | 关联站点 |
| observed_at | 监测时间（唯一索引） |
| batch_time | 采集批次时间 |
| payload | JSON 格式的监测指标数据 |

## 去重机制

- **readings 表**: 基于 `(station_id, observed_at)` 唯一索引去重
- **stations 表**: 基于 `(province, city, basin, river, station_name)` 复合唯一约束去重

验证去重效果：
```bash
sqlite3 data/water_quality.db "SELECT station_id, observed_at, COUNT(*) as cnt FROM readings GROUP BY station_id, observed_at HAVING cnt > 1;"
# 预期结果为空（无重复）
```

## 服务器部署

可使用 cron 定时任务实现自动采集：
```bash
# 每 2 小时采集一次
0 */2 * * * cd /path/to/water-scraper-main && /path/to/python run_once.py >> logs/scraper.log 2>&1
```

详细部署指南请参考 [DEPLOY_GUIDE.md](./DEPLOY_GUIDE.md)。

## 代码结构

| 文件 | 说明 |
|------|------|
| `scraper/browser.py` | Playwright 浏览器控制，iframe 进入，滚动加载 |
| `scraper/parser.py` | 中文表头映射、数据标准化 |
| `scraper/storage.py` | SQLAlchemy 模型，幂等 upsert 逻辑 |
| `scraper/selectors.py` | 页面选择器配置 |
| `scraper/job.py` | 采集流程编排（按城市遍历 API → 解析 → 存储） |
| `run_once.py` | 单次采集入口 |
