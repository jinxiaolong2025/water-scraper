# 水质数据抓取部署与运维手册

> 目录：`/opt/water-scraper`

本文档记录整个爬虫的工作原理、本地/服务器操作、常用命令、日志位置、排错方式等，供查阅。

---

## 1. 项目概览

- **入口**：`run_once.py`，执行一次完整抓取。
- **配置**：`config/settings.yaml`
  ```yaml
  default:
    database_path: "data/water_quality.db"   # 当前写入 SQLite
    timezone: "Asia/Shanghai"
    playwright:
      headless: true
      timeout_ms: 60000
  ```
- **核心模块**：
  | 模块 | 作用 |
  | --- | --- |
  | `scraper/browser.py` | 启动 Playwright、进入 iframe、触发滚动/全国按钮 |
  | `scraper/job.py` | 主流程：重试、快照、写库 |
  | `scraper/parser.py` | 将表格转换为字段、解析时间/数值 |
  | `scraper/storage.py` | SQLAlchemy 模型 + upsert 逻辑 |
  | `scraper/webapp` | FastAPI 可视化页面 |
- **数据存储**：`data/water_quality.db`（SQLite）。`data/snapshots/` 保存 HTML 快照和日志。

流程：Playwright -> 选择全国 -> 滚动加载 -> 解析 -> 写入 SQLite -> 保存快照。

---

## 2. 本地环境

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
python run_once.py      # 抓取一次
uvicorn webapp.main:app --reload   # 查看页面
```

> 注：首次运行需安装浏览器内核 `playwright install chromium`。

---

## 3. 服务器部署（ECS Ubuntu 22.04）

### 3.1 安装依赖
```bash
apt update && apt upgrade -y
apt install -y git wget build-essential python3 python3-venv python3-pip \
               libnss3 libgbm1 libasound2 sqlite3
```

### 3.2 获取代码与环境
```bash
mkdir -p /opt
cd /opt
git clone https://github.com/jinxiaolong2025/water-scraper.git
cd water-scraper
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

### 3.3 手动抓取（检查）
```bash
/opt/water-scraper/.venv/bin/python /opt/water-scraper/run_once.py
```
运行成功会下发快照和日志：`data/snapshots/2025...html`、`/var/log/water-scraper.log`。

---

## 4. 定时任务 (systemd, 每 2 小时)

### 4.1 执行脚本 `/opt/water-scraper/run_job.sh`
```bash
#!/bin/bash
cd /opt/water-scraper
/opt/water-scraper/.venv/bin/python run_once.py >> /var/log/water-scraper.log 2>&1
```
`chmod +x /opt/water-scraper/run_job.sh`

### 4.2 Service `/etc/systemd/system/water-scraper.service`
```ini
[Unit]
Description=Water Scraper Run

[Service]
Type=oneshot
ExecStart=/opt/water-scraper/run_job.sh
WorkingDirectory=/opt/water-scraper
```

### 4.3 Timer `/etc/systemd/system/water-scraper.timer`
```ini
[Unit]
Description=Run water scraper every 2 hours

[Timer]
OnUnitActiveSec=2h           # 每 2 小时运行一次
Unit=water-scraper.service

[Install]
WantedBy=timers.target
```

### 4.4 启动
```bash
systemctl daemon-reload
systemctl enable --now water-scraper.timer   # 定时器
systemctl start water-scraper.service       # 如需立即执行一次
```

---

## 5. 监控与排错

### 5.1 定时器/服务状态
```bash
systemctl list-timers --all | grep water-scraper
systemctl status water-scraper.timer
systemctl status water-scraper.service
journalctl -u water-scraper.service -n 50
```

### 5.2 日志位置
- 运行日志：`/var/log/water-scraper.log`
  ```bash
  tail -f /var/log/water-scraper.log
  ```
- systemd 错误：`journalctl -xeu water-scraper.service`
- Playwright 快照：`/opt/water-scraper/data/snapshots/`

### 5.3 常见错误
| 现象 | 原因 | 处理 |
| ---- | ---- | ---- |
| `ERR_EMPTY_RESPONSE` | 目标站无响应或 IP 被限制 | 等待或配置代理；脚本自动重试 5 次 |
| `playwright install-deps` 提示依赖 | 系统缺少 libnss3/libasound2 等 | `apt install` 上述包 |
| systemd `status=127` | `/usr/bin/python` 不存在 | 在脚本中使用 `.venv/bin/python` |
| `frame.wait_for_function` 报参数错误 | Playwright 版本不同 | 已调整为 `frame.wait_for_function(..., arg=(...))` |

---

## 6. 数据库操作

- 文件：`/opt/water-scraper/data/water_quality.db`
- 备份：`cp data/water_quality.db data/water_quality_$(date +%F).db`
- 查看统计：
  ```bash
  cd /opt/water-scraper
  sqlite3 data/water_quality.db <<'SQL'
  .mode column
  .headers on
  SELECT COUNT(*) AS stations FROM stations;
  SELECT COUNT(*) AS readings FROM readings;
  SELECT MAX(batch_time) AS last_batch FROM readings;
  SQL
  ```
- 导出 CSV：
  ```bash
  sqlite3 data/water_quality.db <<'SQL'
  .headers on
  .mode csv
  .output /tmp/readings.csv
  SELECT * FROM readings;
  .quit
  SQL
  # 下载 CSV
  scp root@<ECS_IP>:/tmp/readings.csv .
  ```
- 下载 DB：`scp root@<ECS_IP>:/opt/water-scraper/data/water_quality.db .`

---

## 7. Web 界面
```bash
cd /opt/water-scraper
source .venv/bin/activate
uvicorn webapp.main:app --host 0.0.0.0 --port 8000
```
默认页面 `http://<ECS_IP>:8000`，功能：
- 省/市/流域/数值筛选
- 每页条数/分页
- 数值字段范围过滤、空值过滤
- CSV 导出

要部署到线上，需要在安全组开放 8000 端口或通过 Nginx 代理。

---

## 8. 数据备份与扩展

- **备份**：定期将 `data/water_quality.db` 或 `/tmp/readings.csv` 上传到 OSS。
- **迁移至 MySQL/RDS**：
  1. 在 RDS 创建 `stations`/`readings` 表（结构同 `storage.py`）。
  2. 写迁移脚本读取 SQLite，调用 `storage.upsert_row` 写入 RDS。
  3. 在 `config/settings.yaml` 中增加 `database_url`，`storage.get_session_factory` 使用 `create_engine(database_url)`。
- **代理/白名单**：若云服务器经常 `ERR_EMPTY_RESPONSE`，需要联系目标站放行 IP 或配置 HTTP 代理，并在 Playwright `browser.new_context` 中设置 `proxy`。

---

## 9. 目录结构速查

```
/opt/water-scraper
├── config/settings.yaml
├── data/
│   ├── water_quality.db
│   └── snapshots/*.html
├── run_once.py
├── run_job.sh
├── scraper/
│   ├── browser.py
│   ├── job.py
│   ├── parser.py
│   └── storage.py
└── webapp/
    ├── main.py
    └── templates/index.html
```

---

## 10. 常用命令清单

| 操作 | 命令 |
| --- | --- |
| 手动抓取一次 | `/opt/water-scraper/.venv/bin/python run_once.py` |
| 查看定时器状态 | `systemctl list-timers --all | grep water-scraper` |
| 查看 service 日志 | `journalctl -u water-scraper.service -n 50` |
| 实时查看日志 | `tail -f /var/log/water-scraper.log` |
| 下载 SQLite | `scp root@IP:/opt/water-scraper/data/water_quality.db .` |
| 导出 CSV | `sqlite3 ...; scp root@<IP>:/tmp/readings.csv .` |
| 启动/停止定时器 | `systemctl start/stop water-scraper.service`、`systemctl enable/disable water-scraper.timer` |

---

---

## 11. 迁移到 MySQL/RDS 

1. **创建数据库**：在 RDS 或自建 MySQL 中执行：
   ```sql
   CREATE DATABASE water DEFAULT CHARSET utf8mb4;
   USE water;
   -- 站点表
   CREATE TABLE stations (
     id INT AUTO_INCREMENT PRIMARY KEY,
     province VARCHAR(64),
     city VARCHAR(64),
     basin VARCHAR(128),
     river VARCHAR(128),
     station_name VARCHAR(128) NOT NULL,
     station_code VARCHAR(64) UNIQUE,
     UNIQUE KEY uq_station_composite (province, city, basin, river, station_name)
   );
   -- 读数表
   CREATE TABLE readings (
     id INT AUTO_INCREMENT PRIMARY KEY,
     station_id INT NOT NULL,
     observed_at DATETIME NOT NULL,
     batch_time DATETIME NOT NULL,
     payload JSON,
     UNIQUE KEY uq_station_time (station_id, observed_at),
     FOREIGN KEY (station_id) REFERENCES stations(id) ON DELETE CASCADE
   );
   ```
2. **迁移脚本**：
   ```python
   from sqlalchemy import create_engine
   from sqlalchemy.orm import sessionmaker
   from scraper.storage import Base, Station, Reading

   sqlite_engine = create_engine('sqlite:////opt/water-scraper/data/water_quality.db')
   mysql_engine = create_engine('mysql+pymysql://user:password@host:3306/water')

   Base.metadata.create_all(mysql_engine)
   SqliteSession = sessionmaker(bind=sqlite_engine)
   MysqlSession = sessionmaker(bind=mysql_engine)

   with SqliteSession() as src, MysqlSession() as dst:
       for station in src.query(Station):
           dst.merge(station)
       dst.commit()
       for reading in src.query(Reading):
           dst.merge(reading)
       dst.commit()
   ```
3. **切换配置**：在 `settings.yaml` 中新增 `database_url`，并让 `storage.get_session_factory` 使用 `create_engine(database_url)`。
4. **测试**：在 ECS 上运行一次 `python run_once.py`，确认 RDS 表的记录数递增。

---

## 12. OSS 备份脚本

如需将 SQLite 或 CSV 备份到 OSS，可使用阿里云 `ossutil64`：
```bash
# 安装 ossutil (参考阿里云文档)
chmod +x ossutil64
./ossutil64 config   # 设置 key/secret/bucket

# 备份数据库
ossutil64 cp /opt/water-scraper/data/water_quality.db oss://your-bucket/backups/water_quality_$(date +%F).db
# 备份 CSV
ossutil64 cp /tmp/readings.csv oss://your-bucket/backups/readings_$(date +%F).csv
```

---

## 13. FAQ

1. **更新代码**
   ```bash
   cd /opt/water-scraper
   git pull
   source .venv/bin/activate && pip install -r requirements.txt
   systemctl restart water-scraper.service
   ```
2. **如何查看快照具体内容？**
   ```bash
   ls data/snapshots | tail
   less data/snapshots/2025xxxx.html
   ```
3. **如何修改运行频率？** 修改 `water-scraper.timer` 的 `OnUnitActiveSec`，例如改为 4 小时：`OnUnitActiveSec=4h`，然后 `systemctl daemon-reload && systemctl restart water-scraper.timer`。
4. **如何强制停止当前运行？**
   ```bash
   pkill -f run_once.py
   systemctl stop water-scraper.service
   ```
5. **如何排查 Playwright 网络问题？** 使用 `curl -I https://...` 验证网络；必要时在 `browser.new_context` 中设置代理 `proxy={"server": "http://ip:port"}`。

---

## 14. 联系与变更记录

- **负责人**：`jinxiaolong`
- **最新版代码**：https://github.com/jinxiaolong2025/water-scraper
- **常用路径**：
  - `/opt/water-scraper`：代码
  - `/opt/water-scraper/.venv`：虚拟环境
  - `/var/log/water-scraper.log`：运行日志

