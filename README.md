# National Water Quality Scraper (Skeleton)

This project provides a minimal, editable Playwright + Python pipeline for capturing national water quality data into SQLite. The selectors are placeholder values—you should update them once the target site is confirmed.

## Prerequisites

- Python 3.9+
- [Poetry](https://python-poetry.org/) or `pip`
- Playwright browsers (install step shown below)

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install
python run_once.py
```

By default, results are stored in `data/water_quality.db` (created automatically). The scraper logs progress to stdout.

## Configuration

- Edit `config/settings.yaml` for database path, timezone (`Asia/Shanghai` by default), and Playwright options.
- Update `scraper/selectors.py` with the actual URL, iframe chain, table selectors, and pagination strategy (`infinite_scroll` or `load_more`).

## Chrome DevTools selector checklist

1. 打开目标站点并按 `F12`（或 `Cmd+Opt+I`）进入 Chrome DevTools。
2. 在 Elements 面板点击左上角的箭头工具，依次点选页面中的 iframe 元素，确认表格是否嵌套于 iframe；复制 `id`/`name` 或 `css` 选择器填入 `iframe_chain`。
3. 在选中最终承载表格的 iframe 后，重新聚焦到该 iframe（在右上角的 “Select frame” 下拉菜单里选择它）。
4. 选中整个表格外层容器（通常包含 `<table>` 或虚拟滚动列表），复制其唯一的 CSS 选择器填到 `TableSelectors.table_container`。
5. 选中表头行中的任意 `<th>`，右键选择 “Copy > Copy selector”，粘贴到 `TableSelectors.header_cells`，必要时手动简化成更稳定的类选择器。
6. 选中任意数据行 `<tr>` 元素，复制其 CSS 选择器到 `TableSelectors.data_rows`；如果每列结构一致，再把单元格 `<td>` 的选择器填到 `cell_selector`。
7. 若存在“加载更多”按钮或分页控件，选中按钮并复制选择器填入 `ScrollSettings.load_more_button`；如需滚动，找到滚动容器元素把选择器填入 `ScrollSettings.container`。
8. 回到 Console 面板，使用 `document.querySelectorAll('<your selector>')` 验证是否能选中文件；如需 iframe，使用 `document.querySelector('<iframe selector>').contentDocument` 链式验证。
9. 记录页面是否需要等待动态渲染；可在 Network 面板观察接口返回时间，并根据情况调大 `ScrollSettings.wait_for_ms`。

## Code layout

- `scraper/browser.py`: Launches Playwright, enters iframes, handles scrolling/load-more.
- `scraper/parser.py`: Maps Chinese headers to canonical fields and normalizes values.
- `scraper/storage.py`: SQLAlchemy models for stations/readings with idempotent upsert logic.
- `scraper/job.py`: End-to-end orchestration (load selectors, browse, parse, persist).
- `run_once.py`: Convenience entry point to execute a single scraping batch.

Once selectors are confirmed, run `python run_once.py` to perform a scrape without modifying any code. The placeholder selectors will need to be replaced with the real page structure before it can succeed against the live site.
