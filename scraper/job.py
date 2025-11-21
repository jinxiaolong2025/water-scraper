"""
High-level orchestration for a single scraping run.

The job loads configuration, fetches pages described in `selectors.py`, parses
the tabular data, and persists it using the storage layer.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import yaml

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:  # pragma: no cover
    from pytz import timezone as ZoneInfo  # type: ignore

from playwright.sync_api import Error as PlaywrightError, TimeoutError as PlaywrightTimeout

from .browser import BrowserConfig, browser_page, navigate_to_table, perform_scroll
from .parser import ParsedRow, build_header_mapping, parse_row
from .selectors import PageSelectors, get_all_pages
from .storage import UpsertResult, get_session_factory, upsert_row

logger = logging.getLogger(__name__)


@dataclass
class JobStats:
    pages_processed: int = 0
    rows_seen: int = 0
    rows_inserted: int = 0
    database_path: str = ""
    snapshots: List[Path] = field(default_factory=list)


class SelectorValidationError(RuntimeError):
    """Raised when configured selectors fail to match page elements."""

    def __init__(self, message: str, suggestion: str):
        super().__init__(message)
        self.suggestion = suggestion


def load_settings(settings_path: Path) -> Dict[str, object]:
    with settings_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)["default"]


def run_once() -> JobStats:
    """
    Execute a single batch scraping job.

    The function assumes it is invoked from the project root (via run_once.py)
    so relative paths remain stable.
    """
    project_root = Path(__file__).resolve().parent.parent
    config_path = project_root / "config" / "settings.yaml"
    settings = load_settings(config_path)

    tz_name = settings.get("timezone", "Asia/Shanghai")
    tz = ZoneInfo(tz_name) if isinstance(ZoneInfo, type) else ZoneInfo(tz_name)
    batch_time = datetime.now(tz)

    db_path = project_root / settings.get("database_path", "data/water_quality.db")
    SessionFactory = get_session_factory(str(db_path))

    playwright_cfg = settings.get("playwright", {})
    browser_cfg = BrowserConfig(
        headless=bool(playwright_cfg.get("headless", True)),
        timeout_ms=int(playwright_cfg.get("timeout_ms", 15_000)),
    )

    stats = JobStats(database_path=str(db_path))
    pages: List[PageSelectors] = get_all_pages()

    max_attempts = 5

    with SessionFactory() as session:
        for page_config in pages:
            last_error: Optional[SelectorValidationError] = None
            for attempt in range(1, max_attempts + 1):
                logger.info("Processing page %s (attempt %s/%s)", page_config.url, attempt, max_attempts)
                try:
                    with browser_page(browser_cfg) as page:
                        try:
                            frame = navigate_to_table(page, page_config)
                        except PlaywrightError as exc:
                            raise SelectorValidationError(
                                f"无法根据 iframe 选择器进入目标页面: {exc}",
                                "请检查 iframe_chain 是否完整，并确保每个选择器都能唯一定位到 iframe 元素。",
                            ) from exc

                        _select_national_scope(frame)
                        perform_scroll(frame, page_config.scroll)
                        snapshot_path = _save_snapshot(frame, project_root, page_config, batch_time, stats.pages_processed)
                        stats.snapshots.append(snapshot_path)

                        try:
                            table_root = frame.wait_for_selector(
                                page_config.table.table_container,
                                timeout=browser_cfg.timeout_ms,
                            )
                        except PlaywrightTimeout as exc:
                            raise SelectorValidationError(
                                f"未找到表格容器选择器: {page_config.table.table_container}",
                                "请在 Chrome DevTools 中重新确认 TableSelectors.table_container 是否指向包含数据表的元素。",
                            ) from exc

                        if table_root is None:
                            raise SelectorValidationError(
                                f"表格容器选择器返回空元素: {page_config.table.table_container}",
                                "请确保 table_container 指向页面上的真实表格容器。",
                            )

                        raw_headers = _extract_headers(frame, page_config)
                        normalized_headers = build_header_mapping(raw_headers)

                        if not normalized_headers:
                            raise SelectorValidationError(
                                "未能解析到任何表头文本。",
                                "请确认 header_cells 选择器能够匹配到 <th> 元素，或更新 column_overrides。",
                            )

                        row_found = False
                        for row_payload in _extract_rows(frame, page_config, raw_headers):
                            row_texts = row_payload["cells"]
                            extras = row_payload.get("extras")
                            row_found = True
                            parsed = parse_row(normalized_headers, row_texts, tz_name, extras=extras)
                            stats.rows_seen += 1
                            result = upsert_row(
                                session,
                                parsed.station,
                                parsed.reading,
                                batch_time,
                            )
                            if result.created:
                                stats.rows_inserted += 1

                        if not row_found:
                            raise SelectorValidationError(
                                "表格数据行未匹配到。",
                                "请检查 data_rows / cell_selector 设置，确保它们指向实际的数据行与单元格。",
                            )

                    stats.pages_processed += 1
                    last_error = None
                    break
                except SelectorValidationError as exc:
                    session.rollback()
                    last_error = exc
                    if attempt >= max_attempts:
                        raise
                    logger.warning("Attempt %s/%s failed: %s，retrying...", attempt, max_attempts, exc)
                    time.sleep(1)
                    continue
                except PlaywrightError as exc:
                    session.rollback()
                    raise SelectorValidationError(
                        f"页面交互过程中出现 Playwright 错误: {exc}",
                        "请确认页面结构是否发生变化，必要时调整选择器或增加超时时间。",
                    ) from exc

            if last_error:
                raise last_error

        session.commit()

    return stats


def _extract_headers(frame, page_config: PageSelectors) -> List[str]:
    """Return plain-text headers from the configured selectors."""
    table = frame.query_selector(page_config.table.table_container)
    if table is None:
        return []

    header_elements = table.query_selector_all(page_config.table.header_cells)
    return [element.inner_text().strip() for element in header_elements]


def _extract_rows(frame, page_config: PageSelectors, raw_headers: List[str]):
    """Yield rows of text values using the configured selectors."""
    table = frame.query_selector(page_config.table.table_container)
    if table is None:
        return []

    def _cell_value(cell):
        def _extract_raw(text: Optional[str]) -> Optional[str]:
            if not text or "原始值" not in text:
                return None
            for line in text.splitlines():
                if "原始值" in line:
                    raw = line.split("原始值", 1)[-1]
                    raw = raw.replace("：", "").replace(":", "").strip()
                    if raw:
                        return raw
            return None

        if not cell:
            return ""

        tooltip = cell.get_attribute("data-original-title")
        raw = _extract_raw(tooltip)
        if raw:
            return raw

        inner = cell.query_selector("[data-original-title]")
        if inner:
            raw = _extract_raw(inner.get_attribute("data-original-title"))
            if raw:
                return raw

        return cell.inner_text().strip()

    for row in table.query_selector_all(page_config.table.data_rows):
        column_text: List[str]
        if page_config.table.cell_selector:
            cells = row.query_selector_all(page_config.table.cell_selector)
            column_text = [_cell_value(cell) for cell in cells]
        elif page_config.table.column_overrides:
            column_text = []
            for header in raw_headers:
                selector = page_config.table.column_overrides.get(header)
                if not selector:
                    column_text.append("")
                    continue
                cell = row.query_selector(selector)
                column_text.append(_cell_value(cell))
        else:
            cells = row.query_selector_all("td, th")
            column_text = [_cell_value(cell) for cell in cells]

        extras: Dict[str, Optional[str]] = {}
        tooltip_host = row.query_selector("td.MN [data-original-title]")
        if tooltip_host:
            tooltip = tooltip_host.get_attribute("data-original-title") or ""
            for line in tooltip.splitlines():
                line = line.strip()
                if line.startswith("所在地市:"):
                    extras["city"] = line.split(":", 1)[1].strip() or None
                    break

        if column_text:
            yield {"cells": column_text, "extras": extras}


def _save_snapshot(frame, project_root: Path, page_config: PageSelectors, batch_time: datetime, page_index: int) -> Path:
    """
    Persist the current HTML to data/snapshots for auditing.
    """
    snapshots_dir = project_root / "data" / "snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    timestamp = batch_time.strftime("%Y%m%dT%H%M%S")
    url_fragment = (
        page_config.url.replace("https://", "")
        .replace("http://", "")
        .replace("/", "_")
        .replace("?", "_")
        .replace("=", "_")
    )
    filename = f"{timestamp}_{page_index:02d}_{url_fragment}.html"
    snapshot_path = snapshots_dir / filename
    snapshot_path.write_text(frame.content(), encoding="utf-8")
    return snapshot_path


def _select_national_scope(frame) -> None:
    """
    Ensure the data grid is scoped to the nationwide view instead of the default region.

    The site renders a Bootstrap dropdown with id `ddm_Area`; selecting the first
    item labelled “全国” loads the nationwide dataset. We no-op if the dropdown
    is missing or already shows 全国.
    """
    try:
        dropdown = frame.wait_for_selector("#ddm_Area", timeout=2_000)
    except PlaywrightTimeout:
        logger.debug("Area dropdown #ddm_Area not found.")
        return

    if dropdown is None:
        return

    try:
        current_label = dropdown.inner_text().strip()
    except PlaywrightError:
        current_label = ""

    if "全国" in current_label:
        return

    rows_locator = frame.locator("#gridDatas li")
    try:
        rows_before = rows_locator.count()
    except PlaywrightError:
        rows_before = 0

    dropdown.click()
    option_locator = frame.locator(
        "ul.dropdown-menu[aria-labelledby='ddm_Area'] a.area-item",
        has_text="全国",
    )
    if option_locator.count() == 0:
        logger.debug("Nationwide option not found in area dropdown.")
        return

    option_locator.first.click()
    try:
        frame.wait_for_timeout(1_500)
        if rows_before:
            frame.wait_for_function(
                "([selector, previous]) => document.querySelectorAll(selector).length !== previous",
                ("#gridDatas li", rows_before),
                timeout=5_000,
            )
    except PlaywrightTimeout:
        logger.debug("Nationwide data did not change row count within timeout.")
