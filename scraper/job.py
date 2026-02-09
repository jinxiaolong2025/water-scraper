"""
High-level orchestration for a single scraping run.

The job loads configuration, fetches pages described in `selectors.py`, parses
the tabular data, and persists it using the storage layer.
"""

from __future__ import annotations

import logging
import html
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

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

# 直辖市：这些省份本身就是城市级别，无需城市子选项
MUNICIPALITY_PROVINCES = {"北京市", "天津市", "上海市", "重庆市"}


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

                        raw_headers, row_payloads = _extract_rows_via_publish_api(frame)
                        if raw_headers and row_payloads:
                            logger.info("Using publish API extraction, rows=%s", len(row_payloads))
                        else:
                            logger.warning("Publish API extraction returned no rows, falling back to DOM selectors.")
                            _select_national_scope(frame)
                            perform_scroll(frame, page_config.scroll)

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
                            row_payloads = list(_extract_rows(frame, page_config, raw_headers))

                        snapshot_path = _save_snapshot(
                            frame,
                            project_root,
                            page_config,
                            batch_time,
                            stats.pages_processed,
                        )
                        stats.snapshots.append(snapshot_path)

                        normalized_headers = build_header_mapping(raw_headers)
                        if not normalized_headers:
                            raise SelectorValidationError(
                                "未能解析到任何表头文本。",
                                "请确认 header_cells 选择器能够匹配到 <th> 元素，或更新 column_overrides。",
                            )

                        if not row_payloads:
                            raise SelectorValidationError(
                                "表格数据行未匹配到。",
                                "请检查 data_rows / cell_selector 设置，确保它们指向实际的数据行与单元格。",
                            )

                        for row_payload in row_payloads:
                            row_texts = row_payload["cells"]
                            extras = row_payload.get("extras")
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


def _extract_rows_via_publish_api(frame) -> Tuple[List[str], List[Dict[str, Any]]]:
    """
    Prefer the site's own publish API over brittle UI scrolling.

    The frontend script `RealDatas.js` queries `/GJZ/Ajax/Publish.ashx` with
    action=`getRealDatas`. We replay that request per province and merge rows.
    """
    try:
        frame.wait_for_function(
            "() => Array.isArray(window._TopAreaInfo) && window._TopAreaInfo.length > 0",
            timeout=8_000,
        )
    except PlaywrightTimeout:
        return [], []

    area_ids, province_names_by_id, city_options_by_province = _extract_area_metadata(frame)
    if not area_ids:
        return [], []

    try:
        river_ids = frame.evaluate(
            "() => (window._TopRiverInfo || []).map(item => String(item.RiverID || '')).filter(Boolean)"
        )
    except PlaywrightError:
        river_ids = []
    if not isinstance(river_ids, list):
        river_ids = []

    headers: List[str] = []
    rows: List[Dict[str, Any]] = []
    row_by_key: Dict[Tuple[str, ...], Dict[str, Any]] = {}

    fetch_script = """
        async ({ areaId, riverId, pageIndex, pageSize }) => {
            const params = new URLSearchParams();
            params.set("action", "getRealDatas");
            params.set("AreaID", areaId || "");
            params.set("RiverID", riverId || "");
            params.set("MNName", "");
            params.set("PageIndex", String(pageIndex));
            params.set("PageSize", String(pageSize));
            const resp = await fetch("/GJZ/Ajax/Publish.ashx", {
                method: "POST",
                headers: {
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "X-Requested-With": "XMLHttpRequest"
                },
                body: params.toString(),
                credentials: "same-origin"
            });
            const text = await resp.text();
            try {
                return JSON.parse(text);
            } catch (e) {
                return { result: 0, error: text.slice(0, 300) };
            }
        }
    """

    def collect_scope(area_id: str, river_id: str, scope_label: str, city_name: Optional[str] = None) -> int:
        page_index = 1
        total_pages = 1
        scope_rows = 0
        while page_index <= total_pages and page_index <= 200:
            try:
                payload = frame.evaluate(
                    fetch_script,
                    {"areaId": area_id, "riverId": river_id, "pageIndex": page_index, "pageSize": 9999},
                )
            except PlaywrightError:
                break

            if not isinstance(payload, dict) or not payload.get("result"):
                break

            if not headers:
                thead = payload.get("thead") or []
                if isinstance(thead, list):
                    headers.extend(_normalize_api_text(item) for item in thead)

            tbody = payload.get("tbody") or []
            if not isinstance(tbody, list):
                break

            for row in tbody:
                if not isinstance(row, list):
                    continue
                raw_cells = ["" if item is None else str(item) for item in row]
                cells = [_normalize_api_text(item) for item in raw_cells]
                if not cells:
                    continue
                dedupe_key = tuple(cells[:5])
                existing = row_by_key.get(dedupe_key)
                extras: Dict[str, Optional[str]] = {}
                # 优先使用传入的城市名，其次尝试从 API 单元格提取
                if city_name:
                    extras["city"] = city_name
                else:
                    city = _extract_city_from_api_cells(raw_cells)
                    if city:
                        extras["city"] = city

                if existing:
                    existing_city = (existing.get("extras") or {}).get("city")
                    if city and not existing_city:
                        existing.setdefault("extras", {})["city"] = city
                    continue

                row_payload = {"cells": cells, "extras": extras}
                rows.append(row_payload)
                row_by_key[dedupe_key] = row_payload
                scope_rows += 1

            try:
                total_pages = max(1, int(payload.get("total") or 1))
            except (TypeError, ValueError):
                total_pages = 1
            page_index += 1

        logger.info("API %s -> rows=%s", scope_label, scope_rows)
        return scope_rows

    # 按城市级别遍历采集，自动注入城市名
    for province_id in area_ids:
        province_name = province_names_by_id.get(province_id, "")
        city_list = city_options_by_province.get(province_id, [])
        if city_list:
            # 普通省份：遍历其下所有城市
            for city_id, city_label in city_list:
                collect_scope(
                    area_id=city_id,
                    river_id="",
                    scope_label=f"city:{city_label}",
                    city_name=city_label,
                )
        else:
            # 直辖市或无城市子选项的省份
            city_value = province_name if province_name in MUNICIPALITY_PROVINCES else None
            collect_scope(
                area_id=province_id,
                river_id="",
                scope_label=f"area:{province_id}",
                city_name=city_value,
            )

    # Fallback enrichment: if area pass is unexpectedly small, add river pass.
    if len(rows) < 1000 and river_ids:
        logger.warning("Area API rows=%s is lower than expected, running river fallback pass.", len(rows))
        for river_id in river_ids:
            collect_scope(area_id="", river_id=river_id, scope_label=f"river:{river_id}")

    return headers, rows


_HTML_TAG_RE = re.compile(r"<[^>]+>")
_CITY_HINT_RE = re.compile(r"所在地市\s*[:：]\s*([^\n\r<\"]+)")


def _normalize_api_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    if "<" in text and ">" in text:
        text = _HTML_TAG_RE.sub("", text)
    text = text.replace("&nbsp;", "").replace("\xa0", " ").replace("\n", "")
    text = text.strip()
    if text == "--":
        return "--"
    return text


def _extract_area_metadata(frame) -> Tuple[List[str], Dict[str, str], Dict[str, List[Tuple[str, str]]]]:
    """
    Parse province/city area IDs from the area dropdown.

    Returns:
        area_ids: province-level AreaID list
        province_names_by_id: mapping from province AreaID to label
        city_options_by_province: {province_id: [(city_id, city_label), ...]}
    """
    try:
        raw = frame.evaluate(
            """
            () => {
                const payload = { items: [] };
                const anchors = Array.from(document.querySelectorAll("#ddm_Area + ul a[onclick*='filterArea(']"));
                for (const anchor of anchors) {
                    const onclick = anchor.getAttribute("onclick") || "";
                    const m = onclick.match(/filterArea\\('([^']*)','([^']*)',(\\d+)\\)/);
                    if (!m) continue;
                    const id = String(m[1] || "");
                    const label = String(m[2] || "");
                    const level = Number(m[3] || "0");
                    const parentId = String(anchor.getAttribute("data-id") || "");
                    payload.items.push({ id, label, level, parentId });
                }
                return payload;
            }
            """
        )
    except PlaywrightError:
        raw = None

    items = raw.get("items") if isinstance(raw, dict) else None
    area_ids: List[str] = []
    province_names_by_id: Dict[str, str] = {}
    city_options_by_province: Dict[str, List[Tuple[str, str]]] = {}
    seen_province_ids: Set[str] = set()
    seen_city_ids_by_province: Dict[str, Set[str]] = {}

    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            area_id = _normalize_api_text(item.get("id"))
            label = _normalize_api_text(item.get("label"))
            parent_id = _normalize_api_text(item.get("parentId"))
            try:
                level = int(item.get("level") or 0)
            except (TypeError, ValueError):
                level = 0
            if not area_id or not label:
                continue

            if level == 1:
                if area_id not in seen_province_ids:
                    seen_province_ids.add(area_id)
                    area_ids.append(area_id)
                province_names_by_id[area_id] = label
            elif level == 2 and parent_id:
                city_options_by_province.setdefault(parent_id, [])
                seen_city_ids_by_province.setdefault(parent_id, set())
                if area_id in seen_city_ids_by_province[parent_id]:
                    continue
                seen_city_ids_by_province[parent_id].add(area_id)
                city_options_by_province[parent_id].append((area_id, label))

    if area_ids:
        return area_ids, province_names_by_id, city_options_by_province

    # Fallback to JS globals when dropdown parsing fails.
    try:
        fallback = frame.evaluate(
            "() => (window._TopAreaInfo || []).map(item => ({id: String(item.AreaID || ''), label: String(item.AreaName || '')}))"
        )
    except PlaywrightError:
        fallback = []

    if not isinstance(fallback, list):
        return [], {}, {}

    for item in fallback:
        if not isinstance(item, dict):
            continue
        area_id = _normalize_api_text(item.get("id"))
        label = _normalize_api_text(item.get("label"))
        if not area_id:
            continue
        area_ids.append(area_id)
        if label:
            province_names_by_id[area_id] = label
    return area_ids, province_names_by_id, {}


def _extract_city_from_api_cells(raw_cells: List[str]) -> Optional[str]:
    """Best-effort extraction of city from HTML tooltip fragments in API payload."""
    for raw in raw_cells:
        if not raw:
            continue
        text = html.unescape(raw)
        match = _CITY_HINT_RE.search(text)
        if not match:
            continue
        city = _normalize_api_text(match.group(1))
        if city and city != "--":
            return city
    return None


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
    rows_before = _safe_row_count(frame)
    switched = False

    # Prefer first-party page APIs when available (more resilient than clicking).
    try:
        switched = bool(
            frame.evaluate(
                "() => (typeof window.filterArea === 'function') && (window.filterArea('', '城市', 0), true)"
            )
        )
    except PlaywrightError:
        switched = False

    if not switched:
        try:
            dropdown = frame.wait_for_selector("#ddm_Area", timeout=2_000)
        except PlaywrightTimeout:
            dropdown = None

        if dropdown is not None:
            try:
                dropdown.click()
                option_locator = frame.locator("ul[aria-labelledby='ddm_Area'] a", has_text="全国")
                if option_locator.count() == 0:
                    option_locator = frame.locator("ul[aria-labelledby='ddm_Area'] a[onclick*=\"filterArea('',\"]")
                if option_locator.count() > 0:
                    option_locator.first.click()
                    switched = True
                else:
                    logger.debug("Nationwide option not found in area dropdown.")
            except PlaywrightError:
                logger.debug("Failed to switch area filter via dropdown interaction.")

    if switched:
        _wait_for_row_change(frame, rows_before, label="Area filter")

    _select_all_river_scope(frame)


def _select_all_river_scope(frame) -> None:
    """Reset basin filter to 'all' so nationwide rows are not truncated."""
    rows_before = _safe_row_count(frame)
    switched = False
    try:
        switched = bool(
            frame.evaluate(
                "() => (typeof window.filterRiver === 'function') && (window.filterRiver('', '流域'), true)"
            )
        )
    except PlaywrightError:
        switched = False

    if switched:
        _wait_for_row_change(frame, rows_before, label="River filter")


def _safe_row_count(frame) -> int:
    try:
        return frame.locator("#gridDatas li").count()
    except PlaywrightError:
        return 0


def _wait_for_row_change(frame, rows_before: int, label: str) -> None:
    try:
        frame.wait_for_timeout(1_500)
        if rows_before:
            frame.wait_for_function(
                "([selector, previous]) => document.querySelectorAll(selector).length !== previous",
                ("#gridDatas li", rows_before),
                timeout=6_000,
            )
    except PlaywrightTimeout:
        logger.debug("%s did not change row count within timeout.", label)
