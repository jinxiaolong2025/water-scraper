from __future__ import annotations

import json
from datetime import datetime, time
import csv
import io
import math
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlencode

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import Float, Select, cast, func, select
from sqlalchemy.orm import Session

from scraper.job import load_settings
from scraper.parser import parse_numeric
from scraper.storage import Reading, Station, get_engine

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SETTINGS_PATH = PROJECT_ROOT / "config" / "settings.yaml"
SETTINGS = load_settings(SETTINGS_PATH)
DB_PATH = PROJECT_ROOT / SETTINGS.get("database_path", "data/water_quality.db")
ENGINE = get_engine(str(DB_PATH))

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
app = FastAPI(title="Water Quality Browser")

DISPLAY_METRICS = [
    ("water_quality_class", "水质类别"),
    ("water_temperature_c", "水温(℃)"),
    ("ph", "pH"),
    ("dissolved_oxygen_mg_l", "溶解氧(mg/L)"),
    ("conductivity_us_cm", "电导率(μS/cm)"),
    ("turbidity_ntu", "浊度(NTU)"),
    ("permanganate_index_mg_l", "高锰酸盐指数(mg/L)"),
    ("ammonia_n_mg_l", "氨氮(mg/L)"),
    ("total_phosphorus_mg_l", "总磷(mg/L)"),
    ("total_nitrogen_mg_l", "总氮(mg/L)"),
    ("chlorophyll_a_mg_l", "叶绿素α(mg/L)"),
    ("algae_density_cells_l", "藻密度(cells/L)"),
    ("station_status", "站点情况"),
]

NUMERIC_METRICS = [
    item for item in DISPLAY_METRICS if item[0] not in {"water_quality_class", "station_status"}
]

PAGE_SIZE_OPTIONS = [50, 100, 200, 500]
EXPORT_LIMIT = 5000
SERIES_LIMIT = 400
WATER_QUALITY_CLASSES = ["Ⅰ", "Ⅱ", "Ⅲ", "Ⅳ", "Ⅴ", "劣Ⅴ"]

def _open_session() -> Session:
    return Session(ENGINE)


def _parse_date(value: Optional[str], default_time: time) -> Optional[datetime]:
    if not value:
        return None
    try:
        date_part = datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return None
    return datetime.combine(date_part.date(), default_time)


def _prepare_rows(result_rows) -> List[Dict[str, object]]:
    prepared: List[Dict[str, object]] = []
    for province, city, basin, station_name, observed_at, payload_text in result_rows:
        try:
            payload = json.loads(payload_text) if payload_text else {}
        except json.JSONDecodeError:
            payload = {}

        metrics = {key: payload.get(key) for key, _ in DISPLAY_METRICS}
        prepared.append(
            {
                "province": province or "",
                "city": city or "",
                "basin": basin or "",
                "station_name": station_name,
                "observed_at": observed_at.strftime("%Y-%m-%d %H:%M")
                if observed_at
                else "",
                "metrics": metrics,
            }
        )
    return prepared


def _build_conditions(
    province: Optional[str],
    basin: Optional[str],
    city: Optional[str],
    station_keyword: Optional[str],
    water_quality_class: Optional[str],
    start_dt: Optional[datetime],
    end_dt: Optional[datetime],
    metric_filter: Optional[str],
    min_value: Optional[float],
    max_value: Optional[float],
    non_null: bool,
):
    conditions = []
    if province:
        conditions.append(Station.province == province)
    if basin:
        conditions.append(Station.basin == basin)
    if city:
        conditions.append(Station.city == city)
    if station_keyword:
        conditions.append(Station.station_name.contains(station_keyword))
    if water_quality_class:
        conditions.append(
            func.json_extract(Reading.payload, "$.water_quality_class") == water_quality_class
        )
    if metric_filter:
        metric_expr = cast(func.json_extract(Reading.payload, f"$.{metric_filter}"), Float)
        if non_null:
            conditions.append(metric_expr.isnot(None))
        if min_value is not None:
            conditions.append(metric_expr >= min_value)
        if max_value is not None:
            conditions.append(metric_expr <= max_value)
    if start_dt:
        conditions.append(Reading.observed_at >= start_dt)
    if end_dt:
        conditions.append(Reading.observed_at <= end_dt)
    return conditions


def _parse_float(value: Optional[str]) -> Optional[float]:
    if value in (None, "", " "):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _base_select() -> Select:
    return select(
        Station.province,
        Station.city,
        Station.basin,
        Station.station_name,
        Reading.observed_at,
        Reading.payload,
    ).join(Reading, Reading.station_id == Station.id)


def _build_query_params(params: Dict[str, Optional[object]]) -> str:
    clean = {
        key: value
        for key, value in params.items()
        if value not in (None, "", 0) and value != []
    }
    return urlencode(clean, doseq=True)


def _collect_filter_options(session: Session):
    provinces = [
        row[0]
        for row in session.execute(
            select(Station.province)
            .where(Station.province.isnot(None), Station.province != "")
            .distinct()
            .order_by(Station.province)
        )
        if row[0]
    ]
    basins = [
        row[0]
        for row in session.execute(
            select(Station.basin)
            .where(Station.basin.isnot(None), Station.basin != "")
            .distinct()
            .order_by(Station.basin)
        )
        if row[0]
    ]
    return provinces, basins


def _collect_cities(session: Session, province: Optional[str] = None) -> List[str]:
    query = select(Station.city).where(Station.city.isnot(None), Station.city != "")
    if province:
        query = query.where(Station.province == province)
    rows = session.execute(query.distinct().order_by(Station.city)).all()
    return [row[0] for row in rows if row[0]]


def _compute_class_distribution(session: Session, conditions):
    json_path = "$.water_quality_class"
    stmt = (
        select(func.json_extract(Reading.payload, json_path).label("cls"), func.count())
        .join(Station, Reading.station_id == Station.id)
        .where(*conditions)
        .group_by("cls")
    )
    rows = session.execute(stmt).all()
    total = sum(row[1] for row in rows if row[0])
    distribution = []
    for label, count in rows:
        name = (label or "其它").strip()
        percent = 0 if total == 0 else round(count / total * 100, 1)
        distribution.append({"label": name, "value": count, "percent": percent})
    order_map = {name: idx for idx, name in enumerate(WATER_QUALITY_CLASSES)}
    distribution.sort(key=lambda item: order_map.get(item["label"], 999))
    return distribution, total


@app.get("/", response_class=HTMLResponse)
def index(
    request: Request,
    province: Optional[str] = Query(default=None, description="省份"),
    basin: Optional[str] = Query(default=None, description="流域"),
    city: Optional[str] = Query(default=None, description="城市"),
    station_keyword: Optional[str] = Query(default=None, description="断面关键词"),
    water_quality_class: Optional[str] = Query(default=None, description="水质类别"),
    start_date: Optional[str] = Query(default=None, description="开始日期 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(default=None, description="结束日期 (YYYY-MM-DD)"),
    metric_filter: Optional[str] = Query(default=None, description="数值字段筛选"),
    min_value: Optional[str] = Query(default=None, description="最小值"),
    max_value: Optional[str] = Query(default=None, description="最大值"),
    non_null: bool = Query(False, description="只看非空"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(100, ge=10, le=500, description="每页数量"),
):
    session = _open_session()
    try:
        provinces, basins = _collect_filter_options(session)
        cities = _collect_cities(session, province)

        start_dt = _parse_date(start_date, time.min)
        end_dt = _parse_date(end_date, time.max)

        min_value_parsed = _parse_float(min_value)
        max_value_parsed = _parse_float(max_value)

        conditions = _build_conditions(
            province,
            basin,
            city,
            station_keyword,
            water_quality_class,
            start_dt,
            end_dt,
            metric_filter,
            min_value_parsed,
            max_value_parsed,
            non_null,
        )

        count_stmt = select(func.count()).select_from(Reading).join(
            Station, Reading.station_id == Station.id
        )
        for condition in conditions:
            count_stmt = count_stmt.where(condition)
        total = session.execute(count_stmt).scalar_one()

        page_size = page_size if page_size in PAGE_SIZE_OPTIONS else PAGE_SIZE_OPTIONS[1]
        total_pages = max(1, math.ceil(total / page_size))
        page = min(page, total_pages)
        offset = (page - 1) * page_size

        data_stmt = _base_select().where(*conditions).order_by(Reading.observed_at.desc())
        data_stmt = data_stmt.offset(offset).limit(page_size)
        rows = session.execute(data_stmt).all()
        prepared_rows = _prepare_rows(rows)
    finally:
        session.close()

    base_params = {
        "province": province or "",
        "basin": basin or "",
        "city": city or "",
        "station_keyword": station_keyword or "",
        "water_quality_class": water_quality_class or "",
        "start_date": start_date or "",
        "end_date": end_date or "",
        "metric_filter": metric_filter or "",
        "min_value": min_value if min_value is not None else "",
        "max_value": max_value if max_value is not None else "",
        "non_null": int(non_null),
        "page_size": page_size,
    }
    export_query = _build_query_params(base_params)
    prev_query = _build_query_params({**base_params, "page": page - 1}) if page > 1 else ""
    next_query = (
        _build_query_params({**base_params, "page": page + 1})
        if page < total_pages
        else ""
    )

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "rows": prepared_rows,
            "metrics": DISPLAY_METRICS,
            "provinces": provinces,
            "basins": basins,
            "cities": cities,
            "selected_filters": {
                "province": province or "",
                "basin": basin or "",
                "city": city or "",
                "station_keyword": station_keyword or "",
                "water_quality_class": water_quality_class or "",
                "start_date": start_date or "",
                "end_date": end_date or "",
                "page": page,
                "page_size": page_size,
            },
            "row_count": len(prepared_rows),
            "total": total,
            "page": page,
            "total_pages": total_pages,
            "page_size_options": PAGE_SIZE_OPTIONS,
            "prev_query": prev_query,
            "next_query": next_query,
            "export_query": export_query,
            "water_quality_classes": WATER_QUALITY_CLASSES,
            "chart_metrics": NUMERIC_METRICS,
            "selected_metric_filter": metric_filter or "",
            "min_value": "" if min_value is None else min_value,
            "max_value": "" if max_value is None else max_value,
            "non_null": non_null,
            "active_tab": "table",
        },
    )


def _fetch_series(session: Session, conditions, metric: str):
    stmt = (
        select(Reading.observed_at, Reading.payload)
        .join(Station, Reading.station_id == Station.id)
        .where(*conditions)
        .order_by(Reading.observed_at)
        .limit(SERIES_LIMIT)
    )
    rows = session.execute(stmt).all()
    grouped: Dict[datetime, List[float]] = {}
    for observed_at, payload_text in rows:
        if not payload_text:
            continue
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            continue
        value = payload.get(metric)
        if value is None:
            continue
        numeric_value = parse_numeric(str(value))
        if numeric_value is None:
            continue
        grouped.setdefault(observed_at, []).append(numeric_value)

    series = []
    for timestamp in sorted(grouped.keys()):
        values = grouped[timestamp]
        if not values:
            continue
        avg = sum(values) / len(values)
        series.append(
            {
                "time": timestamp.strftime("%Y-%m-%d %H:%M") if timestamp else "",
                "value": round(avg, 3),
            }
        )
    return series


@app.get("/charts", response_class=HTMLResponse)
def charts(
    request: Request,
    province: Optional[str] = Query(default=None),
    basin: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    station_keyword: Optional[str] = Query(default=None),
    water_quality_class: Optional[str] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    metric: Optional[str] = Query(None),
):
    session = _open_session()
    try:
        provinces, basins = _collect_filter_options(session)
        cities = _collect_cities(session, province)

        start_dt = _parse_date(start_date, time.min)
        end_dt = _parse_date(end_date, time.max)

        conditions = _build_conditions(
            province, basin, city, station_keyword, water_quality_class, start_dt, end_dt
        )

        class_distribution, distribution_total = _compute_class_distribution(session, conditions)
        metric_keys = [key for key, _ in NUMERIC_METRICS]
        if not metric or metric not in metric_keys:
            metric = metric_keys[0]
        series = _fetch_series(session, conditions, metric)
    finally:
        session.close()

    selected_filters = {
        "province": province or "",
        "basin": basin or "",
        "city": city or "",
        "station_keyword": station_keyword or "",
        "water_quality_class": water_quality_class or "",
        "start_date": start_date or "",
        "end_date": end_date or "",
        "page_size": PAGE_SIZE_OPTIONS[1],
    }

    return templates.TemplateResponse(
        "charts.html",
        {
            "request": request,
            "provinces": provinces,
            "basins": basins,
            "cities": cities,
            "selected_filters": selected_filters,
            "water_quality_classes": WATER_QUALITY_CLASSES,
            "metrics": DISPLAY_METRICS,
            "chart_metrics": NUMERIC_METRICS,
            "selected_metric": metric,
            "metric_label": dict(NUMERIC_METRICS).get(metric, metric),
            "selected_metric_filter": metric_filter or "",
            "min_value": "" if min_value is None else min_value,
            "max_value": "" if max_value is None else max_value,
            "non_null": non_null,
            "class_distribution_json": json.dumps(class_distribution, ensure_ascii=False),
            "distribution_total": distribution_total,
            "series_data_json": json.dumps(series, ensure_ascii=False),
            "active_tab": "charts",
        },
    )


@app.get("/export")
def export_csv(
    province: Optional[str] = Query(default=None, description="省份"),
    basin: Optional[str] = Query(default=None, description="流域"),
    city: Optional[str] = Query(default=None, description="城市"),
    station_keyword: Optional[str] = Query(default=None, description="断面关键词"),
    water_quality_class: Optional[str] = Query(default=None, description="水质类别"),
    start_date: Optional[str] = Query(default=None, description="开始日期 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(default=None, description="结束日期 (YYYY-MM-DD)"),
    metric_filter: Optional[str] = Query(default=None, description="数值字段筛选"),
    min_value: Optional[str] = Query(default=None, description="最小值"),
    max_value: Optional[str] = Query(default=None, description="最大值"),
    non_null: bool = Query(False, description="只看非空"),
):
    session = _open_session()
    try:
        start_dt = _parse_date(start_date, time.min)
        end_dt = _parse_date(end_date, time.max)

        min_value_parsed = _parse_float(min_value)
        max_value_parsed = _parse_float(max_value)

        conditions = _build_conditions(
            province,
            basin,
            city,
            station_keyword,
            water_quality_class,
            start_dt,
            end_dt,
            metric_filter,
            min_value_parsed,
            max_value_parsed,
            non_null,
        )

        stmt = (
            _base_select()
            .where(*conditions)
            .order_by(Reading.observed_at.desc())
            .limit(EXPORT_LIMIT)
        )
        rows = session.execute(stmt).all()
        prepared_rows = _prepare_rows(rows)
    finally:
        session.close()

    output = io.StringIO()
    writer = csv.writer(output)
    header = ["省份", "城市", "流域", "断面名称", "监测时间"] + [label for _, label in DISPLAY_METRICS]
    writer.writerow(header)
    for row in prepared_rows:
        row_values = [
            row["province"],
            row["city"],
            row["basin"],
            row["station_name"],
            row["observed_at"],
        ]
        for key, _ in DISPLAY_METRICS:
            row_values.append(row["metrics"].get(key))
        writer.writerow(row_values)

    output.seek(0)
    filename = "water_quality_export.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
