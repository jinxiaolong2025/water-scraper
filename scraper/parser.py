"""
Utilities for parsing water quality tables into normalized Python objects.

The functions here focus on:
    - Mapping raw Chinese headers onto stable internal field names.
    - Normalizing numeric values (treating placeholders such as 9999 as NULL).
    - Parsing timestamps into timezone-aware datetimes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:  # pragma: no cover - fallback for older runtimes
    from pytz import timezone as ZoneInfo  # type: ignore

from dateutil import parser as date_parser

RAW_TO_FIELD = {
    "省份": "province",
    "城市": "city",
    "流域": "basin",
    "河流": "river",
    "断面": "station_name",
    "断面名称": "station_name",
    "断面编码": "station_code",
    "监测时间": "observed_at",
    "测站": "station_name",
    "站点": "station_name",
    "站点名称": "station_name",
    "监测点": "station_name",
    "水质类别": "water_quality_class",
    "站点情况": "station_status",
    "水温(℃)": "water_temperature_c",
    "pH(无量纲)": "ph",
    "溶解氧(mg/L)": "dissolved_oxygen_mg_l",
    "电导率(μS/cm)": "conductivity_us_cm",
    "浊度(NTU)": "turbidity_ntu",
    "高锰酸盐指数(mg/L)": "permanganate_index_mg_l",
    "氨氮(mg/L)": "ammonia_n_mg_l",
    "总磷(mg/L)": "total_phosphorus_mg_l",
    "总氮(mg/L)": "total_nitrogen_mg_l",
    "叶绿素α(mg/L)": "chlorophyll_a_mg_l",
    "藻密度(cells/L)": "algae_density_cells_l",
}

NULL_TOKENS = {"", "-", "—", "--", "——", "null", "NULL", "9999", "NaN"}

STATION_TEXT_FIELDS = {
    "province",
    "city",
    "basin",
    "river",
    "station_name",
    "station_code",
}

READING_TEXT_FIELDS = {
    "water_quality_class",
    "station_status",
}


@dataclass
class ParsedRow:
    """
    Structured representation of a row parsed from the table.

    Standard site metadata fields are kept as strings, while metrics are
    stored as floats. `extra_metrics` captures any column not recognized as a
    standard field so downstream code can decide how to store them.
    """

    station: Dict[str, Optional[str]]
    reading: Dict[str, Optional[object]]
    extra_metrics: Dict[str, Optional[float]]


def build_header_mapping(headers: Iterable[str]) -> List[str]:
    """
    Map raw header labels to normalized field keys.

    Unknown labels are returned unchanged so they can be treated as metric
    names during row parsing.
    """
    normalized: List[str] = []
    for header in headers:
        header = header.strip().replace("\n", "")
        normalized.append(RAW_TO_FIELD.get(header, header))
    return normalized


def parse_timestamp(value: str, tz_name: str) -> Optional[datetime]:
    """Parse timestamps into the requested timezone."""
    value = value.strip()
    if value in NULL_TOKENS:
        return None

    dt = date_parser.parse(value, fuzzy=True)
    if dt.tzinfo is None:
        tz = ZoneInfo(tz_name) if isinstance(ZoneInfo, type) else ZoneInfo(tz_name)
        dt = dt.replace(tzinfo=tz)
    else:
        tz = ZoneInfo(tz_name) if isinstance(ZoneInfo, type) else ZoneInfo(tz_name)
        dt = dt.astimezone(tz)
    return dt


def parse_numeric(value: str) -> Optional[float]:
    """Convert raw metric strings into floats with NULL token handling."""
    raw = value.strip()
    if raw in NULL_TOKENS:
        return None
    try:
        return float(raw.replace(",", ""))
    except ValueError:
        return None


def parse_row(
    headers: Sequence[str],
    cells: Sequence[str],
    tz_name: str,
    extras: Optional[Dict[str, Optional[str]]] = None,
) -> ParsedRow:
    """
    Convert a single row of raw text into structured station and reading data.

    Parameters
    ----------
    headers
        The normalized headers produced by `build_header_mapping`.
    cells
        Column values aligned with `headers`.
    tz_name
        Target timezone; the observed_at timestamp is converted here.
    """
    station_data: Dict[str, Optional[str]] = {}
    reading_data: Dict[str, Optional[object]] = {}
    extra: Dict[str, Optional[float]] = {}

    for header, value in zip(headers, cells):
        if header == "observed_at":
            reading_data["observed_at"] = parse_timestamp(value, tz_name)
        elif header in STATION_TEXT_FIELDS:
            station_data[header] = value.strip() or None
        elif header in READING_TEXT_FIELDS:
            reading_data[header] = value.strip() or None
        else:
            extra[header] = parse_numeric(value)

    if extras:
        for key, value in extras.items():
            if key in STATION_TEXT_FIELDS:
                station_data[key] = value
            elif key in READING_TEXT_FIELDS:
                reading_data[key] = value
    reading_data.update(extra)
    return ParsedRow(station=station_data, reading=reading_data, extra_metrics=extra)
