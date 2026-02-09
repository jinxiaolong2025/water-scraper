"""
Persistence layer built on SQLAlchemy for the water quality scraper.

Two tables are defined:
    - stations: static metadata about monitoring stations.
    - readings: time-series measurements for each station.

`upsert_reading` ensures idempotency by de-duplicating on (station_id, observed_at).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, Tuple

from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint, create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, declarative_base, relationship, sessionmaker

Base = declarative_base()


class Station(Base):
    __tablename__ = "stations"

    id = Column(Integer, primary_key=True)
    province = Column(String(64), nullable=True)
    city = Column(String(64), nullable=True)
    basin = Column(String(128), nullable=True)
    river = Column(String(128), nullable=True)
    station_name = Column(String(128), nullable=False)
    station_code = Column(String(64), nullable=True, unique=True)

    readings = relationship("Reading", back_populates="station", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint(
            "province",
            "city",
            "basin",
            "river",
            "station_name",
            name="uq_station_composite",
        ),
    )


class Reading(Base):
    __tablename__ = "readings"

    id = Column(Integer, primary_key=True)
    station_id = Column(Integer, ForeignKey("stations.id"), nullable=False)
    observed_at = Column(DateTime(timezone=True), nullable=False)
    batch_time = Column(DateTime(timezone=True), nullable=False)
    payload = Column(Text, nullable=True)  # JSON blob containing metrics and raw fields

    station = relationship("Station", back_populates="readings")

    __table_args__ = (
        Index("ix_unique_reading", "station_id", "observed_at", unique=True),
    )


def get_engine(database_path: str):
    """Create a SQLite engine, ensuring the parent directory is available."""
    os.makedirs(os.path.dirname(database_path), exist_ok=True)
    return create_engine(f"sqlite:///{database_path}", future=True)


def get_session_factory(database_path: str):
    engine = get_engine(database_path)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False, class_=Session)


@dataclass
class UpsertResult:
    station: Station
    reading: Optional[Reading]
    created: bool


def _json_ready_payload(payload: Dict[str, Optional[object]]) -> Dict[str, Optional[object]]:
    """
    Convert payload values into JSON-serializable primitives.

    Datetime objects are rendered as ISO8601 strings so that they can be stored
    inside the readings payload column without serialization errors.
    """
    prepared: Dict[str, Optional[object]] = {}
    for key, value in payload.items():
        if isinstance(value, datetime):
            prepared[key] = value.isoformat()
        else:
            prepared[key] = value
    return prepared


def upsert_station(session: Session, station_data: Dict[str, Optional[str]]) -> Station:
    """
    Find or create a station record based on station_code when available,
    otherwise falling back to the composite unique constraint.
    """
    station_code = station_data.get("station_code")
    query = session.query(Station)
    if station_code:
        instance = query.filter(Station.station_code == station_code).one_or_none()
        if instance:
            _merge_station_values(instance, station_data)
            return instance

    filters = {
        "province": station_data.get("province"),
        "city": station_data.get("city"),
        "basin": station_data.get("basin"),
        "river": station_data.get("river"),
        "station_name": station_data.get("station_name"),
    }
    instance = query.filter_by(**filters).one_or_none()
    if instance:
        _merge_station_values(instance, station_data)
        return instance

    # API fallback: city metadata can be missing in one run and available in a
    # later run. Reuse a unique fuzzy match to avoid duplicate station rows.
    fuzzy = _find_station_fuzzy_candidate(query, station_data)
    if fuzzy:
        _merge_station_values(fuzzy, station_data)
        return fuzzy

    instance = Station(**station_data)
    session.add(instance)
    return instance


def _is_blank(value: Optional[str]) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _merge_station_values(instance: Station, station_data: Dict[str, Optional[str]]) -> None:
    """
    Update station fields without overwriting existing non-empty values with blanks.
    """
    for key, value in station_data.items():
        if _is_blank(value):
            continue
        setattr(instance, key, value)


def _find_station_fuzzy_candidate(query, station_data: Dict[str, Optional[str]]) -> Optional[Station]:
    candidates = (
        query.filter(
            Station.province == station_data.get("province"),
            Station.basin == station_data.get("basin"),
            Station.river == station_data.get("river"),
            Station.station_name == station_data.get("station_name"),
        ).all()
    )
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]

    target_city = station_data.get("city")
    if not _is_blank(target_city):
        exact = [item for item in candidates if not _is_blank(item.city) and item.city == target_city]
        if len(exact) == 1:
            return exact[0]
        blanks = [item for item in candidates if _is_blank(item.city)]
        if len(blanks) == 1 and len(exact) == 0:
            return blanks[0]
        return None

    non_blank = [item for item in candidates if not _is_blank(item.city)]
    if len(non_blank) == 1:
        return non_blank[0]
    return None


def upsert_reading(
    session: Session,
    station: Station,
    reading_payload: Dict[str, Optional[object]],
    batch_time: datetime,
) -> Tuple[Optional[Reading], bool]:
    """
    Insert or update a reading for the given station.

    Returns None if the row already exists and no update was required.
    """
    observed_at = reading_payload.get("observed_at")
    if observed_at is None:
        return None, False

    payload_copy = dict(reading_payload)
    payload_copy["batch_time"] = batch_time.isoformat()
    payload_copy = _json_ready_payload(payload_copy)

    existing = (
        session.query(Reading)
        .filter(Reading.station_id == station.id, Reading.observed_at == observed_at)
        .one_or_none()
    )

    if existing:
        existing.batch_time = batch_time
        existing.payload = json.dumps(payload_copy, ensure_ascii=False)
        return existing, False

    reading = Reading(
        station=station,
        observed_at=observed_at,
        batch_time=batch_time,
        payload=json.dumps(payload_copy, ensure_ascii=False),
    )
    session.add(reading)
    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        return (
            session.query(Reading)
            .filter(Reading.station_id == station.id, Reading.observed_at == observed_at)
            .one_or_none()
        ), False
    return reading, True


def upsert_row(
    session: Session,
    station_data: Dict[str, Optional[str]],
    reading_payload: Dict[str, Optional[object]],
    batch_time: datetime,
) -> UpsertResult:
    """Convenience helper used by the scraping job."""
    station = upsert_station(session, station_data)
    reading, created = upsert_reading(session, station, reading_payload, batch_time)
    return UpsertResult(station=station, reading=reading, created=created)
