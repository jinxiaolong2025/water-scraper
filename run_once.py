"""Entry point for performing a single scrape run."""

import logging
import sqlite3
from pathlib import Path

from scraper.job import SelectorValidationError, run_once


def _count_rows(database_path: str) -> dict:
    if not Path(database_path).exists():
        return {"stations": 0, "readings": 0}
    with sqlite3.connect(database_path) as conn:
        cursor = conn.cursor()
        counts = {}
        for table in ("stations", "readings"):
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = cursor.fetchone()[0]
            except sqlite3.OperationalError:
                counts[table] = 0
        return counts


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        stats = run_once()
    except SelectorValidationError as exc:
        logging.error("抓取失败: %s", exc)
        logging.info("修正建议: %s", exc.suggestion)
        return

    logging.info(
        "Scraping finished: pages=%s rows_seen=%s rows_inserted=%s",
        stats.pages_processed,
        stats.rows_seen,
        stats.rows_inserted,
    )

    print(f"Total rows detected: {stats.rows_seen}")
    counts = _count_rows(stats.database_path)
    print(f"SQLite path: {stats.database_path}")
    print(
        "Table counts -> stations: {stations}, readings: {readings}".format(
            stations=counts.get("stations", 0),
            readings=counts.get("readings", 0),
        )
    )
    if stats.snapshots:
        latest_snapshot = stats.snapshots[-1]
        print(f"Latest snapshot saved to: {latest_snapshot}")


if __name__ == "__main__":
    main()
