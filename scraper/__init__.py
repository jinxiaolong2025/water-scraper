"""
Water quality scraper package.

The modules expose:
    - selectors: Page selectors and scraping configuration.
    - parser: Utilities to normalize tabular data into typed rows.
    - storage: SQLAlchemy models and database helpers.
    - browser: Playwright helpers for consistent browser automation.
    - job: End-to-end scraping workflow orchestrating the above pieces.
"""

__all__ = ["selectors", "parser", "storage", "browser", "job"]
