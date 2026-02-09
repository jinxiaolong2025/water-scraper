"""
Selectors configuration for the national water quality scraper.

Pydantic models are used so that any missing or malformed selector simply
raises a validation error, making it easier to spot typos early. Replace the
placeholder values below with the actual selectors once you confirm them in
Chrome DevTools.
"""

from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field, validator

ScrollMode = Literal["none", "infinite_scroll", "load_more"]


class TableSelectors(BaseModel):
    """
    Selectors used to isolate the data table.

    Every field defaults to a placeholder string. Update them once you have
    inspected the live page. Keep selectors short and resilient (prefer data
    attributes over brittle `nth-child` selectors when possible).
    """

    table_container: str = Field(
        "div.table-container-placeholder",
        description="CSS selector that wraps both headers and rows.",
    )
    header_cells: str = Field(
        "table thead tr th",
        description="Selector that matches the header cells; text is used for column mapping.",
    )
    data_rows: str = Field(
        "table tbody tr",
        description="Selector that returns each data row element.",
    )
    cell_selector: Optional[str] = Field(
        default="td",
        description="Selector for cells inside a row when each column shares the same structure.",
    )
    column_overrides: Dict[str, str] = Field(
        default_factory=dict,
        description="Optional per-header overrides when certain cells require special selectors.",
    )

    @validator("table_container", "header_cells", "data_rows", pre=True, always=True)
    def _strip_strings(cls, value: str) -> str:
        """Normalize accidental whitespace in selector definitions."""
        if isinstance(value, str):
            return value.strip()
        return value


class ScrollSettings(BaseModel):
    """
    Pagination controls for infinite scroll or click-to-load pages.

    Choose exactly one mode per page configuration. When using load-more,
    ensure `load_more_button` is populated. For infinite scroll, provide the
    scrollable container (use 'body' if the entire page scrolls).
    """

    mode: ScrollMode = Field(
        "none",
        description="Pagination mode: 'none', 'infinite_scroll', or 'load_more'.",
    )
    container: Optional[str] = Field(
        default=None,
        description="Scrollable element selector used for infinite scroll (optional for load-more).",
    )
    load_more_button: Optional[str] = Field(
        default=None,
        description="Button selector that triggers additional rows when mode='load_more'.",
    )
    max_iterations: int = Field(
        10,
        description="Safety cap to prevent endless scroll loops.",
    )
    wait_for_ms: int = Field(
        800,
        description="Delay between scroll/load actions to allow the table to settle.",
    )


class PageSelectors(BaseModel):
    """
    Top-level selectors for a single page harvest.

    Attributes
    ----------
    url:
        Page URL to visit. Must be reachable without authentication.
    iframe_chain:
        Ordered list of selectors the scraper uses to enter nested iframes.
        Leave empty when the table is in the top-level document.
    table:
        `TableSelectors` instance describing the core table structure.
    scroll:
        Optional `ScrollSettings` instance configuring pagination.
    """

    url: str = Field("https://szzdjc.cnemc.cn:8070/GJZ/Business/Publish/Main.html", description="Target page URL.")
    iframe_chain: List[str] = Field(
        default_factory=list,
        description="Selectors for nested iframes (outermost first).",
    )
    table: TableSelectors = Field(default_factory=TableSelectors)
    scroll: ScrollSettings = Field(default_factory=ScrollSettings)


def get_default_page() -> PageSelectors:
    """
    Template selector set to be replaced with real values.

    Returns a configuration object describing how to reach the data table.
    """
    """
    Construct a page configuration with selectors tuned for the national water quality
    realtime data page.  The data grid sits inside the iframe whose id is `MF`.
    Headers render in a fixed table `#gridHd`, while rows stream inside the
    scrollable list `#gridDatas`.  Scrolling the `#div_gridBodys` container
    increments pagination.

    You should only need to adjust these values if the site layout changes.
    """
    return PageSelectors(
        # Landing page for the realtime data system
        url="https://szzdjc.cnemc.cn:8070/GJZ/Business/Publish/Main.html",
        # Enter the top-level data iframe before selecting elements
        iframe_chain=[
            "#MF",
        ],
        # Table structure definitions
        table=TableSelectors(
            table_container="body",
            header_cells="#gridHd tr td",
            data_rows="#gridDatas li tr",
            cell_selector="td",
            column_overrides={},
        ),
        # Scroll behaviour – the page loads data as you scroll, so treat it
        # as an infinite scroll on the body element.  We cap the number of
        # iterations to avoid endless loops.
        scroll=ScrollSettings(
            mode="infinite_scroll",
            container="#div_gridBodys",
            load_more_button=None,
            max_iterations=120,
            wait_for_ms=1200,
        ),
    )


def get_all_pages() -> List[PageSelectors]:
    """
    Return the list of pages to process. Start with a single page template.

    Expanding to multiple URLs later only requires appending additional
    configs.
    """
    return [get_default_page()]
