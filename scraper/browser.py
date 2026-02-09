"""
Playwright utilities used by the scraping job.

The functions here wrap common tasks such as launching a browser session,
navigating into nested iframes, and triggering pagination behavior. The
actual selectors come from `selectors.py`.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional, Union

from playwright.sync_api import (
    Browser,
    BrowserContext,
    Error as PlaywrightError,
    Frame,
    Page,
    TimeoutError as PlaywrightTimeout,
    sync_playwright,
)

from .selectors import PageSelectors, ScrollSettings


@dataclass
class BrowserConfig:
    headless: bool = True
    timeout_ms: int = 15_000


@contextmanager
def browser_page(config: BrowserConfig) -> Page:
    """
    Context manager yielding a single Playwright page.

    Closes all resources automatically, even if an exception bubbles up.
    """
    playwright = sync_playwright().start()
    browser: Optional[Browser] = None
    context: Optional[BrowserContext] = None
    try:
        # 使用 "new" headless 模式，更难被网站检测
        launch_args = [
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-extensions",
            "--disable-software-rasterizer",
            "--disable-blink-features=AutomationControlled",  # 隐藏自动化标识
        ]
        
        # 检测系统级 Chromium 路径（Linux 服务器用）
        import os
        import shutil
        executable_path = None
        system_chromium_paths = [
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
            "/usr/bin/google-chrome",
            "/usr/bin/google-chrome-stable",
        ]
        for path in system_chromium_paths:
            if os.path.exists(path):
                executable_path = path
                break
        
        browser = playwright.chromium.launch(
            headless=config.headless,
            args=launch_args,
            executable_path=executable_path,  # None 时使用 Playwright 自带的
        )
        
        # 模拟真实浏览器环境
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
        )
        
        # 注入脚本隐藏 webdriver 属性
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        """)
        
        page = context.new_page()
        page.set_default_timeout(config.timeout_ms)
        yield page
    finally:
        if context is not None:
            context.close()
        if browser is not None:
            browser.close()
        playwright.stop()


def navigate_to_table(page: Page, config: PageSelectors) -> Union[Page, Frame]:
    """
    Navigate to the page URL, optionally drilling into nested iframes.

    Returns the page or frame that contains the table selectors.
    """
    page.goto(config.url)

    current: Union[Page, Frame] = page
    for selector in config.iframe_chain:
        frame_element = current.wait_for_selector(selector)
        if frame_element is None:
            raise PlaywrightError(f"Iframe selector not found: {selector}")
        current_frame = frame_element.content_frame()
        if current_frame is None:
            raise PlaywrightError(f"Unable to resolve iframe: {selector}")
        current = current_frame
    return current


def perform_scroll(target: Union[Page, Frame], scroll: ScrollSettings) -> None:
    """Trigger pagination behavior based on the configured scroll mode."""
    if scroll.mode == "none":
        return

    if scroll.mode == "infinite_scroll":
        _perform_infinite_scroll(target, scroll)
    elif scroll.mode == "load_more":
        _perform_load_more(target, scroll)
    else:
        raise ValueError(f"Unsupported scroll mode: {scroll.mode}")


def _perform_infinite_scroll(page: Union[Page, Frame], scroll: ScrollSettings) -> None:
    container_selector = scroll.container
    if not container_selector:
        container_selector = "body"

    last_height = -1
    last_rows = -1
    stable_rounds = 0

    def _count_rows() -> int:
        try:
            return page.locator("#gridDatas li").count()
        except PlaywrightError:
            return -1

    for _ in range(scroll.max_iterations):
        try:
            page.eval_on_selector(
                container_selector,
                "(el) => { el.scrollTo(0, el.scrollHeight); el.dispatchEvent(new Event('scroll', { bubbles: true })); }",
            )
        except PlaywrightError:
            # Fall back to top-level page scroll when container scroll fails.
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")

        page.wait_for_timeout(scroll.wait_for_ms)
        try:
            height = page.eval_on_selector(
                container_selector,
                "(el) => el.scrollHeight",
            )
        except PlaywrightError:
            height = page.evaluate("document.body.scrollHeight")

        rows = _count_rows()
        if height == last_height and rows == last_rows:
            stable_rounds += 1
        else:
            stable_rounds = 0

        # Require several stable rounds to reduce early-stop on slow updates.
        if stable_rounds >= 3:
            break

        last_height = height
        last_rows = rows


def _perform_load_more(page: Page, scroll: ScrollSettings) -> None:
    if not scroll.load_more_button:
        raise ValueError("load_more mode requires load_more_button selector")

    for _ in range(scroll.max_iterations):
        try:
            button = page.wait_for_selector(scroll.load_more_button, timeout=1000)
        except PlaywrightTimeout:
            break

        if button is None or not button.is_enabled():
            break

        button.click()
        page.wait_for_timeout(scroll.wait_for_ms)
        # In case the button disappears after loading finishes
        try:
            page.wait_for_selector(scroll.load_more_button, timeout=500)
        except PlaywrightTimeout:
            break
