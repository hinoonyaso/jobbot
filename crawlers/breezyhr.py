import re
from datetime import datetime
from typing import Any, Dict, List

from crawlers.common import get_render_policy, parse_title_description, request_with_retry, search_multi_domains

DOMAINS = ["breezy.hr", "bearrobotics.breezy.hr"]
JOB_URL_RE = re.compile(r"https?://[a-z0-9\-]+\.breezy\.hr/p/[a-zA-Z0-9\-_]+", re.I)


def _valid(url: str) -> bool:
    return bool(JOB_URL_RE.match(url.strip()))


def _fetch_with_playwright(render: Dict[str, Any], logger) -> List[str]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return []

    urls: List[str] = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                locale="ko-KR",
            )
            page = ctx.new_page()
            page.goto("https://bearrobotics.breezy.hr/", wait_until=render["wait_until"], timeout=render["timeout_ms"])
            page.wait_for_timeout(3000)
            for _ in range(render["scroll_rounds"]):
                page.mouse.wheel(0, 3000)
                page.wait_for_timeout(1000)
            hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
            for href in hrefs or []:
                if isinstance(href, str) and _valid(href):
                    urls.append(href)
            # Also extract from page HTML
            if not urls:
                content = page.content()
                urls = list(dict.fromkeys(JOB_URL_RE.findall(content)))
            browser.close()
    except Exception as exc:
        logger.info("source=breezyhr playwright failed err=%s", exc)
    return list(dict.fromkeys(urls))


def fetch_list(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))
    render = get_render_policy(opts, default_timeout_ms=30000, default_scroll_rounds=2)

    # 1) Playwright first (breezy.hr renders job listings with JS)
    urls: List[str] = []
    if render["enabled"]:
        urls = _fetch_with_playwright(render, logger)
        logger.info("source=breezyhr playwright links=%d", len(urls))

    # 2) HTTP fallback
    if not urls:
        board_resp = request_with_retry("GET", "https://bearrobotics.breezy.hr/", timeout, retries, logger)
        if board_resp:
            urls = list(dict.fromkeys(JOB_URL_RE.findall(board_resp.text)))

    # 3) Search fallback
    if not urls:
        keyword = opts.get("query", {}).get("keyword", "bear robotics software engineer")
        urls = [u for u in search_multi_domains(DOMAINS, keyword, timeout, retries, logger, cfg.get("search", {})) if _valid(u)]

    if not urls:
        urls = ["https://bearrobotics.breezy.hr/"]

    max_items = int(opts.get("max_items", 10))
    return [
        {
            "source_job_id": u.rsplit("/", 1)[-1].split("?", 1)[0],
            "url": u,
            "posted_at": datetime.now().strftime("%Y-%m-%d"),
            "title": "BearRobotics Position",
            "company": "베어로보틱스",
            "location": "미상",
        }
        for u in urls[:max_items]
    ]


def fetch_detail(item: Dict[str, Any], opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> Dict[str, Any]:
    url = item.get("url", "")
    render = get_render_policy(opts, default_timeout_ms=20000, default_scroll_rounds=1)

    page_html = ""
    if render["enabled"]:
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                ctx = browser.new_context(
                    user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                    locale="ko-KR",
                )
                pg = ctx.new_page()
                pg.goto(url, wait_until="domcontentloaded", timeout=render["timeout_ms"])
                pg.wait_for_timeout(2000)
                page_html = pg.content()
                browser.close()
        except Exception as exc:
            logger.info("source=breezyhr detail playwright failed url=%s err=%s", url, exc)

    if not page_html:
        timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
        retries = int(cfg.get("network", {}).get("retry", 2))
        resp = request_with_retry("GET", url, timeout, retries, logger)
        if resp:
            page_html = resp.text

    if not page_html:
        return {}

    parsed = parse_title_description(page_html)
    blob = f"{parsed.get('title','')} {parsed.get('description','')}"
    return {
        "title": parsed.get("title", item.get("title", "")),
        "description": parsed.get("description", ""),
        "employment_type": "인턴" if "intern" in blob.lower() or "인턴" in blob else "정규직",
        "status_text": "Open",
    }
