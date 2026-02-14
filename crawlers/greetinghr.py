import re
from datetime import datetime
from typing import Any, Dict, List

from crawlers.common import get_render_policy, parse_title_description, request_with_retry, search_multi_domains

DOMAINS = ["greetinghr.com"]
JOB_URL_RE = re.compile(r"https?://[a-z0-9\-.]*greetinghr\.com/(?:o|jobs?|positions?|job)/[a-zA-Z0-9\-_/]+", re.I)


def _valid(url: str) -> bool:
    return bool(JOB_URL_RE.match(url.strip()))


def _fetch_with_playwright(keyword: str, render: Dict[str, Any], logger) -> List[str]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return []

    urls: List[str] = []
    # Try multiple entry points
    start_urls = [
        "https://www.greetinghr.com/",
        f"https://www.greetinghr.com/search?keyword={keyword}",
    ]

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                locale="ko-KR",
            )

            for start_url in start_urls:
                if urls:
                    break
                try:
                    page = ctx.new_page()
                    page.goto(start_url, wait_until=render["wait_until"], timeout=render["timeout_ms"])
                    page.wait_for_timeout(2000)
                    for _ in range(render["scroll_rounds"]):
                        page.mouse.wheel(0, 3000)
                        page.wait_for_timeout(1000)
                    hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
                    for href in hrefs or []:
                        if isinstance(href, str) and "greetinghr.com" in href.lower():
                            if _valid(href):
                                urls.append(href)
                            elif "/o/" in href or "/job" in href:
                                urls.append(href)
                    page.close()
                except Exception as exc:
                    logger.info("source=greetinghr playwright page failed url=%s err=%s", start_url, exc)

            browser.close()
    except Exception as exc:
        logger.info("source=greetinghr playwright failed err=%s", exc)
    return list(dict.fromkeys(urls))


def fetch_list(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))
    keyword = opts.get("query", {}).get("keyword", "로봇")
    render = get_render_policy(opts, default_timeout_ms=25000, default_scroll_rounds=2)

    # 1) Playwright first
    urls: List[str] = []
    if render["enabled"]:
        urls = _fetch_with_playwright(keyword, render, logger)
        logger.info("source=greetinghr playwright links=%d", len(urls))

    # 2) HTTP fallback
    if not urls:
        resp = request_with_retry("GET", "https://www.greetinghr.com/", timeout, retries, logger)
        if resp:
            for m in JOB_URL_RE.findall(resp.text):
                urls.append(m)
            urls = list(dict.fromkeys(urls))

    # 3) Search fallback
    if not urls:
        urls = [u for u in search_multi_domains(DOMAINS, f"로봇 채용 slam navigation", timeout, retries, logger, cfg.get("search", {})) if "greetinghr.com" in u.lower()]

    if not urls:
        urls = ["https://www.greetinghr.com/"]

    max_items = int(opts.get("max_items", 12))
    return [
        {
            "source_job_id": u.rsplit("/", 1)[-1],
            "url": u,
            "posted_at": datetime.now().strftime("%Y-%m-%d"),
            "title": "GreetingHR Robotics Position",
            "company": "Unknown",
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
            logger.info("source=greetinghr detail playwright failed url=%s err=%s", url, exc)

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
        "employment_type": "인턴" if "인턴" in blob or "intern" in blob.lower() else "정규직",
        "status_text": "모집중",
    }
