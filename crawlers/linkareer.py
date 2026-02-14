import re
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import quote_plus

from crawlers.common import get_render_policy, parse_title_description, request_with_retry, search_multi_domains

DOMAINS = ["linkareer.com"]
JOB_URL_RE = re.compile(r"https?://linkareer\.com/(?:activity|recruit|recruits|jobs?|content)/\d+", re.I)


def _valid(url: str) -> bool:
    return bool(JOB_URL_RE.match(url.strip()))


def _fetch_with_playwright(keyword: str, render: Dict[str, Any], logger) -> List[str]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return []

    search_url = f"https://linkareer.com/search/result?query={quote_plus(keyword)}"
    urls: List[str] = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                locale="ko-KR",
            )
            page = ctx.new_page()
            page.goto(search_url, wait_until=render["wait_until"], timeout=render["timeout_ms"])
            page.wait_for_timeout(3000)
            for _ in range(render["scroll_rounds"]):
                page.mouse.wheel(0, 4000)
                page.wait_for_timeout(1500)
            hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
            for href in hrefs or []:
                if isinstance(href, str) and _valid(href):
                    urls.append(href)
            # Also try extracting from page content (some links may be in JS state)
            if not urls:
                content = page.content()
                for m in JOB_URL_RE.findall(content):
                    urls.append(m)
            browser.close()
    except Exception as exc:
        logger.info("source=linkareer playwright failed err=%s", exc)
    return list(dict.fromkeys(urls))


def fetch_list(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))
    keyword = opts.get("query", {}).get("keyword", "로봇")
    render = get_render_policy(opts, default_timeout_ms=40000, default_scroll_rounds=3)

    # 1) Playwright first (linkareer is SPA, HTTP times out)
    urls: List[str] = []
    if render["enabled"]:
        urls = _fetch_with_playwright(keyword, render, logger)
        if not urls:
            urls = _fetch_with_playwright("로봇 SW", render, logger)
        logger.info("source=linkareer playwright links=%d", len(urls))

    # 2) HTTP fallback (likely to timeout but try anyway)
    if not urls:
        search_url = f"https://linkareer.com/search/result?query={quote_plus(keyword)}"
        resp = request_with_retry("GET", search_url, timeout, retries, logger)
        if resp:
            urls = list(dict.fromkeys(JOB_URL_RE.findall(resp.text)))

    # 3) Search fallback
    if not urls:
        urls = [u for u in search_multi_domains(DOMAINS, f"로봇 채용 신입", timeout, retries, logger, cfg.get("search", {})) if _valid(u)]

    max_items = int(opts.get("max_items", 12))
    return [
        {
            "source_job_id": u.rsplit("/", 1)[-1],
            "url": u,
            "posted_at": datetime.now().strftime("%Y-%m-%d"),
            "title": "Linkareer Robotics Position",
            "company": "Unknown",
            "location": "미상",
        }
        for u in urls[:max_items]
    ]


def fetch_detail(item: Dict[str, Any], opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> Dict[str, Any]:
    url = item.get("url", "")
    render = get_render_policy(opts, default_timeout_ms=25000, default_scroll_rounds=1)

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
            logger.info("source=linkareer detail playwright failed url=%s err=%s", url, exc)

    if not page_html:
        timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
        retries = int(cfg.get("network", {}).get("retry", 2))
        resp = request_with_retry("GET", url, timeout, retries, logger)
        if resp:
            page_html = resp.text

    if not page_html:
        return {}

    parsed = parse_title_description(page_html)
    return {
        "title": parsed.get("title", item.get("title", "")),
        "description": parsed.get("description", ""),
        "employment_type": "정규직",
        "status_text": "모집중",
    }
