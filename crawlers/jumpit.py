import re
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import quote_plus

from crawlers.common import get_render_policy, parse_title_description, request_with_retry, search_multi_domains

DOMAINS = ["jumpit.saramin.co.kr", "jumpit.co.kr"]
JOB_URL_RE = re.compile(
    r"https?://jumpit(?:\.saramin)?\.co\.kr/position/(\d+)",
    re.I,
)


def _is_job_url(url: str) -> bool:
    return bool(JOB_URL_RE.match(url.strip()))


def _fetch_with_playwright(keyword: str, render: Dict[str, Any], logger) -> List[str]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return []

    search_url = f"https://jumpit.saramin.co.kr/positions?keyword={quote_plus(keyword)}"
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
            page.wait_for_timeout(2000)
            for _ in range(render["scroll_rounds"]):
                page.mouse.wheel(0, 4000)
                page.wait_for_timeout(1500)
            hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
            for href in hrefs or []:
                if isinstance(href, str) and _is_job_url(href):
                    urls.append(href)
            browser.close()
    except Exception as exc:
        logger.info("source=jumpit playwright failed err=%s", exc)
    return list(dict.fromkeys(urls))


def fetch_list(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))
    keyword = opts.get("query", {}).get("keyword", "로봇")
    render = get_render_policy(opts, default_timeout_ms=30000, default_scroll_rounds=3)

    # 1) Playwright first (jumpit is SPA)
    urls: List[str] = []
    if render["enabled"]:
        # Try with main keyword, then broader keyword
        urls = _fetch_with_playwright(keyword, render, logger)
        if not urls:
            urls = _fetch_with_playwright("로봇", render, logger)
        logger.info("source=jumpit playwright links=%d", len(urls))

    # 2) Search engine fallback
    if not urls:
        urls = [u for u in search_multi_domains(DOMAINS, f"로봇 SW 채용", timeout, retries, logger, cfg.get("search", {})) if _is_job_url(u)]

    max_items = int(opts.get("max_items", 12))
    items = []
    for u in urls[:max_items]:
        m = JOB_URL_RE.match(u)
        items.append({
            "source_job_id": m.group(1) if m else u.rsplit("/", 1)[-1],
            "url": u,
            "posted_at": datetime.now().strftime("%Y-%m-%d"),
            "title": "Jumpit Robotics Position",
            "company": "Unknown",
            "location": "미상",
        })
    return items


def fetch_detail(item: Dict[str, Any], opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> Dict[str, Any]:
    url = item.get("url", "")
    render = get_render_policy(opts, default_timeout_ms=20000, default_scroll_rounds=1)

    title = ""
    body_text = ""
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
                pg.wait_for_timeout(3000)
                title = pg.title() or ""
                body_text = pg.inner_text("body") or ""
                browser.close()
        except Exception as exc:
            logger.info("source=jumpit detail playwright failed url=%s err=%s", url, exc)

    if not body_text:
        timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
        retries = int(cfg.get("network", {}).get("retry", 2))
        resp = request_with_retry("GET", url, timeout, retries, logger)
        if resp:
            parsed = parse_title_description(resp.text)
            title = parsed.get("title", "")
            body_text = parsed.get("description", "")

    if not body_text:
        return {}

    # Clean and truncate
    desc = re.sub(r"\s+", " ", body_text).strip()[:2600]
    blob = f"{title} {desc}"
    emp_type = "인턴" if "인턴" in blob or "intern" in blob.lower() else "정규직"

    return {
        "title": title[:160],
        "description": desc,
        "employment_type": emp_type,
        "status_text": "모집중",
    }
