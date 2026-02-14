import re
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import quote_plus

import requests as _requests

from crawlers.common import DEFAULT_HEADERS, get_render_policy, parse_title_description, request_with_retry, search_site_links

_LINK_RE = re.compile(r'(?:href=["\'])?(https?://(?:www\.)?rocketpunch\.com/jobs/[^"\'?#\s]+|/jobs/[^"\'?#\s]+)', re.I)
_TITLE_RE = re.compile(r"<title>(.*?)</title>", re.I | re.S)
_META_DESC_RE = re.compile(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)', re.I)


def _to_abs(link: str) -> str:
    if link.startswith("http"):
        return link
    return f"https://www.rocketpunch.com{link}"


def _is_job_detail(url: str) -> bool:
    return bool(re.match(r"https?://(?:www\.)?rocketpunch\.com/jobs/\d+", url))


def _fetch_with_playwright(keyword: str, render: Dict[str, Any], logger) -> List[str]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return []

    search_url = f"https://www.rocketpunch.com/jobs?keywords={quote_plus(keyword)}"
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
                if isinstance(href, str) and "/jobs/" in href:
                    abs_url = _to_abs(href)
                    if _is_job_detail(abs_url):
                        urls.append(abs_url)
            browser.close()
    except Exception as exc:
        logger.info("source=rocketpunch playwright failed err=%s", exc)
    return list(dict.fromkeys(urls))


def fetch_list(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    q = opts.get("query", {})
    keyword = q.get("keyword", "로봇 SW")
    render = get_render_policy(opts, default_timeout_ms=30000, default_scroll_rounds=3)

    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))

    # 1) HTTP with Accept: application/json (rocketpunch returns JSON with job HTML)
    urls: List[str] = []
    endpoint = opts.get("api_url", "https://www.rocketpunch.com/jobs")
    json_headers = {**DEFAULT_HEADERS, "Accept": "application/json"}
    for search_keyword in [keyword, "로봇", "robot"]:
        if len(urls) >= 5:
            break
        for page_num in range(1, 4):
            try:
                resp = _requests.get(
                    endpoint, params={"keywords": search_keyword, "page": page_num},
                    headers=json_headers, timeout=timeout,
                )
                if resp.status_code != 200:
                    break
            except Exception:
                break
            found = [_to_abs(u) for u in _LINK_RE.findall(resp.text)]
            new_urls = [u for u in found if _is_job_detail(u) and u not in urls]
            urls.extend(new_urls)
            if not new_urls:
                break
    urls = list(dict.fromkeys(urls))
    logger.info("source=rocketpunch http links=%d", len(urls))

    # 2) Playwright fallback (requires login, may not work)
    if not urls and render["enabled"]:
        urls = _fetch_with_playwright(keyword, render, logger)

    # 3) Search fallback
    if not urls:
        urls = [u for u in search_site_links("rocketpunch.com", f"{keyword} /jobs/", timeout, retries, logger, cfg.get("search", {})) if "/jobs/" in u and _is_job_detail(_to_abs(u))]

    max_items = int(opts.get("max_items", 20))
    items: List[Dict[str, Any]] = []
    for url in urls[:max_items]:
        slug = url.rsplit("/", 1)[-1]
        items.append(
            {
                "source_job_id": slug,
                "url": url,
                "title": "RocketPunch Robotics Position",
                "company": "Unknown",
                "location": "미상",
                "posted_at": datetime.now().strftime("%Y-%m-%d"),
            }
        )
    return items


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
            logger.info("source=rocketpunch detail playwright failed url=%s err=%s", url, exc)

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
        "title": parsed.get("title", "RocketPunch Robotics Position"),
        "description": parsed.get("description", ""),
        "employment_type": "정규직",
        "status_text": "모집중",
    }
