import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import quote_plus

from crawlers.common import get_render_policy, parse_title_description, request_with_retry, search_multi_domains

DOMAINS = ["linkareer.com"]
JOB_URL_RE = re.compile(
    r"https?://(?:www\.)?linkareer\.com/(?:activity|recruit|recruits|recruitments?|jobs?|content)(?:/[^\"?\s#]+)+",
    re.I,
)
LIST_XHR_HINTS = ("/search", "/graphql", "/api")
BLOCKED_RESOURCE_TYPES = {"image", "media", "font"}
BREAKER_FILE = "data/source_health/linkareer_breaker.json"
BREAKER_504_THRESHOLD = 2
BREAKER_DNS_THRESHOLD = 2


def _valid(url: str) -> bool:
    return bool(JOB_URL_RE.match(url.strip()))


def _load_breaker() -> Dict[str, Any]:
    if not os.path.exists(BREAKER_FILE):
        return {}
    try:
        with open(BREAKER_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_breaker(data: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(BREAKER_FILE), exist_ok=True)
    with open(BREAKER_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _is_circuit_open() -> bool:
    day = datetime.now().strftime("%Y%m%d")
    state = _load_breaker()
    if state.get("day") != day:
        return False
    return int(state.get("consecutive_504", 0)) >= BREAKER_504_THRESHOLD or int(state.get("consecutive_dns", 0)) >= BREAKER_DNS_THRESHOLD


def _mark_504_failure() -> int:
    day = datetime.now().strftime("%Y%m%d")
    state = _load_breaker()
    if state.get("day") != day:
        state = {"day": day, "consecutive_504": 0, "consecutive_dns": 0}
    state["consecutive_504"] = int(state.get("consecutive_504", 0)) + 1
    _save_breaker(state)
    return int(state["consecutive_504"])


def _reset_504_failure() -> None:
    day = datetime.now().strftime("%Y%m%d")
    state = _load_breaker()
    if state.get("day") == day and int(state.get("consecutive_504", 0)) == 0:
        return
    _save_breaker({"day": day, "consecutive_504": 0, "consecutive_dns": int(state.get("consecutive_dns", 0)) if state.get("day") == day else 0})


def _mark_dns_failure() -> int:
    day = datetime.now().strftime("%Y%m%d")
    state = _load_breaker()
    if state.get("day") != day:
        state = {"day": day, "consecutive_504": 0, "consecutive_dns": 0}
    state["consecutive_dns"] = int(state.get("consecutive_dns", 0)) + 1
    _save_breaker(state)
    return int(state["consecutive_dns"])


def _reset_dns_failure() -> None:
    day = datetime.now().strftime("%Y%m%d")
    state = _load_breaker()
    if state.get("day") == day and int(state.get("consecutive_dns", 0)) == 0:
        return
    _save_breaker({"day": day, "consecutive_504": int(state.get("consecutive_504", 0)) if state.get("day") == day else 0, "consecutive_dns": 0})


def _fetch_with_playwright(keyword: str, render: Dict[str, Any], logger) -> tuple[List[str], bool]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return [], False

    search_url = f"https://linkareer.com/search/result?query={quote_plus(keyword)}"
    urls: List[str] = []
    dns_failed = False
    attempts = 2
    for attempt in range(1, attempts + 1):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                ctx = browser.new_context(
                    user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                    locale="ko-KR",
                    extra_http_headers={
                        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Referer": "https://linkareer.com/",
                    },
                )
                page = ctx.new_page()
                page.route(
                    "**/*",
                    lambda route: route.abort() if route.request.resource_type in BLOCKED_RESOURCE_TYPES else route.continue_(),
                )
                page.goto(search_url, wait_until="domcontentloaded", timeout=render["timeout_ms"])
                try:
                    page.wait_for_response(
                        lambda r: (
                            r.status == 200
                            and any(hint in r.url for hint in LIST_XHR_HINTS)
                            and r.request.resource_type in {"xhr", "fetch"}
                        ),
                        timeout=5000,
                    )
                except Exception:
                    page.wait_for_timeout(2000)
                try:
                    page.wait_for_selector(
                        "a[href*='/recruit/'], a[href*='/jobs/'], a[href*='/activity/'], a[href*='/content/']",
                        timeout=5000,
                    )
                except Exception:
                    pass
                for _ in range(render["scroll_rounds"]):
                    page.mouse.wheel(0, 4000)
                    page.wait_for_timeout(1000)
                hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
                for href in hrefs or []:
                    if isinstance(href, str) and _valid(href):
                        urls.append(href)
                if not urls:
                    content = page.content()
                    for m in JOB_URL_RE.findall(content):
                        urls.append(m)
                browser.close()
                if urls:
                    break
        except Exception as exc:
            logger.info("source=linkareer playwright failed attempt=%d/%d err=%s", attempt, attempts, exc)
            if "ERR_NAME_NOT_RESOLVED" in str(exc):
                dns_failed = True
    return list(dict.fromkeys(urls)), dns_failed


def fetch_list(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    timeout = int(opts.get("http_timeout_sec", max(25, int(cfg.get("network", {}).get("timeout_sec", 10)))))
    retries = int(cfg.get("network", {}).get("retry", 2))
    keyword = opts.get("query", {}).get("keyword", "로봇")
    render = get_render_policy(opts, default_timeout_ms=40000, default_scroll_rounds=3)
    circuit_open = _is_circuit_open()

    # 1) Search fallback first (more stable when renderer/DNS is unstable)
    urls: List[str] = []
    queries = [
        "site:linkareer.com 로봇 recruit",
        "site:linkareer.com 로봇 jobs",
        "site:linkareer.com 로봇 activity",
        "site:linkareer.com robotics",
        "로봇 채용 신입",
    ]
    merged: List[str] = []
    seen = set()
    for q in queries:
        for u in search_multi_domains(DOMAINS, q, timeout, retries, logger, cfg.get("search", {})):
            if not _valid(u) or u in seen:
                continue
            seen.add(u)
            merged.append(u)
    urls = merged
    logger.info("source=linkareer search-first links=%d", len(urls))

    # 2) Playwright fallback
    dns_failed = False
    if not urls and render["enabled"] and not circuit_open:
        urls, dns_failed = _fetch_with_playwright(keyword, render, logger)
        if not urls:
            urls, dns_failed2 = _fetch_with_playwright("로봇 SW", render, logger)
            dns_failed = dns_failed or dns_failed2
        logger.info("source=linkareer playwright links=%d", len(urls))
        if dns_failed and not urls:
            dns_count = _mark_dns_failure()
            logger.info("source=linkareer dns_failure count=%d threshold=%d", dns_count, BREAKER_DNS_THRESHOLD)
        elif urls:
            _reset_dns_failure()
    elif circuit_open:
        logger.info("source=linkareer circuit_open=true skip direct crawling")

    # 3) HTTP fallback (last resort)
    if not urls and not circuit_open and not dns_failed:
        search_url = f"https://linkareer.com/search/result?query={quote_plus(keyword)}"
        meta: Dict[str, Any] = {}
        resp = request_with_retry("GET", search_url, timeout, retries, logger, _meta=meta)
        if resp:
            _reset_504_failure()
            _reset_dns_failure()
            urls = list(dict.fromkeys(JOB_URL_RE.findall(resp.text)))
        elif int(meta.get("status_code", 0)) == 504:
            failed = _mark_504_failure()
            logger.info("source=linkareer http_504 count=%d threshold=%d", failed, BREAKER_504_THRESHOLD)
        elif "name resolution" in str(meta.get("error", "")).lower():
            dns_count = _mark_dns_failure()
            logger.info("source=linkareer dns_failure count=%d threshold=%d", dns_count, BREAKER_DNS_THRESHOLD)

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
        attempts = 2
        for attempt in range(1, attempts + 1):
            try:
                from playwright.sync_api import sync_playwright
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    ctx = browser.new_context(
                        user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                        locale="ko-KR",
                        extra_http_headers={
                            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                            "Referer": "https://linkareer.com/",
                        },
                    )
                    pg = ctx.new_page()
                    pg.route(
                        "**/*",
                        lambda route: route.abort() if route.request.resource_type in BLOCKED_RESOURCE_TYPES else route.continue_(),
                    )
                    pg.goto(url, wait_until="domcontentloaded", timeout=render["timeout_ms"])
                    pg.wait_for_timeout(2000)
                    page_html = pg.content()
                    browser.close()
                    if page_html:
                        break
            except Exception as exc:
                logger.info("source=linkareer detail playwright failed url=%s attempt=%d/%d err=%s", url, attempt, attempts, exc)

    if not page_html:
        timeout = int(opts.get("http_timeout_sec", max(25, int(cfg.get("network", {}).get("timeout_sec", 10)))))
        retries = int(cfg.get("network", {}).get("retry", 2))
        resp = request_with_retry("GET", url, timeout, retries, logger)
        if resp:
            page_html = resp.text

    if not page_html:
        return {}

    parsed = parse_title_description(page_html)
    title = parsed.get("title", item.get("title", ""))
    desc = parsed.get("description", "")
    if not title or len(desc.strip()) < 60:
        return {}
    return {
        "title": title,
        "description": desc,
        "employment_type": "정규직",
        "status_text": "모집중",
    }
