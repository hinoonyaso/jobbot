import re
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import urldefrag, urlparse

from crawlers.common import (
    get_render_policy,
    parse_title_description,
    request_with_retry,
    same_host_or_relative,
    search_links_with_playwright,
)

HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.I)
JOB_PATH_RE = re.compile(r"(career|careers|recruit|jobs?|position|posting|apply|o/)", re.I)
BLOCKED_PATH_RE = re.compile(
    r"(about|company|news|press|media|blog|story|ir|investor|contact|privacy|terms|policy|faq)",
    re.I,
)
JOB_SIGNAL_RE = re.compile(
    r"(채용|모집|지원|공고|recruit|career|job|position|opening|hiring|apply)",
    re.I,
)


def _extract_job_links(html: str, base_url: str) -> List[str]:
    links: List[str] = []
    for href in HREF_RE.findall(html or ""):
        abs_url = same_host_or_relative(base_url, href)
        if not abs_url:
            continue
        # Drop fragment-only variants and known error routes that frequently produce 404.
        abs_url, _ = urldefrag(abs_url)
        if not abs_url or abs_url.endswith("/error") or "/error/" in abs_url:
            continue
        if BLOCKED_PATH_RE.search(abs_url):
            continue
        if JOB_PATH_RE.search(abs_url):
            links.append(abs_url)

    uniq: List[str] = []
    seen = set()
    for u in links:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    return uniq


def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def fetch_list(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))
    max_items = int(opts.get("max_items", 20))
    pages = opts.get("company_pages", [])
    render = get_render_policy(opts, default_timeout_ms=30000, default_scroll_rounds=3)

    items: List[Dict[str, Any]] = []
    seen = set()
    per_page_limit = int(opts.get("per_page_limit", 5))

    for page in pages:
        if not isinstance(page, dict):
            continue
        start_url = str(page.get("url", "")).strip()
        company = str(page.get("company", "Unknown")).strip() or "Unknown"
        region = str(page.get("region", "미상")).strip() or "미상"
        if not start_url:
            continue

        links: List[str] = []
        resp = request_with_retry("GET", start_url, timeout, retries, logger)
        if resp:
            links.extend(_extract_job_links(resp.text, start_url))

        if not links and render["enabled"]:
            host = _domain(start_url)
            if host:
                links = search_links_with_playwright(
                    start_url=start_url,
                    link_regex=rf"{re.escape(host)}/.*(?:career|careers|recruit|jobs?|position|posting|apply|o/)",
                    timeout_ms=render["timeout_ms"],
                    logger=logger,
                    wait_until=render["wait_until"],
                    scroll_rounds=render["scroll_rounds"],
                )

        # 채용성 링크를 찾지 못한 회사는 스킵한다.
        if not links:
            logger.info("source=company_pages skip no_job_links company=%s url=%s", company, start_url)
            continue

        for u in links[:per_page_limit]:
            if u in seen:
                continue
            seen.add(u)
            sid = u.rstrip("/").rsplit("/", 1)[-1][:80]
            items.append(
                {
                    "source_job_id": sid,
                    "url": u,
                    "posted_at": datetime.now().strftime("%Y-%m-%d"),
                    "title": f"{company} Careers",
                    "company": company,
                    "location": region,
                }
            )
            if len(items) >= max_items:
                return items

    return items


def fetch_detail(item: Dict[str, Any], opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> Dict[str, Any]:
    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))
    resp = request_with_retry("GET", item.get("url", ""), timeout, retries, logger)
    if not resp:
        return {}

    parsed = parse_title_description(resp.text)
    text_blob = f"{parsed.get('title','')} {parsed.get('description','')} {item.get('url','')}"
    if not JOB_SIGNAL_RE.search(text_blob):
        return {}
    return {
        "title": parsed.get("title", item.get("title", "")),
        "description": parsed.get("description", ""),
        "employment_type": "인턴" if ("인턴" in text_blob or "intern" in text_blob.lower()) else "정규직",
        "status_text": "모집중",
    }
