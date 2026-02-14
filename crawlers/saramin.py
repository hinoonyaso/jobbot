import re
from datetime import datetime
import os
from typing import Any, Dict, List
from urllib.parse import quote_plus
import html
import xml.etree.ElementTree as ET

from crawlers.common import get_render_policy, request_with_retry, search_site_links

_LINK_RE = re.compile(
    r"(https?://www\.saramin\.co\.kr/zf_user/jobs/relay/view\?[^\"'\s<>]*rec_idx=\d+[^\"'\s<>]*|/zf_user/jobs/relay/view\?[^\"'\s<>]*rec_idx=\d+[^\"'\s<>]*)",
    re.I,
)
_TITLE_RE = re.compile(r"<title>(.*?)</title>", re.I | re.S)
_DEADLINE_RE = re.compile(r"(20\d{2}[./-]\d{1,2}[./-]\d{1,2}).{0,20}(마감|까지|종료)", re.I)
SARAMIN_API_URL = "https://oapi.saramin.co.kr/job-search"

EMPLOYMENT_CODE_MAP = {
    "1": "정규직",
    "2": "계약직",
    "4": "인턴",
    "10": "계약직",
    "11": "인턴",
}


def _to_abs(url: str) -> str:
    url = html.unescape((url or "").strip())
    if url.startswith("http"):
        return url
    return f"https://www.saramin.co.kr{url}"


def _normalize_detail_url(url: str) -> str:
    raw = _to_abs(url)
    m = re.search(r"[?&]rec_idx=(\d+)", raw)
    if m:
        return f"https://www.saramin.co.kr/zf_user/jobs/view?rec_idx={m.group(1)}"
    return raw


def _extract_list_urls(page_html: str) -> List[str]:
    return list(dict.fromkeys([_normalize_detail_url(u) for u in _LINK_RE.findall(page_html or "")]))


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _fmt_date_from_ts(ts: Any) -> str:
    x = _safe_int(ts, 0)
    if x <= 0:
        return ""
    if x > 2_000_000_000_000:
        x = x // 1000
    try:
        return datetime.fromtimestamp(x).strftime("%Y-%m-%d")
    except Exception:
        return ""


def _fetch_from_api(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    api_cfg = opts.get("api", {}) if isinstance(opts.get("api", {}), dict) else {}
    if not bool(api_cfg.get("enabled", False)):
        return []

    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))
    max_items = int(opts.get("max_items", 20))
    q = opts.get("query", {})

    access_key = str(api_cfg.get("access_key", "")).strip()
    if not access_key:
        env_name = str(api_cfg.get("access_key_env", "SARAMIN_ACCESS_KEY"))
        access_key = os.getenv(env_name, "").strip()
    if not access_key:
        logger.info("source=saramin api skipped: missing access key")
        return []

    keyword = str(q.get("keyword", "로봇 SW")).strip()
    api_url = str(api_cfg.get("api_url", SARAMIN_API_URL))
    params = {
        "access-key": access_key,
        "keywords": keyword,
        "count": min(max_items, 110),
        "start": int(api_cfg.get("start", 0)),
        "sort": str(api_cfg.get("sort", "pd")),
    }
    if q.get("loc_codes"):
        params["loc_cd"] = ",".join([str(x) for x in q.get("loc_codes", []) if str(x).strip()])
    if q.get("job_type_codes"):
        params["job_type"] = ",".join([str(x) for x in q.get("job_type_codes", []) if str(x).strip()])
    if q.get("edu_codes"):
        params["edu_lv"] = ",".join([str(x) for x in q.get("edu_codes", []) if str(x).strip()])

    resp = request_with_retry("GET", api_url, timeout, retries, logger, params=params)
    if not resp:
        return []

    items: List[Dict[str, Any]] = []
    try:
        data = resp.json()
        jobs = (data.get("jobs") or {}).get("job") or []
        if isinstance(jobs, dict):
            jobs = [jobs]
        for j in jobs:
            company = (((j.get("company") or {}).get("detail") or {}).get("name") or "Unknown").strip()
            title = (((j.get("position") or {}).get("title") or "Saramin Robotics Position")).strip()
            url = str(j.get("url") or "").strip()
            if not url:
                continue
            source_job_id = str(j.get("id") or "").strip() or url
            loc = ((j.get("position") or {}).get("location") or {})
            job_type = ((j.get("position") or {}).get("job-type") or {})
            employment_code = str(job_type.get("code") or "").strip()
            items.append(
                {
                    "source_job_id": source_job_id,
                    "url": url,
                    "title": title,
                    "company": company or "Unknown",
                    "location": str(loc.get("name") or "미상"),
                    "employment_type": EMPLOYMENT_CODE_MAP.get(employment_code, "정규직"),
                    "posted_at": _fmt_date_from_ts(j.get("posting-timestamp")) or datetime.now().strftime("%Y-%m-%d"),
                    "deadline": _fmt_date_from_ts(j.get("expiration-timestamp")),
                    "status_text": "모집중",
                }
            )
        if items:
            return items[:max_items]
    except Exception:
        pass

    try:
        root = ET.fromstring(resp.text)
        for node in root.findall(".//job"):
            source_job_id = (node.findtext("id") or "").strip()
            url = (node.findtext("url") or "").strip()
            if not url:
                continue
            title = (node.findtext("./position/title") or "Saramin Robotics Position").strip()
            company = (node.findtext("./company/detail/name") or "Unknown").strip()
            location = (node.findtext("./position/location/name") or "미상").strip()
            employment_code = (node.findtext("./position/job-type/code") or "").strip()
            posted_ts = node.findtext("posting-timestamp")
            deadline_ts = node.findtext("expiration-timestamp")
            items.append(
                {
                    "source_job_id": source_job_id or url,
                    "url": url,
                    "title": title,
                    "company": company or "Unknown",
                    "location": location or "미상",
                    "employment_type": EMPLOYMENT_CODE_MAP.get(employment_code, "정규직"),
                    "posted_at": _fmt_date_from_ts(posted_ts) or datetime.now().strftime("%Y-%m-%d"),
                    "deadline": _fmt_date_from_ts(deadline_ts),
                    "status_text": "모집중",
                }
            )
    except Exception:
        return []

    return items[:max_items]


def _fetch_with_playwright(search_url: str, render: Dict[str, Any], logger) -> List[str]:
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
            page.goto(search_url, wait_until=render["wait_until"], timeout=render["timeout_ms"])
            for _ in range(render["scroll_rounds"]):
                page.mouse.wheel(0, 3000)
                page.wait_for_timeout(800)
            hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
            for href in hrefs or []:
                if isinstance(href, str) and "rec_idx=" in href:
                    urls.append(_normalize_detail_url(href))
            browser.close()
    except Exception as exc:
        logger.info("source=saramin playwright failed err=%s", exc)
    return list(dict.fromkeys(urls))


def fetch_list(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    api_items = _fetch_from_api(opts, cfg, logger)
    if api_items:
        return api_items

    q = opts.get("query", {})
    keyword = q.get("keyword", "로봇 sw")
    search_url = f"https://www.saramin.co.kr/zf_user/search/recruit?searchword={quote_plus(keyword)}"
    render = get_render_policy(opts, default_timeout_ms=30000, default_scroll_rounds=3)

    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))

    # 1) Playwright first (saramin blocks bot HTTP requests)
    urls: List[str] = []
    if render["enabled"]:
        urls = _fetch_with_playwright(search_url, render, logger)
        logger.info("source=saramin playwright links=%d", len(urls))

    # 2) HTTP fallback
    if not urls:
        resp = request_with_retry("GET", search_url, timeout, retries, logger)
        if resp:
            urls = _extract_list_urls(resp.text)

    # 3) Search engine fallback
    if not urls:
        urls = [_normalize_detail_url(u) for u in search_site_links("saramin.co.kr", keyword, timeout, retries, logger, cfg.get("search", {})) if "rec_idx=" in u]

    max_items = int(opts.get("max_items", 20))
    out = []
    for u in urls[:max_items]:
        m = re.search(r"rec_idx=(\d+)", u)
        out.append(
            {
                "url": u,
                "source_job_id": (m.group(1) if m else u),
                "title": "Saramin Robotics Position",
                "company": "Unknown",
                "location": "미상",
                "posted_at": datetime.now().strftime("%Y-%m-%d"),
            }
        )
    return out


def fetch_detail(item: Dict[str, Any], opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> Dict[str, Any]:
    url = _normalize_detail_url(str(item.get("url", "")))
    render = get_render_policy(opts, default_timeout_ms=20000, default_scroll_rounds=1)

    # Try Playwright for detail too (saramin blocks HTTP)
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
                pg.wait_for_timeout(1000)
                page_html = pg.content()
                browser.close()
        except Exception as exc:
            logger.debug("source=saramin detail playwright failed url=%s err=%s", url, exc)

    # HTTP fallback
    if not page_html:
        timeout = int(opts.get("detail_http_timeout_sec", min(6, int(cfg.get("network", {}).get("timeout_sec", 10)))))
        retries = int(opts.get("detail_http_retries", 0))
        resp = request_with_retry("GET", url, timeout, retries, logger, log_failures=False)
        if resp:
            page_html = resp.text

    if not page_html:
        return {}

    title_match = _TITLE_RE.search(page_html)
    deadline_match = _DEADLINE_RE.search(page_html)
    title = (title_match.group(1).strip() if title_match else "Saramin Robotics Position")[:120]
    desc = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", page_html))[:2200]

    return {
        "title": title,
        "company": "Unknown",
        "location": "미상",
        "employment_type": "정규직",
        "deadline": deadline_match.group(1).replace(".", "-").replace("/", "-") if deadline_match else "",
        "status_text": "모집중" if "모집중" in desc or "진행중" in desc else "",
        "description": desc,
    }
