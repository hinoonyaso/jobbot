import re
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import quote_plus

from crawlers.common import get_render_policy, parse_title_description, request_with_retry, search_links_with_playwright, search_multi_domains

DOMAINS = ["wanted.co.kr"]
JOB_URL_RE = re.compile(r"^https?://(?:www\.)?wanted\.co\.kr/wd/(\d+)(?:\?.*)?$", re.I)
WD_ID_RE = re.compile(r"/wd/(\d+)", re.I)
LINK_RE = re.compile(r"https?://(?:www\.)?wanted\.co\.kr/wd/\d+|/wd/\d+", re.I)
TITLE_COMPANY_RE = re.compile(r"^\[([^\]]+)\]\s*(.+)$")
LOCATION_RE = re.compile(r"(서울|성남|용인|강남|판교|경기)[^\\n,.;:]{0,20}")


def _to_abs(link: str) -> str:
    if link.startswith("http"):
        return link
    return f"https://www.wanted.co.kr{link}"


def _extract_wd_id(url: str) -> str:
    m = WD_ID_RE.search(url or "")
    return m.group(1) if m else ""


def _is_job_url(url: str) -> bool:
    return bool(JOB_URL_RE.match(url.strip()))


def fetch_list(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))
    keyword = opts.get("query", {}).get("keyword", "로봇 SW")
    render = get_render_policy(opts, default_timeout_ms=20000, default_scroll_rounds=3)
    api_url = opts.get("api_url", "https://www.wanted.co.kr/api/v4/jobs")
    max_items = int(opts.get("max_items", 20))

    urls: List[str] = []
    api_items: List[Dict[str, Any]] = []

    # 1) official-ish API first
    api_terms = []
    for term in [keyword, keyword.replace("SW", "").strip(), "로봇"]:
        t = (term or "").strip()
        if t and t not in api_terms:
            api_terms.append(t)
    for term in api_terms:
        if len(api_items) >= max_items:
            break
        for offset in (0, 20, 40):
            params = {
                "query": term,
                "country": "kr",
                "years": -1,
                "limit": 20,
                "offset": offset,
                "job_sort": "job.latest_order",
            }
            resp_api = request_with_retry("GET", api_url, timeout, retries, logger, params=params)
            if not resp_api:
                continue
            try:
                data = (resp_api.json() or {}).get("data", [])
            except Exception:
                data = []
            if not data:
                continue
            for row in data:
                if str(row.get("status", "")).lower() != "active":
                    continue
                job_id = str(row.get("id", "")).strip()
                if not job_id:
                    continue
                api_items.append(
                    {
                        "source_job_id": job_id,
                        "url": f"https://www.wanted.co.kr/wd/{job_id}",
                        "posted_at": datetime.now().strftime("%Y-%m-%d"),
                        "title": str(row.get("position", "")).strip() or "Wanted Robotics Position",
                        "company": str((row.get("company") or {}).get("name", "")).strip() or "Unknown",
                        "location": str((row.get("address") or {}).get("location", "")).strip() or "미상",
                    }
                )
            if len(data) < 20:
                break
    if api_items:
        dedup = []
        seen_ids = set()
        for it in api_items:
            sid = it.get("source_job_id", "")
            if not sid or sid in seen_ids:
                continue
            seen_ids.add(sid)
            dedup.append(it)
        return dedup[:max_items]

    # 2) HTML/search fallback
    search_url = f"https://www.wanted.co.kr/search?query={quote_plus(keyword)}&tab=position"
    resp = request_with_retry("GET", search_url, timeout, retries, logger)
    if resp:
        urls.extend([_to_abs(x) for x in LINK_RE.findall(resp.text or "")])

    if not urls:
        urls.extend(search_multi_domains(DOMAINS, f"{keyword} site:wanted.co.kr/wd/", timeout, retries, logger, cfg.get("search", {})))
    if not urls:
        if render["enabled"]:
            urls.extend(
                search_links_with_playwright(
                    start_url=search_url,
                    link_regex=r"wanted\.co\.kr/wd/\d+",
                    timeout_ms=render["timeout_ms"],
                    logger=logger,
                    wait_until=render["wait_until"],
                    scroll_rounds=render["scroll_rounds"],
                )
            )

    seen, uniq = set(), []
    for u in urls:
        u = _to_abs(u)
        if u in seen:
            continue
        if not _is_job_url(u):
            continue
        seen.add(u)
        uniq.append(u)

    return [
        {
            "source_job_id": _extract_wd_id(u),
            "url": u,
            "posted_at": datetime.now().strftime("%Y-%m-%d"),
            "title": "Wanted Robotics Position",
            "company": "Unknown",
            "location": "미상",
        }
        for u in uniq[:max_items]
    ]


def fetch_detail(item: Dict[str, Any], opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> Dict[str, Any]:
    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))
    resp = request_with_retry("GET", item.get("url", ""), timeout, retries, logger)
    if not resp:
        return {}

    parsed = parse_title_description(resp.text)
    blob = f"{parsed.get('title','')} {parsed.get('description','')}"
    raw_title = parsed.get("title", item.get("title", ""))
    raw_desc = parsed.get("description", "")

    company = "Unknown"
    title = raw_title
    m = TITLE_COMPANY_RE.match(raw_title.strip())
    if m:
        company = m.group(1).strip()
        title = m.group(2).strip()

    loc = "미상"
    lm = LOCATION_RE.search(raw_desc)
    if lm:
        loc = lm.group(0).strip()

    return {
        "title": title,
        "company": company,
        "location": loc,
        "description": raw_desc,
        "employment_type": "인턴" if "인턴" in blob or "intern" in blob.lower() else "정규직",
        "status_text": "Open",
    }
