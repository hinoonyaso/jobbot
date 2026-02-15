import re
from datetime import datetime
import os
from typing import Any, Dict, List
import html
import json
import xml.etree.ElementTree as ET
from urllib.parse import quote_plus, unquote

from crawlers.common import (
    get_render_policy,
    request_with_retry,
    search_links_with_playwright,
    search_multi_domains,
    search_site_links,
)

_TITLE_RE = re.compile(r"<title>(.*?)</title>", re.I | re.S)
_DEADLINE_RE = re.compile(r"(20\d{2}[./-]\d{1,2}[./-]\d{1,2}).{0,20}(마감|까지|종료)", re.I)
_META_OG_TITLE_RE = re.compile(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', re.I)
_META_TW_TITLE_RE = re.compile(r'<meta[^>]+name=["\']twitter:title["\'][^>]+content=["\']([^"\']+)["\']', re.I)
_META_OG_DESC_RE = re.compile(r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']', re.I)
_JSONLD_RE = re.compile(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.I | re.S)
_TITLE_COMPANY_RE = re.compile(r"^\s*\[([^\]]+)\]")
_LABEL_VALUE_RE = re.compile(
    r"(?:근무지역|근무\s*지역|지역)\s*[:：]?\s*([가-힣A-Za-z0-9·\-/(),\s]{2,60})",
    re.I,
)
SARAMIN_API_URL = "https://oapi.saramin.co.kr/job-search"
SARAMIN_SEARCH_URL = "https://www.saramin.co.kr/zf_user/search/recruit?searchType=search&searchword={q}"

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
    rec_idx = _extract_rec_idx(url)
    if rec_idx:
        return f"https://www.saramin.co.kr/zf_user/jobs/view?rec_idx={rec_idx}"
    raw = _to_abs(url)
    if "/zf_user/jobs/" in raw.lower():
        return raw
    return raw


def _extract_rec_idx(url: str) -> str:
    raw = html.unescape((url or "").strip())
    for _ in range(2):
        raw = unquote(raw)
    low = raw.lower()
    if "saramin.co.kr" not in low:
        return ""
    m = re.search(r"(?:\?|&|amp;)rec_idx(?:=|%3d)(\d+)", raw, re.I)
    if m:
        return m.group(1)
    m = re.search(r"rec_idx(?:=|%3d)(\d+)", raw, re.I)
    if m:
        return m.group(1)
    return ""


def _extract_rec_idx_urls_from_html(page_html: str) -> List[str]:
    raw = html.unescape(page_html or "")
    for _ in range(2):
        raw = unquote(raw)
    out: List[str] = []
    seen = set()
    for rec_idx in re.findall(r"rec_idx(?:=|%3[dD])(\d+)", raw, re.I):
        u = f"https://www.saramin.co.kr/zf_user/jobs/view?rec_idx={rec_idx}"
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _strip_tags(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s or "")
    return re.sub(r"\s+", " ", s).strip()


def _extract_jsonld_objects(page_html: str) -> List[Dict[str, Any]]:
    objs: List[Dict[str, Any]] = []
    for m in _JSONLD_RE.finditer(page_html or ""):
        raw = (m.group(1) or "").strip()
        if not raw:
            continue
        raw = html.unescape(raw)
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                objs.append(data)
            elif isinstance(data, list):
                for x in data:
                    if isinstance(x, dict):
                        objs.append(x)
        except Exception:
            continue
    return objs


def _pick_company_from_jsonld(objs: List[Dict[str, Any]]) -> str:
    cand_keys = ["hiringOrganization", "worksFor", "author", "publisher"]
    for o in objs:
        if o.get("@type") in ("JobPosting", "jobPosting"):
            for k in cand_keys:
                v = o.get(k)
                if isinstance(v, dict) and v.get("name"):
                    return str(v["name"]).strip()
            v = o.get("organization")
            if isinstance(v, dict) and v.get("name"):
                return str(v["name"]).strip()
    for o in objs:
        if o.get("@type") in ("Organization", "organization") and o.get("name"):
            return str(o["name"]).strip()
    return ""


def _pick_location_from_jsonld(objs: List[Dict[str, Any]]) -> str:
    for o in objs:
        if o.get("@type") not in ("JobPosting", "jobPosting"):
            continue
        loc = o.get("jobLocation")
        if isinstance(loc, list):
            loc = loc[0] if loc else {}
        if isinstance(loc, dict):
            addr = loc.get("address")
            if isinstance(addr, dict):
                parts: List[str] = []
                for key in ("addressRegion", "addressLocality", "streetAddress"):
                    if addr.get(key):
                        parts.append(str(addr[key]).strip())
                if parts:
                    return " ".join(parts)
    return ""


def _pick_company_from_title(title: str) -> str:
    m = _TITLE_COMPANY_RE.search(title or "")
    if not m:
        return ""
    return html.unescape(m.group(1).strip())


def _pick_location_from_text(page_html: str) -> str:
    text = _strip_tags(page_html)
    m = _LABEL_VALUE_RE.search(text)
    if not m:
        return ""
    loc = re.sub(r"\s+", " ", m.group(1).strip())
    # trim obvious trailing labels if the text run is too long
    loc = re.split(r"(경력|학력|급여|직급|고용형태|근무요일|근무시간)\s*[:：]?", loc, maxsplit=1)[0].strip()
    return loc[:40]


def _fetch_direct_search_urls(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[str]:
    q = opts.get("query", {})
    keyword = str(q.get("keyword", "로봇 SW")).strip()
    if not keyword:
        return []

    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))
    max_items = int(opts.get("max_items", 20))
    search_url = SARAMIN_SEARCH_URL.format(q=quote_plus(keyword))

    urls: List[str] = []
    seen = set()

    resp = request_with_retry("GET", search_url, timeout, retries, logger, log_failures=False)
    page_html = resp.text if resp else ""
    for u in _extract_rec_idx_urls_from_html(page_html):
        if u in seen:
            continue
        seen.add(u)
        urls.append(u)

    if not urls:
        render = get_render_policy(opts, default_timeout_ms=30000, default_wait_until="domcontentloaded", default_scroll_rounds=3)
        if render.get("enabled", True):
            pw_urls = search_links_with_playwright(
                start_url=search_url,
                link_regex=r"https?://(?:www\.)?saramin\.co\.kr/zf_user/jobs/(?:relay/)?view\?rec_idx=\d+",
                timeout_ms=int(render.get("timeout_ms", 30000)),
                logger=logger,
                wait_until=str(render.get("wait_until", "domcontentloaded")),
                scroll_rounds=int(render.get("scroll_rounds", 3)),
            )
            logger.info("source=saramin playwright links=%d", len(pw_urls))
            for u in pw_urls:
                rec_idx = _extract_rec_idx(u)
                if not rec_idx:
                    continue
                clean = f"https://www.saramin.co.kr/zf_user/jobs/view?rec_idx={rec_idx}"
                if clean in seen:
                    continue
                seen.add(clean)
                urls.append(clean)

    logger.info("source=saramin direct_search links=%d sample=%s", len(urls), urls[:3])
    return urls[:max_items]


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


def fetch_list(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    api_items = _fetch_from_api(opts, cfg, logger)
    if api_items:
        return api_items

    direct_urls = _fetch_direct_search_urls(opts, cfg, logger)
    if direct_urls:
        max_items = int(opts.get("max_items", 20))
        out = []
        for u in direct_urls[:max_items]:
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

    q = opts.get("query", {})
    keyword = q.get("keyword", "로봇 sw")
    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))
    search_query = f"{keyword} rec_idx"
    links = search_site_links("saramin.co.kr", search_query, timeout, retries, logger, cfg.get("search", {}))
    logger.info(
        "source=saramin links returned type=%s len=%d",
        type(links).__name__,
        len(links) if isinstance(links, list) else -1,
    )
    if not links:
        query_candidates = [
            f"{keyword} rec_idx",
            "site:saramin.co.kr zf_user jobs view rec_idx",
            "site:saramin.co.kr zf_user jobs relay view rec_idx",
        ]
        seen = set()
        for qx in query_candidates:
            for u in search_multi_domains(["saramin.co.kr"], qx, timeout, retries, logger, cfg.get("search", {})):
                if u in seen:
                    continue
                seen.add(u)
                links.append(u)
    logger.info("source=saramin search returned urls sample=%s", links[:3])
    urls = []
    for link in links:
        rec_idx = _extract_rec_idx(link)
        if not rec_idx:
            continue
        urls.append(f"https://www.saramin.co.kr/zf_user/jobs/view?rec_idx={rec_idx}")
    urls = list(dict.fromkeys(urls))
    logger.info("source=saramin search_links=%d normalized=%d", len(links), len(urls))

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
    if not bool(opts.get("detail_fetch_enabled", False)):
        return {}

    url = _normalize_detail_url(str(item.get("url", "")))
    timeout = int(opts.get("detail_http_timeout_sec", max(20, int(cfg.get("network", {}).get("timeout_sec", 10)))))
    retries = int(opts.get("detail_http_retries", 1))
    resp = request_with_retry("GET", url, timeout, retries, logger, log_failures=False)
    page_html = resp.text if resp else ""

    if not page_html:
        return {}

    title = ""
    m = _META_OG_TITLE_RE.search(page_html)
    if m:
        title = html.unescape(m.group(1).strip())
    if not title:
        m = _META_TW_TITLE_RE.search(page_html)
        if m:
            title = html.unescape(m.group(1).strip())
    if not title:
        m = _TITLE_RE.search(page_html)
        if m:
            title = html.unescape(m.group(1).strip())

    jsonld = _extract_jsonld_objects(page_html)
    company = _pick_company_from_jsonld(jsonld)
    if not company:
        company = _pick_company_from_title(title)
    if not company:
        company = "Unknown"

    location = _pick_location_from_jsonld(jsonld)
    if not location:
        location = _pick_location_from_text(page_html)
    if not location:
        location = "미상"

    deadline_match = _DEADLINE_RE.search(page_html)
    deadline = deadline_match.group(1).replace(".", "-").replace("/", "-") if deadline_match else ""

    desc = ""
    dm = _META_OG_DESC_RE.search(page_html)
    if dm:
        desc = html.unescape(dm.group(1).strip())
    if not desc:
        desc = _strip_tags(page_html)[:2200]

    employment_type = "정규직"
    for o in jsonld:
        if o.get("@type") in ("JobPosting", "jobPosting") and o.get("employmentType"):
            employment_type = str(o.get("employmentType")).strip()
            break

    status_text = ""
    if any(x in desc for x in ("모집중", "접수중", "진행중")):
        status_text = "모집중"

    return {
        "title": (title or "Saramin Robotics Position")[:120],
        "company": company,
        "location": location,
        "employment_type": employment_type,
        "deadline": deadline,
        "status_text": status_text,
        "description": desc,
    }
