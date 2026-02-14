import re
from html import unescape
from typing import Any, Dict, List
from urllib.parse import quote_plus

from crawlers.common import is_live_url, request_with_retry, search_site_links

_LINK_RE = re.compile(r"(https?://www\.jobkorea\.co\.kr/Recruit/GI_Read/\d+|/Recruit/GI_Read/\d+)", re.I)
_TITLE_RE = re.compile(r"<title>(.*?)</title>", re.I | re.S)
_DEADLINE_RE = re.compile(r"(20\d{2}[./-]\d{1,2}[./-]\d{1,2}).{0,20}(마감|까지|종료)", re.I)
_META_DESC_RE = re.compile(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)', re.I)
_OG_DESC_RE = re.compile(r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)', re.I)
_SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b[^>]*>.*?</\1>", re.I | re.S)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_COMPANY_IN_TITLE_RE = re.compile(r"\s*([^|\-]+?)\s*채용\s*[-|]\s*", re.I)
_REGION_RE = re.compile(r"(용인|성남|강남|분당|수지|판교)")


def _to_abs(url: str) -> str:
    if url.startswith("http"):
        return url
    return f"https://www.jobkorea.co.kr{url}"


def _extract_list_urls(page_html: str) -> List[str]:
    return list(dict.fromkeys([_to_abs(u) for u in _LINK_RE.findall(page_html or "")]))


def _extract_company_from_title(title: str) -> str:
    m = _COMPANY_IN_TITLE_RE.search(title or "")
    if m:
        return m.group(1).strip()
    return "Unknown"


def _extract_region(text: str) -> str:
    m = _REGION_RE.search(text or "")
    if m:
        return m.group(1)
    return "미상"


def _clean_html_text(html: str) -> str:
    text = _SCRIPT_STYLE_RE.sub(" ", html or "")
    text = _TAG_RE.sub(" ", text)
    text = unescape(text)
    text = _WS_RE.sub(" ", text).strip()
    return text


def fetch_list(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    q = opts.get("query", {})
    keyword = q.get("keyword", "로봇 소프트웨어")
    search_url = f"https://www.jobkorea.co.kr/Search/?stext={quote_plus(keyword)}"

    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))

    urls: List[str] = []
    resp = request_with_retry("GET", search_url, timeout, retries, logger)
    if resp:
        urls = _extract_list_urls(resp.text)

    if not urls:
        urls = [u for u in search_site_links("jobkorea.co.kr", keyword, timeout, retries, logger, cfg.get("search", {})) if "/Recruit/GI_Read/" in u]

    max_items = int(opts.get("max_items", 20))
    alive = [u for u in urls if is_live_url(u, timeout, logger)]
    return [{"url": u, "source_job_id": u.rsplit("/", 1)[-1]} for u in alive[:max_items]]


def fetch_detail(item: Dict[str, Any], opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> Dict[str, Any]:
    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))
    resp = request_with_retry("GET", item.get("url", ""), timeout, retries, logger)
    if not resp:
        return {}

    html = resp.text or ""
    title_match = _TITLE_RE.search(html)
    deadline_match = _DEADLINE_RE.search(html)
    meta_desc_match = _META_DESC_RE.search(html)
    og_desc_match = _OG_DESC_RE.search(html)
    title = (title_match.group(1).strip() if title_match else "JobKorea Robotics Position")[:140]
    cleaned = _clean_html_text(html)
    meta_desc = ""
    if meta_desc_match:
        meta_desc = unescape(meta_desc_match.group(1).strip())
    elif og_desc_match:
        meta_desc = unescape(og_desc_match.group(1).strip())
    desc = (meta_desc if len(meta_desc) >= 40 else cleaned)[:2600]
    desc = desc.replace("window.process", " ").replace("NEXT_PUBLIC_", " ")
    desc = _WS_RE.sub(" ", desc).strip()

    company = _extract_company_from_title(title)
    location = _extract_region(f"{title} {cleaned}")

    return {
        "title": title,
        "company": company,
        "location": location,
        "employment_type": "정규직",
        "deadline": deadline_match.group(1).replace(".", "-").replace("/", "-") if deadline_match else "",
        "status_text": "모집중" if "모집중" in desc or "진행중" in desc else "",
        "description": desc,
    }
