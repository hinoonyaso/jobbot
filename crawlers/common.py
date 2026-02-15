from __future__ import annotations

import html
import json
import os
import re
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, unquote, urlencode, urljoin, urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from core.normalize import normalize_text
from core.searcher import search_links as _search_links
from core.schema import Job, today_str


DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) jobbot/0.2 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
}

DEFAULT_CONNECT_TIMEOUT_SEC = 5
DEFAULT_READ_TIMEOUT_SEC = 25
DEBUG_BODY_PREVIEW_CHARS = 2000
DEBUG_DUMP_MAX_BYTES = 512_000
DEBUG_RETENTION_DAYS = 2
RETRY_STATUS_CODES = (429, 500, 502, 503, 504)
POOL_CONNECTIONS = 20
POOL_MAXSIZE = 50

_SESSIONS: Dict[int, requests.Session] = {}
_SESSION_LOCK = threading.Lock()
_DEBUG_CLEANED_DIRS = set()


CLOSED_KEYWORDS = ["마감", "종료", "closed", "expired"]
OPEN_KEYWORDS = ["모집중", "진행중", "채용중", "open", "active"]
DATE_PATTERNS = [r"(20\d{2})[./-](\d{1,2})[./-](\d{1,2})", r"(\d{2})[./-](\d{1,2})[./-](\d{1,2})"]


def request_with_retry(
    method: str,
    url: str,
    timeout: int,
    retries: int,
    logger,
    log_failures: bool = True,
    **kwargs,
) -> Optional[requests.Response]:
    meta = kwargs.pop("_meta", None)
    if isinstance(meta, dict):
        meta.clear()
    headers = dict(DEFAULT_HEADERS)
    req_headers = kwargs.pop("headers", None)
    if isinstance(req_headers, dict):
        headers.update(req_headers)
    if "Referer" not in headers:
        parsed = urlparse(url or "")
        if parsed.scheme and parsed.netloc:
            headers["Referer"] = f"{parsed.scheme}://{parsed.netloc}/"

    session = _get_session(retries)
    req_timeout = _normalize_timeout(timeout)
    try:
        resp = session.request(method, url, timeout=req_timeout, headers=headers, **kwargs)
    except Exception as exc:
        if isinstance(meta, dict):
            meta.update({"error": str(exc), "url": url})
        if log_failures:
            logger.warning("request failed url=%s err=%s", url, exc)
        return None

    if 200 <= resp.status_code < 400:
        if isinstance(meta, dict):
            meta.update(
                {
                    "status_code": resp.status_code,
                    "reason": resp.reason,
                    "url": resp.url,
                    "content_type": resp.headers.get("content-type", ""),
                }
            )
        return resp

    dump_path = _dump_fail_response(resp, logger)
    if isinstance(meta, dict):
        meta.update(
            {
                "status_code": resp.status_code,
                "reason": resp.reason,
                "url": resp.url,
                "content_type": resp.headers.get("content-type", ""),
                "dump_path": dump_path,
            }
        )
    if log_failures:
        logger.warning(
            "request failed status=%s reason=%s url=%s final_url=%s ct=%s bytes=%d body_preview=%s dump=%s",
            resp.status_code,
            resp.reason,
            url,
            resp.url,
            (resp.headers.get("content-type", "") or "").split(";")[0].strip(),
            len(resp.content or b""),
            _body_preview(resp),
            dump_path or "-",
        )
    return None


def _normalize_timeout(timeout: Any) -> Any:
    if isinstance(timeout, tuple) and len(timeout) == 2:
        return timeout
    try:
        read_timeout = max(DEFAULT_CONNECT_TIMEOUT_SEC, int(timeout))
    except Exception:
        read_timeout = DEFAULT_READ_TIMEOUT_SEC
    return (DEFAULT_CONNECT_TIMEOUT_SEC, read_timeout)


def _get_session(retries: int) -> requests.Session:
    retry_total = max(0, int(retries))
    with _SESSION_LOCK:
        existing = _SESSIONS.get(retry_total)
        if existing is not None:
            return existing

        session = requests.Session()
        retry_cfg = Retry(
            total=retry_total,
            connect=retry_total,
            read=retry_total,
            status=retry_total,
            backoff_factor=0.6,
            status_forcelist=RETRY_STATUS_CODES,
            allowed_methods=frozenset(["HEAD", "GET", "OPTIONS", "POST"]),
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry_cfg, pool_connections=POOL_CONNECTIONS, pool_maxsize=POOL_MAXSIZE)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _SESSIONS[retry_total] = session
        return session


def _body_preview(resp: requests.Response) -> str:
    ct = (resp.headers.get("content-type", "") or "").lower()
    if "text" in ct or "json" in ct or "xml" in ct or "javascript" in ct:
        preview = (resp.text or "")[:DEBUG_BODY_PREVIEW_CHARS]
    else:
        preview = (resp.content[:256] if resp.content else b"").hex()
    return re.sub(r"\s+", " ", preview).strip()


def _dump_fail_response(resp: requests.Response, logger) -> str:
    if os.getenv("JOBBOT_DUMP_FAIL_RESPONSES", "1").strip().lower() not in {"1", "true", "yes", "on"}:
        return ""

    base_dir = os.getenv("JOBBOT_DEBUG_DIR", "data/debug")
    day = datetime.now().strftime("%Y%m%d")
    host = (urlparse(resp.url or "").netloc or "unknown").replace(":", "_")
    status = str(resp.status_code or "0")
    ts = datetime.now().strftime("%H%M%S")
    slug = _url_slug(resp.url or "")
    ext = _content_ext(resp.headers.get("content-type", ""))
    out_dir = os.path.join(base_dir, day, host)
    os.makedirs(out_dir, exist_ok=True)
    _cleanup_old_debug_dirs(base_dir, logger)

    body_path = os.path.join(out_dir, f"{slug}_{status}_{ts}{ext}")
    meta_path = os.path.join(out_dir, f"{slug}_{status}_{ts}.meta.json")
    body = resp.content or b""
    if len(body) > DEBUG_DUMP_MAX_BYTES:
        body = body[:DEBUG_DUMP_MAX_BYTES]
    with open(body_path, "wb") as f:
        f.write(body)

    meta = {
        "status_code": resp.status_code,
        "reason": resp.reason,
        "url": resp.url,
        "content_type": resp.headers.get("content-type", ""),
        "content_length": len(resp.content or b""),
        "headers": dict(resp.headers),
    }
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    return body_path


def _cleanup_old_debug_dirs(base_dir: str, logger) -> None:
    if base_dir in _DEBUG_CLEANED_DIRS:
        return
    _DEBUG_CLEANED_DIRS.add(base_dir)
    if not os.path.isdir(base_dir):
        return

    now = datetime.now()
    for day_name in os.listdir(base_dir):
        day_path = os.path.join(base_dir, day_name)
        if not os.path.isdir(day_path):
            continue
        try:
            day_dt = datetime.strptime(day_name, "%Y%m%d")
        except Exception:
            continue
        if (now - day_dt).days <= DEBUG_RETENTION_DAYS:
            continue
        try:
            for root, _, files in os.walk(day_path, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                if root != day_path:
                    os.rmdir(root)
            os.rmdir(day_path)
        except Exception as exc:
            logger.info("debug cleanup failed path=%s err=%s", day_path, exc)


def _url_slug(url: str) -> str:
    parsed = urlparse(url or "")
    path = (parsed.path or "").strip("/") or "root"
    parts = [p for p in path.split("/") if p]
    raw = "_".join(parts[-2:]) if parts else "root"
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", raw).strip("_")
    return (slug or "page")[:60]


def _content_ext(content_type: str) -> str:
    ct = (content_type or "").lower()
    if "html" in ct:
        return ".html"
    if "json" in ct:
        return ".json"
    if "xml" in ct:
        return ".xml"
    if "text" in ct:
        return ".txt"
    return ".bin"


def canonical_url(url: str) -> str:
    if not url:
        return ""
    normalized = html.unescape(url.strip())
    parsed = urlparse(normalized)
    query = []
    for k, v in parse_qsl(parsed.query, keep_blank_values=True):
        key = (k or "").strip()
        if key.lower().startswith("amp;"):
            key = key[4:]
        if not key or key.lower().startswith("utm_"):
            continue
        query.append((key, v))
    path = parsed.path.rstrip("/")
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), path, "", urlencode(query), ""))


def infer_employment_type(text: str) -> str:
    t = normalize_text(text)
    if "인턴" in t or "intern" in t:
        return "인턴"
    if "계약" in t or "contract" in t:
        return "계약직"
    if "정규" in t or "full-time" in t or "full time" in t:
        return "정규직"
    return "정규직"


def parse_deadline(text: str) -> str:
    t = text or ""
    for pattern in DATE_PATTERNS:
        m = re.search(pattern, t)
        if not m:
            continue
        y, mm, dd = m.groups()
        year = int(y)
        if year < 100:
            year += 2000
        try:
            dt = datetime(year, int(mm), int(dd))
            return dt.strftime("%Y-%m-%d")
        except Exception:
            continue
    return ""


def infer_open_status(status_text: str, deadline: str) -> bool:
    t = normalize_text(status_text)
    if any(k in t for k in CLOSED_KEYWORDS):
        return False
    if any(k in t for k in OPEN_KEYWORDS):
        return True

    if deadline:
        try:
            return datetime.now().date() <= datetime.strptime(deadline, "%Y-%m-%d").date()
        except Exception:
            return True
    return True


def normalize_job(raw: Dict[str, Any], source: str) -> Job:
    text_blob = " ".join(
        [
            str(raw.get("title", "")),
            str(raw.get("description", "")),
            str(raw.get("employment_type", "")),
            str(raw.get("status_text", "")),
            str(raw.get("deadline", "")),
        ]
    )
    deadline = str(raw.get("deadline", "")).strip() or parse_deadline(text_blob)
    status_text = str(raw.get("status_text", "")).strip()

    return Job(
        source=source,
        url=canonical_url(str(raw.get("url", "")).strip()),
        title=re.sub(r"\s+", " ", str(raw.get("title", "")).strip()),
        company=str(raw.get("company", "")).strip() or "Unknown",
        location=str(raw.get("location", "")).strip() or "미상",
        employment_type=str(raw.get("employment_type", "")).strip() or infer_employment_type(text_blob),
        posted_at=str(raw.get("posted_at", "")).strip() or today_str(),
        description=re.sub(r"\s+", " ", str(raw.get("description", "")).strip()),
        source_job_id=str(raw.get("source_job_id", "")).strip(),
        deadline=deadline,
        # Do not infer close/open from full description text; noisy words like "마감" in boilerplate can cause false close.
        is_open=bool(raw.get("is_open", infer_open_status(status_text, deadline))),
        status_text=status_text,
    )


def is_live_url(url: str, timeout: int, logger) -> bool:
    try:
        resp = requests.get(url, timeout=timeout, headers=DEFAULT_HEADERS, allow_redirects=True)
        return 200 <= resp.status_code < 400
    except Exception:
        return False


def _extract_ddg_links(page_html: str) -> List[str]:
    links: List[str] = []
    for m in re.findall(r'href="([^"]+)"', page_html or "", flags=re.I):
        if "uddg=" in m:
            target = m.split("uddg=", 1)[1].split("&", 1)[0]
            links.append(unquote(target))
        elif m.startswith("http://") or m.startswith("https://"):
            links.append(m)
    return links


def search_site_links(site: str, query: str, timeout: int, retries: int, logger, search_cfg: Optional[Dict[str, Any]] = None) -> List[str]:
    search_cfg = search_cfg or {"providers": ["duckduckgo", "bing", "brave"], "timeout_sec": min(timeout, 4), "retries": retries}
    links = _search_links(f"site:{site} {query}", [site], search_cfg, logger)
    logger.info(
        "search_site_links returned type=%s len=%d sample=%s",
        type(links).__name__,
        len(links) if isinstance(links, list) else -1,
        links[:3] if isinstance(links, list) else "-",
    )
    return links if isinstance(links, list) else []


_TITLE_RE = re.compile(r"<title>(.*?)</title>", re.I | re.S)
_META_DESC_RE = re.compile(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)', re.I)
_OG_DESC_RE = re.compile(r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)', re.I)
_SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b[^>]*>.*?</\1>", re.I | re.S)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_REGION_RE = re.compile(r"(용인|성남|강남|분당|수지|판교|서울|경기|remote|재택)", re.I)


def clean_html_text(page_html: str) -> str:
    text = _SCRIPT_STYLE_RE.sub(" ", page_html or "")
    text = _TAG_RE.sub(" ", text)
    text = html.unescape(text)
    return _WS_RE.sub(" ", text).strip()


def parse_title_description(page_html: str) -> Dict[str, str]:
    raw = page_html or ""
    title_match = _TITLE_RE.search(raw)
    meta_match = _META_DESC_RE.search(raw) or _OG_DESC_RE.search(raw)
    title = html.unescape(title_match.group(1).strip()) if title_match else ""
    cleaned = clean_html_text(raw)
    desc = html.unescape(meta_match.group(1).strip()) if meta_match else cleaned[:2200]
    desc = desc.replace("window.process", " ").replace("NEXT_PUBLIC_", " ")
    desc = _WS_RE.sub(" ", desc).strip()
    return {"title": title[:160], "description": desc[:2600]}


def infer_region(text: str) -> str:
    m = _REGION_RE.search(text or "")
    return m.group(1) if m else "미상"


def search_multi_domains(domains: List[str], query: str, timeout: int, retries: int, logger, search_cfg: Optional[Dict[str, Any]] = None) -> List[str]:
    search_cfg = search_cfg or {"providers": ["duckduckgo", "bing", "brave"], "timeout_sec": min(timeout, 4), "retries": retries}
    links = _search_links(query, domains, search_cfg, logger)
    logger.info(
        "search_multi_domains returned type=%s len=%d sample=%s",
        type(links).__name__,
        len(links) if isinstance(links, list) else -1,
        links[:3] if isinstance(links, list) else "-",
    )
    return links if isinstance(links, list) else []


def search_links_with_playwright(
    start_url: str,
    link_regex: str,
    timeout_ms: int,
    logger,
    wait_until: str = "domcontentloaded",
    scroll_rounds: int = 2,
) -> List[str]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return []

    pattern = re.compile(link_regex, re.I)
    links: List[str] = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(start_url, wait_until=wait_until, timeout=timeout_ms)
            for _ in range(max(0, int(scroll_rounds))):
                page.mouse.wheel(0, 5000)
                page.wait_for_timeout(600)
            hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
            for href in hrefs or []:
                if not isinstance(href, str):
                    continue
                if pattern.search(href):
                    links.append(href)
            browser.close()
    except Exception as exc:
        logger.info("playwright fallback failed url=%s err=%s", start_url, exc)
        return []

    uniq = []
    seen = set()
    for u in links:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    return uniq


def get_render_policy(
    opts: Dict[str, Any],
    default_timeout_ms: int = 15000,
    default_wait_until: str = "domcontentloaded",
    default_scroll_rounds: int = 2,
) -> Dict[str, Any]:
    render = opts.get("render", {}) if isinstance(opts, dict) else {}
    return {
        "enabled": bool(render.get("enabled", True)),
        "timeout_ms": int(render.get("timeout_ms", default_timeout_ms)),
        "wait_until": str(render.get("wait_until", default_wait_until)),
        "scroll_rounds": int(render.get("scroll_rounds", default_scroll_rounds)),
    }


def same_host_or_relative(base_url: str, href: str) -> str:
    if not href:
        return ""
    abs_url = urljoin(base_url, href)
    try:
        b = urlparse(base_url).netloc.lower()
        a = urlparse(abs_url).netloc.lower()
    except Exception:
        return ""
    if not a or a == b:
        return abs_url
    return ""
