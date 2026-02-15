import html
import re
from typing import Dict, Iterable, List, Optional
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

import requests

SEARCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}


SEARCH_ENGINE_HOST_SUFFIXES = (
    "bing.com",
    "duckduckgo.com",
    "search.brave.com",
    "google.com",
    "yahoo.com",
    "microsoft.com",
)

_BING_ALGO_A_RE = re.compile(
    r"<li[^>]*class=[\"'][^\"']*\bb_algo\b[^\"']*[\"'][^>]*>.*?<a[^>]*href=[\"']([^\"']+)[\"']",
    re.I | re.S,
)


def _host_of(url: str) -> str:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def _is_search_engine_url(url: str) -> bool:
    host = _host_of(url)
    if not host:
        return False
    return any(host == s or host.endswith(f".{s}") for s in SEARCH_ENGINE_HOST_SUFFIXES)


def _normalize_domain(d: str) -> str:
    host = (d or "").strip().lower().lstrip(".")
    if host.startswith("www."):
        host = host[4:]
    return host


def _domain_url_regex(domain: str) -> re.Pattern:
    d = re.escape(_normalize_domain(domain))
    return re.compile(rf"https?://(?:[\w-]+\.)*{d}(?:/[^\s\"'<>]+)?", re.I)


def extract_bing_result_urls(page_html: str, base_url: str, allowed_domains: Iterable[str]) -> List[str]:
    if not page_html:
        return []

    txt = html.unescape(page_html)
    ds = []
    for d in allowed_domains:
        norm = _normalize_domain(d)
        if norm:
            ds.append(norm)
    cand: List[str] = []

    for href in _BING_ALGO_A_RE.findall(txt):
        u = html.unescape((href or "").strip())
        if not u:
            continue
        if u.startswith("/"):
            u = urljoin(base_url, u)
        for _ in range(2):
            u = unquote(u)
        if "uddg=" in u:
            u = u.split("uddg=", 1)[1].split("&", 1)[0]
            for _ in range(2):
                u = unquote(u)
        cand.append(u)

    if not cand and ds:
        for d in ds:
            for u in _domain_url_regex(d).findall(txt):
                cand.append(html.unescape(u.strip()))

    out: List[str] = []
    seen = set()
    for u in cand:
        if not (u.startswith("http://") or u.startswith("https://")):
            continue
        host = _host_of(u)
        if not host:
            continue
        if _is_search_engine_url(u):
            continue
        if ds and not any(host == d or host.endswith(f".{d}") for d in ds):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _filter_allowed_domain_urls(links: Iterable[str], allowed_domains: Iterable[str]) -> List[str]:
    ds = []
    for d in allowed_domains:
        norm = _normalize_domain(d)
        if norm:
            ds.append(norm)
    out: List[str] = []
    seen = set()
    for raw in links:
        u = html.unescape((raw or "").strip())
        for _ in range(2):
            u = unquote(u)
        if not (u.startswith("http://") or u.startswith("https://")):
            continue
        host = _host_of(u)
        if not host:
            continue
        if _is_search_engine_url(u):
            continue
        if ds and not any(host == d or host.endswith(f".{d}") for d in ds):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _extract_bing_result_urls_playwright(
    query: str,
    allowed_domains: Iterable[str],
    timeout_ms: int,
    wait_until: str,
    scroll_rounds: int,
    logger,
) -> List[str]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return []

    start_url = _provider_url("bing", query)
    if not start_url:
        return []

    links: List[str] = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(start_url, wait_until=wait_until, timeout=timeout_ms)
            for _ in range(max(0, int(scroll_rounds))):
                page.mouse.wheel(0, 4000)
                page.wait_for_timeout(500)

            selectors = [
                "li.b_algo h2 a[href]",
                "li.b_algo a[href]",
                "main a[href]",
            ]
            for sel in selectors:
                hrefs = page.eval_on_selector_all(sel, "els => els.map(e => e.href)")
                for h in hrefs or []:
                    if isinstance(h, str) and h:
                        links.append(h)
                if links:
                    break
            browser.close()
    except Exception as exc:
        logger.info("bing playwright failed err=%s", exc)
        return []

    return _filter_allowed_domain_urls(links, allowed_domains)


def _extract_links(page_html: str) -> List[str]:
    raw = page_html or ""
    links: List[str] = []

    # 1) href attributes (single/double quote)
    for m in re.findall(r"href=[\"']([^\"']+)[\"']", raw, flags=re.I):
        links.append(m)

    # 2) plain absolute URLs in body/script JSON
    for m in re.findall(r"https?://[^\s\"'<>]+", raw, flags=re.I):
        links.append(m)

    # 3) rec_idx fragments from encoded payload (Saramin-friendly)
    for rec in re.findall(r"rec_idx(?:=|%3D)(\d+)", raw, flags=re.I):
        links.append(f"https://www.saramin.co.kr/zf_user/jobs/view?rec_idx={rec}")

    out: List[str] = []
    seen = set()
    for link in links:
        u = html.unescape((link or "").strip())
        for _ in range(2):
            u = unquote(u)
        if u.startswith("/url?"):
            parsed = urlparse(u)
            q = parse_qs(parsed.query)
            cand = (q.get("q", [""]) or [""])[0] or (q.get("url", [""]) or [""])[0]
            if cand:
                u = cand
        elif u.startswith("//"):
            u = f"https:{u}"
        if "uddg=" in u:
            u = u.split("uddg=", 1)[1].split("&", 1)[0]
            for _ in range(2):
                u = unquote(u)
        if not (u.startswith("http://") or u.startswith("https://")):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _provider_url(provider: str, query: str) -> Optional[str]:
    q = quote_plus(query)
    p = (provider or "").lower()
    if p == "duckduckgo":
        return f"https://duckduckgo.com/html/?q={q}"
    if p == "bing":
        return f"https://www.bing.com/search?q={q}"
    if p == "brave":
        return f"https://search.brave.com/search?q={q}&source=web"
    return None


def web_search(
    query: str,
    providers: Iterable[str],
    timeout: int,
    retries: int,
    logger,
    allowed_domains: Optional[Iterable[str]] = None,
    search_cfg: Optional[Dict] = None,
) -> List[str]:
    out: List[str] = []
    seen = set()

    for provider in providers:
        url = _provider_url(provider, query)
        if not url:
            continue

        resp = None
        for _ in range(max(1, retries + 1)):
            try:
                resp = requests.get(url, timeout=timeout, headers=SEARCH_HEADERS)
                if 200 <= resp.status_code < 400:
                    break
                resp = None
            except Exception:
                resp = None

        if not resp:
            continue

        content_type = (resp.headers.get("content-type", "") or "").split(";", 1)[0].strip()
        body = resp.text or ""
        logger.info(
            "search provider=%s status=%d ct=%s body_len=%d body_preview=%s",
            provider,
            resp.status_code,
            content_type or "-",
            len(body),
            re.sub(r"\s+", " ", body[:800]).strip(),
        )

        if (provider or "").lower() == "bing":
            b_algo_hits = len(_BING_ALGO_A_RE.findall(body))
            links = extract_bing_result_urls(body, resp.url, allowed_domains or [])
            logger.info("bing b_algo_hits=%d", b_algo_hits)
            logger.info("bing result_urls=%d sample=%s", len(links), links[:5])
            bing_pw_enabled = bool((search_cfg or {}).get("bing_playwright_fallback", True))
            if bing_pw_enabled and b_algo_hits == 0 and not links:
                pw_timeout_ms = int((search_cfg or {}).get("bing_playwright_timeout_ms", 15000))
                pw_wait_until = str((search_cfg or {}).get("bing_playwright_wait_until", "domcontentloaded"))
                pw_scroll_rounds = int((search_cfg or {}).get("bing_playwright_scroll_rounds", 2))
                pw_links = _extract_bing_result_urls_playwright(
                    query=query,
                    allowed_domains=allowed_domains or [],
                    timeout_ms=pw_timeout_ms,
                    wait_until=pw_wait_until,
                    scroll_rounds=pw_scroll_rounds,
                    logger=logger,
                )
                logger.info("bing playwright result_urls=%d sample=%s", len(pw_links), pw_links[:5])
                if pw_links:
                    links = pw_links
        else:
            links = _extract_links(body)
        logger.info("search provider=%s extracted_links=%d sample=%s", provider, len(links), links[:3])
        for link in links:
            clean = html.unescape(link.strip())
            if clean in seen:
                continue
            seen.add(clean)
            out.append(clean)

    logger.info("search query='%s' links=%d", query[:80], len(out))
    return out


def domain_filter(links: Iterable[str], domains: Iterable[str]) -> List[str]:
    ds = []
    for d in domains:
        norm = _normalize_domain(d)
        if norm:
            ds.append(norm)

    out: List[str] = []
    seen = set()
    for link in links:
        raw = html.unescape((link or "").strip())
        for _ in range(2):
            raw = unquote(raw)
        if not (raw.startswith("http://") or raw.startswith("https://")):
            continue
        if _is_search_engine_url(raw):
            continue
        host = _host_of(raw)
        if not host:
            continue
        if not any(host == d or host.endswith(f".{d}") for d in ds):
            continue
        if raw in seen:
            continue
        seen.add(raw)
        out.append(raw)
    return out


def search_links(
    query: str,
    domains: List[str],
    search_cfg: Dict,
    logger,
) -> List[str]:
    providers = search_cfg.get("providers", ["duckduckgo", "bing"])
    timeout = int(search_cfg.get("timeout_sec", 4))
    retries = int(search_cfg.get("retries", 0))

    # 1) broad query first
    broad_links = web_search(query, providers, timeout, retries, logger, domains, search_cfg)
    filtered = domain_filter(broad_links, domains)
    logger.info("search filtered query='%s' domains=%s count=%d sample=%s", query[:80], domains, len(filtered), filtered[:3])

    # 2) site-constrained fallback per domain
    if filtered:
        logger.info("search return filtered len=%d sample=%s", len(filtered), filtered[:3])
        return filtered

    # Strip any existing site: operators from the query to avoid duplication
    clean_query = re.sub(r"site:\S+\s*", "", query).strip()
    if not clean_query:
        clean_query = query

    merged: List[str] = []
    seen = set()
    for d in domains:
        d_host = _normalize_domain(d)
        q = f"site:{d} {clean_query}"
        for link in web_search(q, providers, timeout, retries, logger, [d_host], search_cfg):
            if link in seen:
                continue
            if _is_search_engine_url(link):
                continue
            host = _host_of(link)
            if not host:
                continue
            if not (host == d_host or host.endswith(f".{d_host}")):
                continue
            seen.add(link)
            merged.append(link)
    logger.info("search return merged len=%d sample=%s", len(merged), merged[:3])
    return merged
