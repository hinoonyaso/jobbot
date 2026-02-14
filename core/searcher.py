import html
import re
from typing import Dict, Iterable, List, Optional
from urllib.parse import quote_plus, unquote

import requests

SEARCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}


def _extract_links(page_html: str) -> List[str]:
    links: List[str] = []
    for m in re.findall(r'href="([^"]+)"', page_html or "", flags=re.I):
        if "uddg=" in m:
            target = m.split("uddg=", 1)[1].split("&", 1)[0]
            links.append(unquote(target))
        elif m.startswith("http://") or m.startswith("https://"):
            links.append(m)
    return links


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

        for link in _extract_links(resp.text):
            clean = html.unescape(link.strip())
            if clean in seen:
                continue
            seen.add(clean)
            out.append(clean)

    logger.info("search query='%s' links=%d", query[:80], len(out))
    return out


def domain_filter(links: Iterable[str], domains: Iterable[str]) -> List[str]:
    ds = [d.lower() for d in domains]
    out: List[str] = []
    seen = set()
    for link in links:
        l = (link or "").lower()
        if not any(d in l for d in ds):
            continue
        if link in seen:
            continue
        seen.add(link)
        out.append(link)
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
    broad_links = web_search(query, providers, timeout, retries, logger)
    filtered = domain_filter(broad_links, domains)

    # 2) site-constrained fallback per domain
    if filtered:
        return filtered

    # Strip any existing site: operators from the query to avoid duplication
    clean_query = re.sub(r"site:\S+\s*", "", query).strip()
    if not clean_query:
        clean_query = query

    merged: List[str] = []
    seen = set()
    for d in domains:
        q = f"site:{d} {clean_query}"
        for link in web_search(q, providers, timeout, retries, logger):
            if link in seen:
                continue
            if d.lower() not in link.lower():
                continue
            seen.add(link)
            merged.append(link)
    return merged
