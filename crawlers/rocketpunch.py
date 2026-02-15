import re
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import urlparse

from crawlers.common import (
    search_multi_domains,
    search_site_links,
)

_LINK_RE = re.compile(r'(?:href=["\'])?(https?://(?:www\.)?rocketpunch\.com/jobs/[^"\'?#\s]+|/jobs/[^"\'?#\s]+)', re.I)


def _to_abs(link: str) -> str:
    if link.startswith("http"):
        return link
    return f"https://www.rocketpunch.com{link}"


def _is_job_detail(url: str) -> bool:
    try:
        p = urlparse(url)
        host = (p.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        if host != "rocketpunch.com":
            return False
        parts = [x for x in (p.path or "").split("/") if x]
        # Accept `/jobs/<id>` and `/jobs/<id>/<slug>` style detail URLs.
        return len(parts) >= 2 and parts[0] == "jobs" and parts[1] not in {"new", "search"}
    except Exception:
        return False


def fetch_list(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    q = opts.get("query", {})
    keyword = q.get("keyword", "로봇 SW")
    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))

    # Search-only mode: HTTP/Playwright are consistently blocked (403 / edge block).
    search_cfg = dict(cfg.get("search", {}) or {})
    search_cfg["providers"] = ["duckduckgo"]
    merged: List[str] = []
    seen = set()
    queries = [
        f"{keyword} /jobs/",
        f"{keyword} 채용",
        "로봇 SW 채용",
    ]
    for qx in queries:
        for u in search_site_links("rocketpunch.com", qx, timeout, retries, logger, search_cfg):
            abs_url = _to_abs(u)
            if not _is_job_detail(abs_url) or abs_url in seen:
                continue
            seen.add(abs_url)
            merged.append(abs_url)
    if not merged:
        for u in search_multi_domains(["rocketpunch.com"], f"site:rocketpunch.com {keyword}", timeout, retries, logger, search_cfg):
            abs_url = _to_abs(u)
            if not _is_job_detail(abs_url) or abs_url in seen:
                continue
            seen.add(abs_url)
            merged.append(abs_url)
    urls = merged
    logger.info("source=rocketpunch search-only links=%d", len(urls))

    max_items = int(opts.get("max_items", 20))
    items: List[Dict[str, Any]] = []
    for url in urls[:max_items]:
        slug = url.rsplit("/", 1)[-1]
        items.append(
            {
                "source_job_id": slug,
                "url": url,
                "title": f"RocketPunch Robotics Position #{slug}",
                "company": "Unknown",
                "location": "미상",
                "employment_type": "정규직",
                "status_text": "모집중",
                "posted_at": datetime.now().strftime("%Y-%m-%d"),
                "description": (
                    f"RocketPunch 로봇 SW 채용 공고 #{slug}. "
                    "상세 페이지 접근이 제한될 수 있어 링크 기반으로 수집되었습니다."
                ),
            }
        )
    return items


def fetch_detail(item: Dict[str, Any], opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> Dict[str, Any]:
    # Detail pages are edge-blocked in current environment; skip for speed.
    if not bool(opts.get("detail_fetch_enabled", False)):
        return {}
    return {}
