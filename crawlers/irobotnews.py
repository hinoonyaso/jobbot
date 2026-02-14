from datetime import datetime
from typing import Any, Dict, List

from crawlers.common import infer_region, is_live_url, parse_title_description, request_with_retry, search_multi_domains

DOMAINS = ["irobotnews.com"]


def _valid(link: str) -> bool:
    l = link.lower()
    return "irobotnews.com" in l and any(k in l for k in ["news", "article", "view", "archives"])


def fetch_list(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    q = opts.get("query", {})
    keyword = q.get("keyword", "로봇 채용 개발자 ros")
    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))

    links = [u for u in search_multi_domains(DOMAINS, keyword, timeout, retries, logger, cfg.get("search", {})) if _valid(u)]
    max_items = int(opts.get("max_items", 10))
    items: List[Dict[str, Any]] = []
    for url in links[: max_items * 3]:
        if not is_live_url(url, timeout, logger):
            continue
        items.append({"source_job_id": url.rsplit("/", 1)[-1], "url": url, "posted_at": datetime.now().strftime("%Y-%m-%d")})
        if len(items) >= max_items:
            break
    return items


def fetch_detail(item: Dict[str, Any], opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> Dict[str, Any]:
    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))
    resp = request_with_retry("GET", item.get("url", ""), timeout, retries, logger)
    if not resp:
        return {}
    parsed = parse_title_description(resp.text)
    blob = f"{parsed.get('title','')} {parsed.get('description','')}"
    return {
        "title": parsed.get("title", ""),
        "company": "Unknown",
        "location": infer_region(blob),
        "employment_type": "정규직",
        "status_text": "모집중" if "모집" in blob else "",
        "description": parsed.get("description", ""),
    }
