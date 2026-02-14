from email.utils import parsedate_to_datetime
from typing import Any, Dict, List
from xml.etree import ElementTree as ET

from crawlers.common import request_with_retry
from core.normalize import normalize_text


def _get_text(parent, tag: str) -> str:
    elem = parent.find(tag)
    return elem.text.strip() if elem is not None and elem.text else ""


def _sample() -> List[Dict[str, Any]]:
    return [
        {
            "_sample": True,
            "source_job_id": "remoteok-sample-1",
            "url": "https://remoteok.com/remote-jobs/1234-robotics-software-engineer-sample",
            "title": "Robotics Software Engineer (Entry)",
            "company": "Sample Robotics",
            "location": "Remote",
            "employment_type": "정규직",
            "posted_at": "2026-02-14",
            "deadline": "2026-12-31",
            "is_open": True,
            "status_text": "Open",
            "description": "ROS2, SLAM, perception, bachelor degree, entry level, autonomous robot stack",
        }
    ]


def fetch_list(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    rss_urls = [
        opts.get("rss_url", "https://remoteok.com/remote-dev+robotics-jobs.rss"),
        "https://remoteok.com/remote-robotics-jobs.rss",
        "https://remoteok.com/remote-engineer-jobs.rss",
    ]
    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))

    resp = None
    for rss_url in rss_urls:
        resp = request_with_retry("GET", rss_url, timeout, retries, logger)
        if resp:
            break
    if not resp:
        return _sample() if opts.get("sample_on_failure", True) else []

    try:
        root = ET.fromstring(resp.text)
    except Exception:
        logger.exception("remoteok parse failed")
        return _sample() if opts.get("sample_on_failure", True) else []

    items = root.findall(".//item")
    q = opts.get("query", {})
    keyword = normalize_text(q.get("keyword", "robotics robot ros slam autonomous"))
    # Use ANY match (OR) instead of requiring specific terms
    match_terms = [x for x in keyword.split(" ") if len(x) >= 2]
    results: List[Dict[str, Any]] = []
    for item in items:
        title = _get_text(item, "title")
        link = _get_text(item, "link")
        desc = _get_text(item, "description")
        pub = _get_text(item, "pubDate")
        try:
            posted_at = parsedate_to_datetime(pub).strftime("%Y-%m-%d") if pub else ""
        except Exception:
            posted_at = ""

        blob = normalize_text(f"{title} {desc}")
        if match_terms and not any(t in blob for t in match_terms):
            continue

        results.append(
            {
                "source_job_id": link.rsplit("/", 1)[-1],
                "url": link,
                "title": title,
                "company": "Unknown",
                "location": "Remote",
                "employment_type": "정규직",
                "posted_at": posted_at,
                "status_text": "Open",
                "description": desc,
            }
        )

    if not results and opts.get("sample_on_failure", True):
        return _sample()
    return results


def fetch_detail(item: Dict[str, Any], opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> Dict[str, Any]:
    return item


def crawl(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    return fetch_list(opts, cfg, logger)
