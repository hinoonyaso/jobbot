import html
import re
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List
from xml.etree import ElementTree as ET

from crawlers.common import request_with_retry
from core.normalize import normalize_text

ROBOT_REQUIRED_TERMS = ("robot", "robotics", "ros", "slam", "autonomous", "agv", "amr", "navigation", "perception")
BLOCKED_TERMS = (
    "security architect",
    "devops",
    "sre",
    "product designer",
    "sales",
    "marketing",
    "frontend",
    "front-end",
    "front end",
    "ui engineer",
    "ux engineer",
    "technical writing",
    "technical writer",
    "content writer",
    "manager",
)
COMPANY_HTML_PATTERNS = (
    re.compile(r"\bAt\s*<strong>\s*([^<]{2,80})\s*</strong>", re.I),
    re.compile(r"\bJoin\s*<strong>\s*([^<]{2,80})\s*</strong>", re.I),
)
ROLE_TOKENS = {
    "software",
    "engineer",
    "engineering",
    "senior",
    "junior",
    "staff",
    "lead",
    "manager",
    "frontend",
    "backend",
    "fullstack",
    "machine",
    "learning",
    "systems",
    "system",
    "developer",
    "development",
    "robot",
    "robotics",
    "autonomous",
    "vision",
    "localization",
    "technical",
    "writing",
    "productivity",
}


def _get_text(parent, tag: str) -> str:
    elem = parent.find(tag)
    return elem.text.strip() if elem is not None and elem.text else ""


def _cleanup_company(name: str) -> str:
    cleaned = re.sub(r"\s+", " ", html.unescape(name or "")).strip(" -|,")
    return cleaned[:80]


def _company_from_item_meta(item) -> str:
    tag_hints = ("source", "author", "creator", "company")
    for ch in list(item):
        tag = str(ch.tag).lower()
        if any(tag.endswith(h) for h in tag_hints):
            text = (ch.text or "").strip()
            cleaned = _cleanup_company(text)
            if cleaned:
                return cleaned
    return ""


def _company_from_description(desc: str) -> str:
    raw = html.unescape(desc or "")
    for pat in COMPANY_HTML_PATTERNS:
        m = pat.search(raw)
        if m:
            cleaned = _cleanup_company(m.group(1))
            if cleaned:
                return cleaned
    return ""


def _company_from_link(link: str, title: str) -> str:
    slug = (link or "").split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
    if not slug:
        return ""
    slug = re.sub(r"-\d+$", "", slug)
    if slug.startswith("remote-"):
        slug = slug[len("remote-") :]
    tokens = [t for t in slug.split("-") if t]
    if not tokens:
        return ""

    title_tokens = re.findall(r"[a-z0-9]+", normalize_text(title))
    i = 0
    while i < len(tokens) and i < len(title_tokens) and tokens[i] == title_tokens[i]:
        i += 1
    candidate = tokens[i:] if i < len(tokens) else tokens[-2:]
    while candidate and candidate[0] in ROLE_TOKENS:
        candidate = candidate[1:]
    if not candidate:
        return ""
    if len(candidate) > 4:
        candidate = candidate[-3:]

    words = []
    for tok in candidate:
        if tok in ("ai", "ml"):
            words.append(tok.upper())
        else:
            words.append(tok.capitalize())
    return _cleanup_company(" ".join(words))


def _extract_company(item, title: str, desc: str, link: str) -> str:
    return (
        _company_from_item_meta(item)
        or _company_from_description(desc)
        or _company_from_link(link, title)
    )


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

    fetched_feeds = []
    for rss_url in rss_urls:
        resp = request_with_retry("GET", rss_url, timeout, retries, logger)
        if not resp:
            continue
        try:
            root = ET.fromstring(resp.text)
            feed_items = root.findall(".//item")
            fetched_feeds.append((rss_url, feed_items))
        except Exception:
            logger.exception("remoteok parse failed url=%s", rss_url)
            continue

    if not fetched_feeds:
        logger.info("source=remoteok rss fetch failed all_urls=%d", len(rss_urls))
        return _sample() if opts.get("sample_on_failure", True) else []
    q = opts.get("query", {})
    keyword = normalize_text(q.get("keyword", "robotics robot ros slam autonomous"))
    # Use ANY match (OR) instead of requiring specific terms
    match_terms = [x for x in keyword.split(" ") if len(x) >= 2]
    results: List[Dict[str, Any]] = []
    drop_robot = 0
    drop_blocked = 0
    drop_match_terms = 0
    total_items = 0
    seen_links = set()
    feed_summaries = []
    for rss_url, items in fetched_feeds:
        feed_total = len(items)
        feed_kept = 0
        feed_drop_robot = 0
        feed_drop_blocked = 0
        feed_drop_match_terms = 0
        total_items += feed_total
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
            if not any(t in blob for t in ROBOT_REQUIRED_TERMS):
                drop_robot += 1
                feed_drop_robot += 1
                continue
            if any(t in blob for t in BLOCKED_TERMS):
                drop_blocked += 1
                feed_drop_blocked += 1
                continue
            if match_terms and not any(t in blob for t in match_terms):
                drop_match_terms += 1
                feed_drop_match_terms += 1
                continue
            if not link or link in seen_links:
                continue
            company = _extract_company(item, title, desc, link) or "Unknown"
            seen_links.add(link)
            feed_kept += 1
            results.append(
                {
                    "source_job_id": link.rsplit("/", 1)[-1],
                    "url": link,
                    "title": title,
                    "company": company,
                    "location": "Remote",
                    "employment_type": "정규직",
                    "posted_at": posted_at,
                    "status_text": "Open",
                    "description": desc,
                }
            )
        feed_summaries.append(
            f"{rss_url} items={feed_total} kept={feed_kept} drop_robot={feed_drop_robot} "
            f"drop_blocked={feed_drop_blocked} drop_match_terms={feed_drop_match_terms}"
        )

    logger.info(
        "source=remoteok feeds=%d total_items=%d kept=%d drop_robot=%d drop_blocked=%d drop_match_terms=%d",
        len(fetched_feeds),
        total_items,
        len(results),
        drop_robot,
        drop_blocked,
        drop_match_terms,
    )
    for summary in feed_summaries:
        logger.info("source=remoteok feed_summary %s", summary)

    if not results and opts.get("sample_on_failure", True):
        logger.info("source=remoteok fallback sample enabled")
        return _sample()
    return results


def fetch_detail(item: Dict[str, Any], opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> Dict[str, Any]:
    return item


def crawl(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    return fetch_list(opts, cfg, logger)
