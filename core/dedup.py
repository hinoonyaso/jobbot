from difflib import SequenceMatcher
from typing import Any, Dict, List, Set
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from core.normalize import desc_fingerprint, normalize_company, normalize_text, title_company_hash


def _is_similar(a: str, b: str, threshold: float) -> bool:
    if not a or not b:
        return False
    return SequenceMatcher(None, a, b).ratio() >= threshold


def _canonical_url(url: str) -> str:
    if not url:
        return ""
    p = urlparse(url.strip())
    query = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if not k.lower().startswith("utm_")]
    return urlunparse((p.scheme.lower(), p.netloc.lower(), p.path.rstrip("/"), "", urlencode(sorted(query)), ""))


def _token_set(text: str) -> Set[str]:
    out = set()
    for t in normalize_text(text).split(" "):
        if len(t) >= 2:
            out.add(t)
    return out


def _jaccard(a: Set[str], b: Set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def deduplicate_jobs(jobs: List[Dict[str, Any]], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    sim_enabled = bool(cfg.get("title_similarity_enabled", True))
    sim_threshold = float(cfg.get("title_similarity_threshold", 0.9))
    cross_enabled = bool(cfg.get("cross_site_similarity_enabled", True))
    cross_threshold = float(cfg.get("cross_site_similarity_threshold", 0.86))

    seen_urls = set()
    seen_title_company = set()
    seen_desc = set()
    canonical_keys = set()
    unique: List[Dict[str, Any]] = []

    for job in jobs:
        url = _canonical_url(job.get("url", ""))
        title = normalize_text(job.get("title", ""))
        company = normalize_company(job.get("company", ""))
        location = normalize_text(job.get("location", ""))
        desc = normalize_text(job.get("description", ""))

        tc_hash = title_company_hash(title, company)
        desc_hash = desc_fingerprint(desc)
        cross_key = f"{title}|{company}|{location[:20]}"

        if url and url in seen_urls:
            continue
        if tc_hash in seen_title_company:
            continue
        if desc_hash in seen_desc:
            continue
        if cross_key in canonical_keys:
            continue

        if sim_enabled and any(_is_similar(title, normalize_text(x.get("title", "")), sim_threshold) for x in unique):
            continue

        if cross_enabled:
            cur_tokens = _token_set(f"{title} {company} {location} {desc[:200]}")
            duplicated = False
            for x in unique:
                x_tokens = _token_set(
                    f"{x.get('title', '')} {x.get('company', '')} {x.get('location', '')} {normalize_text(x.get('description', ''))[:200]}"
                )
                if _jaccard(cur_tokens, x_tokens) >= cross_threshold:
                    duplicated = True
                    break
            if duplicated:
                continue

        if url:
            seen_urls.add(url)
        seen_title_company.add(tc_hash)
        seen_desc.add(desc_hash)
        canonical_keys.add(cross_key)
        copied = dict(job)
        copied["url"] = url or job.get("url", "")
        unique.append(copied)

    logger.info("dedup removed=%d", len(jobs) - len(unique))
    return unique
