#!/usr/bin/env python3
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from importlib import import_module
from typing import Any, Dict, List

import yaml

from core.ai_rank import analyze_candidates
from core.db import init_db, prune_closed_jobs, upsert_jobs
from core.dedup import deduplicate_jobs
from core.filter import rule_filter
from core.mailer import send_email
from core.report import build_daily_report, collect_stack_trends
from core.schema import Job
from crawlers.common import normalize_job


def setup_logger(log_path: str, level: str = "INFO") -> logging.Logger:
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logger = logging.getLogger("jobbot")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def load_config(path: str = "config.yaml") -> Dict[str, Any]:
    path = os.getenv("JOBBOT_CONFIG_PATH", path)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _log_source_breakdown(label: str, jobs: List[Dict[str, Any]], logger: logging.Logger) -> None:
    counts: Dict[str, int] = {}
    for j in jobs:
        src = str(j.get("source", "unknown"))
        counts[src] = counts.get(src, 0) + 1
    if counts:
        summary = ", ".join(f"{k}:{v}" for k, v in sorted(counts.items(), key=lambda x: (-x[1], x[0])))
    else:
        summary = "-"
    logger.info("%s by_source=%s", label, summary)


def _load_source_health(path: str) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_source_health(path: str, data: Dict[str, Any]) -> None:
    if not path:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _fetch_details_parallel(module, source_name: str, list_items: List[Dict[str, Any]], opts: Dict[str, Any], cfg: Dict[str, Any], logger: logging.Logger) -> List[Dict[str, Any]]:
    if not hasattr(module, "fetch_detail"):
        return list_items

    workers = int(opts.get("workers", cfg.get("collection", {}).get("workers", 4)))
    max_items = int(opts.get("max_items", cfg.get("collection", {}).get("max_items_per_source", 30)))
    selected = list_items[:max_items]
    detailed: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futs = {ex.submit(module.fetch_detail, item, opts, cfg, logger): item for item in selected}
        for fut in as_completed(futs):
            base = futs[fut]
            try:
                detail = fut.result() or {}
                if not isinstance(detail, dict):
                    detail = {}
                merged = dict(base)
                merged.update(detail)
                detailed.append(merged)
            except Exception:
                logger.exception("source=%s detail fetch failed url=%s", source_name, base.get("url", ""))
                detailed.append(base)
    return detailed


def _run_source(source_name: str, opts: Dict[str, Any], cfg: Dict[str, Any], logger: logging.Logger) -> List[Job]:
    module = import_module(f"crawlers.{source_name}")

    if hasattr(module, "fetch_list"):
        list_items = module.fetch_list(opts, cfg, logger)
        if not isinstance(list_items, list):
            logger.warning("source=%s fetch_list returned non-list", source_name)
            return []
        raw_jobs = _fetch_details_parallel(module, source_name, list_items, opts, cfg, logger)
    elif hasattr(module, "crawl"):
        raw_jobs = module.crawl(opts, cfg, logger)
    else:
        logger.warning("source=%s has no fetch_list/crawl", source_name)
        return []

    jobs: List[Job] = []
    for raw in raw_jobs:
        if not isinstance(raw, dict):
            continue
        try:
            job = normalize_job(raw, source=source_name)
            if not job.url or not job.title:
                continue
            jobs.append(job)
        except Exception:
            logger.exception("source=%s normalize failed raw=%s", source_name, str(raw)[:250])

    if jobs:
        sample = jobs[0]
        logger.info(
            "source=%s sample url=%s title=%s company=%s",
            source_name,
            sample.url,
            sample.title[:80],
            sample.company,
        )
    return jobs


def run_crawlers(cfg: Dict[str, Any], logger: logging.Logger) -> List[Dict[str, Any]]:
    results: List[Job] = []
    crawler_cfg = cfg.get("crawlers", {})
    collection_cfg = cfg.get("collection", {})
    health_cfg = collection_cfg.get("source_health", {})
    health_enabled = bool(health_cfg.get("enabled", True))
    zero_threshold = int(health_cfg.get("zero_collect_threshold", 3))
    health_file = str(health_cfg.get("file", "data/source_health.json"))
    source_health = _load_source_health(health_file) if health_enabled else {}
    ordered_sources = sorted(
        [(name, opts) for name, opts in crawler_cfg.items() if isinstance(opts, dict)],
        key=lambda x: int(x[1].get("tier", 2)),
    )
    for source_name, opts in ordered_sources:
        if not opts.get("enabled", False):
            continue
        if health_enabled:
            consecutive_zero = int(source_health.get(source_name, {}).get("consecutive_zero", 0))
            if consecutive_zero >= zero_threshold:
                logger.info(
                    "source=%s skipped by health guard consecutive_zero=%d threshold=%d",
                    source_name,
                    consecutive_zero,
                    zero_threshold,
                )
                continue
        try:
            jobs = _run_source(source_name, opts, cfg, logger)
            logger.info("source=%s tier=%s collected=%d", source_name, opts.get("tier", 2), len(jobs))
            results.extend(jobs)
            if health_enabled:
                source_health[source_name] = {
                    "consecutive_zero": 0 if jobs else int(source_health.get(source_name, {}).get("consecutive_zero", 0)) + 1,
                    "last_collected": len(jobs),
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                }
        except Exception:
            logger.exception("source=%s failed; continue", source_name)
            if health_enabled:
                source_health[source_name] = {
                    "consecutive_zero": int(source_health.get(source_name, {}).get("consecutive_zero", 0)) + 1,
                    "last_collected": 0,
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                }

    if health_enabled:
        _save_source_health(health_file, source_health)

    return [j.to_dict() for j in results]


def main() -> None:
    cfg = load_config("config.yaml")
    logger = setup_logger(cfg["paths"]["log_file"], cfg.get("log_level", "INFO"))

    conn = init_db(cfg.get("database", {}))

    logger.info("jobbot started")
    collected = run_crawlers(cfg, logger)
    logger.info("collected total=%d", len(collected))
    _log_source_breakdown("collected", collected, logger)

    deduped = deduplicate_jobs(collected, cfg.get("dedup", {}), logger)
    logger.info("deduplicated total=%d", len(deduped))
    deleted_closed = prune_closed_jobs(conn, deduped)
    if deleted_closed:
        logger.info("pruned closed jobs deleted=%d", deleted_closed)

    filtered = rule_filter(deduped, cfg.get("rule_filter", {}), logger)
    logger.info("rule filtered total=%d", len(filtered))
    _log_source_breakdown("rule_filtered", filtered, logger)

    analyzed = analyze_candidates(filtered, cfg.get("ai", {}), logger)
    _log_source_breakdown("analyzed", analyzed, logger)
    upsert_jobs(conn, analyzed)

    top_n = int(cfg.get("report", {}).get("top_n", 10))
    big_companies = cfg.get("rule_filter", {}).get("big_company_keywords", [])
    preferred_regions = cfg.get("rule_filter", {}).get("preferred_regions", [])
    trends = collect_stack_trends(analyzed, limit=10)

    html = build_daily_report(
        analyzed_jobs=analyzed,
        generated_at=datetime.now(),
        top_n=top_n,
        big_company_keywords=big_companies,
        preferred_regions=preferred_regions,
        trends=trends,
    )

    email_cfg = cfg.get("email", {})
    skip_if_empty = bool(email_cfg.get("skip_if_empty", True))
    if skip_if_empty and len(analyzed) == 0:
        sent = False
        logger.info("email skipped because analyzed candidates are empty")
    else:
        sent = send_email(
            cfg=email_cfg,
            html=html,
            subject=f"[JobBot] 로봇SW 채용 리포트 {datetime.now().strftime('%Y-%m-%d')}",
            logger=logger,
        )

    out_path = cfg.get("report", {}).get("output_html", "data/daily_report.html")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info("report written path=%s email_sent=%s", out_path, sent)
    logger.info("jobbot finished")


if __name__ == "__main__":
    main()
