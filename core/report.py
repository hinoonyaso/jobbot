from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List

from jinja2 import Environment, FileSystemLoader, select_autoescape


def _is_big_company(job: Dict[str, Any], keywords: List[str]) -> bool:
    company = (job.get("company") or "").lower()
    return any((k or "").lower() in company for k in keywords)


def _in_preferred_region(job: Dict[str, Any], regions: List[str]) -> bool:
    text = " ".join([job.get("location", ""), job.get("description", "")]).lower()
    return any((r or "").lower() in text for r in regions)


def collect_stack_trends(analyzed_jobs: List[Dict[str, Any]], limit: int = 10) -> List[Dict[str, Any]]:
    counter = Counter()
    for job in analyzed_jobs:
        analysis = job.get("analysis", {})
        for s in analysis.get("must_have_skills", []):
            counter[s] += 1
        for s in analysis.get("nice_to_have_skills", []):
            counter[s] += 1
    return [{"skill": k, "count": v} for k, v in counter.most_common(limit)]


def _by_employment(analyzed_jobs: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    buckets = defaultdict(list)
    for job in analyzed_jobs:
        key = job.get("employment_type") or "미분류"
        buckets[key].append(job)
    return dict(buckets)


def build_daily_report(
    analyzed_jobs: List[Dict[str, Any]],
    generated_at: datetime,
    top_n: int,
    big_company_keywords: List[str],
    preferred_regions: List[str],
    trends: List[Dict[str, Any]],
) -> str:
    jobs_sorted = sorted(analyzed_jobs, key=lambda x: x.get("analysis", {}).get("fit_score", 0), reverse=True)
    top_jobs = jobs_sorted[:top_n]

    big_companies = [j for j in jobs_sorted if _is_big_company(j, big_company_keywords)][:top_n]
    regional = [j for j in jobs_sorted if _in_preferred_region(j, preferred_regions) and not _is_big_company(j, big_company_keywords)][:top_n]
    by_employment = _by_employment(jobs_sorted)

    env = Environment(
        loader=FileSystemLoader("templates"),
        autoescape=select_autoescape(["html", "xml"]),
    )
    tmpl = env.get_template("daily_email.html")
    return tmpl.render(
        generated_at=generated_at.strftime("%Y-%m-%d %H:%M"),
        total=len(analyzed_jobs),
        top_jobs=top_jobs,
        big_companies=big_companies,
        regional=regional,
        by_employment=by_employment,
        trends=trends,
    )
