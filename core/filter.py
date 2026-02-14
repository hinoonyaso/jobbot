import re
from datetime import datetime
from typing import Any, Dict, List

from core.normalize import normalize_text


def _contains_any(text: str, keywords: List[str]) -> bool:
    t = normalize_text(text)
    return any(normalize_text(k) in t for k in keywords if k)


def _employment_match(job: Dict[str, Any], employment_types: List[str]) -> bool:
    if not employment_types:
        return True
    text = " ".join(
        [
            job.get("employment_type", ""),
            job.get("title", ""),
            job.get("description", ""),
        ]
    )
    return _contains_any(text, employment_types)


def _robot_direct_match(text: str, direct_keywords: List[str]) -> bool:
    if not direct_keywords:
        return True
    return _contains_any(text, direct_keywords)


def _backend_noise(text: str, backend_keywords: List[str], robot_keywords: List[str]) -> bool:
    t = normalize_text(text)
    has_backend = any(normalize_text(k) in t for k in backend_keywords if k)
    has_robot = any(normalize_text(k) in t for k in robot_keywords if k)
    return has_backend and not has_robot


def _region_or_big_company(job: Dict[str, Any], preferred_regions: List[str], big_company_keywords: List[str]) -> bool:
    company = job.get("company", "")
    location = job.get("location", "")
    description = job.get("description", "")

    if _contains_any(company, big_company_keywords):
        return True

    location_text = " ".join([location, description, job.get("title", "")])
    return _contains_any(location_text, preferred_regions)


def _is_open(job: Dict[str, Any]) -> bool:
    if not bool(job.get("is_open", True)):
        return False
    deadline = str(job.get("deadline", "")).strip()
    if not deadline:
        return True
    try:
        return datetime.now().date() <= datetime.strptime(deadline, "%Y-%m-%d").date()
    except Exception:
        return True


def _is_open_text_aware(job: Dict[str, Any], closed_pos: List[str], closed_neg: List[str]) -> bool:
    if not _is_open(job):
        return False
    blob = normalize_text(" ".join([job.get("title", ""), job.get("description", ""), job.get("status_text", "")]))
    if any(normalize_text(k) in blob for k in closed_neg if k):
        return True
    if any(normalize_text(k) in blob for k in closed_pos if k):
        return False
    # 경력 n년 이상 등은 open/close 판단과 분리.
    return True


def _entry_friendly(job: Dict[str, Any], strict_entry: bool, positives: List[str], negatives: List[str]) -> bool:
    if not strict_entry:
        return True
    blob = normalize_text(" ".join([job.get("title", ""), job.get("description", ""), job.get("status_text", "")]))
    has_positive = any(normalize_text(k) in blob for k in positives if k)
    if has_positive:
        return True

    has_negative = any(normalize_text(k) in blob for k in negatives if k)
    if not has_negative:
        if re.search(r"경력\s*[2-9]\d*\s*년", blob):
            has_negative = True
        if re.search(r"\b(2|3|4|5)\+\s*years?\b", blob):
            has_negative = True
    return not has_negative


def rule_filter(jobs: List[Dict[str, Any]], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    education_keywords = cfg.get("education_keywords", [])
    experience_keywords = cfg.get("experience_keywords", [])
    employment_types = cfg.get("employment_types", [])
    preferred_regions = cfg.get("preferred_regions", [])
    big_company_keywords = cfg.get("big_company_keywords", [])
    robot_keywords = cfg.get("robot_keywords", [])

    only_open = bool(cfg.get("only_open", True))
    min_desc_len = int(cfg.get("min_description_len", 80))

    strict_education = bool(cfg.get("strict_education", False))
    strict_experience = bool(cfg.get("strict_experience", False))
    strict_region = bool(cfg.get("strict_region", False))
    strict_entry = bool(cfg.get("strict_entry", True))
    min_profile_matches = int(cfg.get("min_profile_matches", 2))
    require_robot_direct = bool(cfg.get("require_robot_direct", True))
    robot_direct_keywords = cfg.get(
        "robot_direct_keywords",
        ["로봇", "robot", "자율주행", "slam", "ros", "agv", "amr", "로봇제어", "perception", "navigation"],
    )
    backend_noise_keywords = cfg.get(
        "backend_noise_keywords",
        ["golang 서버", "백엔드", "backend", "server", "api", "웹서비스"],
    )
    entry_positive_keywords = cfg.get(
        "entry_positive_keywords",
        ["신입", "경력무관", "entry", "junior", "new grad", "0년"],
    )
    entry_negative_keywords = cfg.get(
        "entry_negative_keywords",
        ["시니어", "senior", "lead", "principal", "manager", "책임", "수석", "과장", "차장", "부장"],
    )
    closed_positive_keywords = cfg.get(
        "closed_positive_keywords",
        ["접수마감", "모집마감", "채용마감", "마감되었습니다", "종료", "closed", "expired"],
    )
    closed_negative_keywords = cfg.get(
        "closed_negative_keywords",
        ["채용시 마감", "상시채용", "상시 모집", "모집중", "채용중"],
    )

    passed = []
    for job in jobs:
        blob = " ".join([job.get("title", ""), job.get("description", ""), job.get("company", "")])

        if only_open and not _is_open_text_aware(job, closed_positive_keywords, closed_negative_keywords):
            continue
        if len(normalize_text(job.get("description", ""))) < min_desc_len:
            continue
        if robot_keywords and not _contains_any(blob, robot_keywords):
            continue
        if require_robot_direct and not _robot_direct_match(blob, robot_direct_keywords):
            continue
        if _backend_noise(blob, backend_noise_keywords, robot_direct_keywords):
            continue
        if not _entry_friendly(job, strict_entry, entry_positive_keywords, entry_negative_keywords):
            continue
        if not _employment_match(job, employment_types):
            continue

        edu_ok = _contains_any(blob, education_keywords) if education_keywords else True
        exp_ok = _contains_any(blob, experience_keywords) if experience_keywords else True
        region_ok = _region_or_big_company(job, preferred_regions, big_company_keywords)

        if strict_education and not edu_ok:
            continue
        if strict_experience and not exp_ok:
            continue
        if strict_region and not region_ok:
            continue

        profile_score = int(edu_ok) + int(exp_ok) + int(region_ok)
        if profile_score < min_profile_matches:
            continue

        passed.append(job)

    logger.info("rule_filter removed=%d", len(jobs) - len(passed))
    return passed
