import os
from datetime import datetime
from typing import Any, Dict, List

from crawlers.common import request_with_retry


def _g(row: Dict[str, Any], keys: List[str], default: str = "") -> str:
    for k in keys:
        v = row.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return default


def fetch_list(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    api_cfg = opts.get("api", {}) if isinstance(opts.get("api"), dict) else {}
    if not bool(api_cfg.get("enabled", False)):
        return []

    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))
    max_items = int(opts.get("max_items", 20))
    q = opts.get("query", {})

    endpoint = str(api_cfg.get("api_url", "")).strip()
    if not endpoint:
        logger.info("source=publicdata api skipped: missing endpoint")
        return []

    service_key = str(api_cfg.get("service_key", "")).strip()
    if not service_key:
        service_key = os.getenv(str(api_cfg.get("service_key_env", "PUBLICDATA_SERVICE_KEY")), "").strip()
    if not service_key:
        logger.info("source=publicdata api skipped: missing service key")
        return []

    base_params = api_cfg.get("params", {}) if isinstance(api_cfg.get("params"), dict) else {}
    params: Dict[str, Any] = dict(base_params)
    params.setdefault("serviceKey", service_key)
    params.setdefault("type", "json")
    if q.get("keyword"):
        params.setdefault("keyword", q.get("keyword"))

    resp = request_with_retry("GET", endpoint, timeout, retries, logger, params=params)
    if not resp:
        return []

    try:
        js = resp.json()
    except Exception:
        return []

    # 흔한 공공 API 결과 포맷들 대응
    rows = []
    if isinstance(js, dict):
        rows = js.get("data", []) or js.get("items", []) or js.get("response", {}).get("body", {}).get("items", [])
        if isinstance(rows, dict):
            rows = rows.get("item", []) if isinstance(rows.get("item"), list) else [rows.get("item")] if rows.get("item") else []
    if not isinstance(rows, list):
        return []

    out: List[Dict[str, Any]] = []
    seen = set()
    for r in rows:
        if not isinstance(r, dict):
            continue
        sid = _g(r, ["id", "jobId", "wantedAuthNo", "recrtNo", "seq"], "")
        url = _g(r, ["url", "infoUrl", "wantedInfoUrl", "link"], "")
        title = _g(r, ["title", "jobTitle", "wantedTitle", "recrtTitle"], "PublicData Job")
        company = _g(r, ["company", "companyNm", "corpNm", "instNm"], "Unknown")
        location = _g(r, ["region", "regionNm", "workRegion", "workPlc"], "미상")
        employment = _g(r, ["employmentType", "holidayTpNm", "empTpNm"], "정규직")
        posted = _g(r, ["postedAt", "regDt", "regDate", "createDt"], datetime.now().strftime("%Y-%m-%d"))
        deadline = _g(r, ["deadline", "receiptCloseDt", "endDate"], "")
        if not sid:
            sid = url or f"publicdata-{len(out)+1}"
        if not url:
            url = endpoint
        if sid in seen:
            continue
        seen.add(sid)
        out.append(
            {
                "source_job_id": sid,
                "url": url,
                "title": title,
                "company": company,
                "location": location,
                "employment_type": employment,
                "posted_at": posted[:10],
                "deadline": deadline[:10] if deadline else "",
                "status_text": "모집중",
            }
        )
        if len(out) >= max_items:
            break
    return out


def fetch_detail(item: Dict[str, Any], opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> Dict[str, Any]:
    return item
