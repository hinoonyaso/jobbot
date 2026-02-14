import os
from datetime import datetime
from typing import Any, Dict, List
from xml.etree import ElementTree as ET

from crawlers.common import request_with_retry
from core.normalize import normalize_text

WORKNET_DEFAULT_URL = "https://openapi.work.go.kr/opi/opi/opia/wantedApi.do"


def _first(d: Dict[str, Any], keys: List[str], default: str = "") -> str:
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return default


def _xml_items(text: str) -> List[Dict[str, str]]:
    root = ET.fromstring(text)
    rows = root.findall(".//dhsOpenEmpInfo")
    if not rows:
        rows = root.findall(".//wanted")
    out: List[Dict[str, str]] = []
    for r in rows:
        item: Dict[str, str] = {}
        for c in list(r):
            tag = (c.tag or "").split("}", 1)[-1]
            item[tag] = (c.text or "").strip()
        if item:
            out.append(item)
    return out


def _to_item(row: Dict[str, Any]) -> Dict[str, Any]:
    jid = _first(row, ["wantedAuthNo", "wantedNo", "jobId", "id"])
    title = _first(row, ["title", "wantedTitle", "jobCont", "jobNm"], "Worknet Job")
    company = _first(row, ["company", "companyNm", "corpNm", "empName"], "Unknown")
    url = _first(
        row,
        ["wantedInfoUrl", "infoUrl", "url", "wantedDtlUrl"],
        f"https://www.work24.go.kr/",
    )
    location = _first(row, ["region", "regionNm", "workRegion", "workPlc"], "미상")
    career = normalize_text(_first(row, ["career", "careerCnd", "careerNm"], ""))
    edu = normalize_text(_first(row, ["minEdubg", "academy", "eduNm"], ""))
    status = _first(row, ["closeType", "closeTypeNm", "status"], "모집중")
    deadline = _first(row, ["receiptCloseDt", "closeDate", "deadline"], "")
    employment = _first(row, ["holidayTpNm", "employmentType", "empTpNm"], "정규직")
    posted = _first(row, ["regDt", "regDate", "postedAt"], datetime.now().strftime("%Y-%m-%d"))
    blob = normalize_text(f"{title} {company} {career} {edu}")
    if "인턴" in blob or "intern" in blob:
        employment = "인턴"
    elif "계약" in blob or "contract" in blob:
        employment = "계약직"
    elif employment == "미상":
        employment = "정규직"
    return {
        "source_job_id": jid or url,
        "url": url,
        "title": title,
        "company": company,
        "location": location,
        "employment_type": employment,
        "posted_at": posted[:10],
        "deadline": deadline[:10] if len(deadline) >= 10 else "",
        "status_text": status or "모집중",
    }


def fetch_list(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    api_cfg = opts.get("api", {}) if isinstance(opts.get("api"), dict) else {}
    if not bool(api_cfg.get("enabled", False)):
        return []

    timeout = int(cfg.get("network", {}).get("timeout_sec", 10))
    retries = int(cfg.get("network", {}).get("retry", 2))
    max_items = int(opts.get("max_items", 20))
    q = opts.get("query", {})

    auth_key = str(api_cfg.get("auth_key", "")).strip()
    if not auth_key:
        auth_key = os.getenv(str(api_cfg.get("auth_key_env", "WORKNET_AUTH_KEY")), "").strip()
    if not auth_key:
        logger.info("source=worknet api skipped: missing auth key")
        return []

    endpoint = str(api_cfg.get("api_url", WORKNET_DEFAULT_URL))
    keyword = str(q.get("keyword", "로봇 소프트웨어")).strip()
    start_page = int(api_cfg.get("start_page", 1))
    display = int(min(max_items, int(api_cfg.get("display", 50))))
    return_type = str(api_cfg.get("return_type", "XML")).upper()

    params = {
        "authKey": auth_key,
        "callTp": "L",
        "returnType": return_type,
        "startPage": start_page,
        "display": display,
        "keyword": keyword,
    }
    resp = request_with_retry("GET", endpoint, timeout, retries, logger, params=params)
    if not resp:
        return []

    rows: List[Dict[str, Any]] = []
    if return_type == "JSON":
        try:
            js = resp.json()
            rows = js.get("dhsOpenEmpInfo", []) or js.get("wantedRoot", {}).get("wanted", []) or []
        except Exception:
            rows = []
    else:
        try:
            rows = _xml_items(resp.text)
        except Exception:
            rows = []

    if not rows:
        return []

    out: List[Dict[str, Any]] = []
    seen = set()
    for r in rows:
        if not isinstance(r, dict):
            continue
        item = _to_item(r)
        sid = item.get("source_job_id")
        if not sid or sid in seen:
            continue
        seen.add(sid)
        out.append(item)
        if len(out) >= max_items:
            break
    return out


def fetch_detail(item: Dict[str, Any], opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> Dict[str, Any]:
    return item
