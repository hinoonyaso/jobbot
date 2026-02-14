import json
import os
from html import unescape
from typing import Any, Dict, List

import requests

from core.normalize import normalize_text


def _empty_result() -> Dict[str, Any]:
    return {
        "pass": False,
        "fail_reasons": [],
        "is_robot_sw": False,
        "fit_score": 0,
        "priority": "low",
        "role_type": "기타",
        "must_have_skills": [],
        "nice_to_have_skills": [],
        "why_fit": "",
        "risk_flags": [],
    }


def _rule_based(job: Dict[str, Any]) -> Dict[str, Any]:
    text = normalize_text(
        " ".join(
            [
                unescape(job.get("title", "")),
                unescape(job.get("description", "")),
                job.get("company", ""),
                job.get("employment_type", ""),
                job.get("location", ""),
            ]
        )
    )
    title = normalize_text(unescape(job.get("title", "")))

    role_keywords = {
        "자율주행/SLAM": ["slam", "자율주행", "localization", "localisation", "navigation", "map", "지도작성", "경로계획", "amr", "agv"],
        "로봇제어": ["제어", "control", "trajectory", "servo", "motor", "pid", "actuator", "kinematics", "동역학"],
        "임베디드": ["embedded", "firmware", "임베디드", "mcu", "rtos", "stm32", "uart", "can", "spi"],
        "플랫폼/미들웨어": ["middleware", "platform", "플랫폼", "미들웨어", "ros", "ros2", "dds", "linux"],
        "비전/인지": ["vision", "비전", "인지", "perception", "opencv", "camera", "detection", "segmentation", "point cloud"],
    }
    tech_keywords = [
        "ros", "ros2", "slam", "c++", "cpp", "python", "opencv", "lidar", "radar", "rtos", "linux", "autonomous",
        "navigation", "perception", "control", "imu", "sensor fusion", "deep learning", "pytorch", "tensorflow",
    ]
    robot_keywords = ["로봇", "robot", "amr", "agv", "cobot", "협동로봇", "자율주행"]
    education_keywords = ["학사", "대졸", "bachelor", "4년제"]
    entry_keywords = ["신입", "entry", "junior", "경력무관", "0년", "인턴"]
    senior_keywords = ["차장", "부장", "과장", "책임", "선임", "lead", "staff", "principal", "경력 5", "경력5", "10년"]

    role_type = "기타"
    role_scores = {}
    for role, kws in role_keywords.items():
        score = sum(1 for k in kws if k in text)
        role_scores[role] = score
    best_role, best_score = max(role_scores.items(), key=lambda x: x[1])
    if best_score > 0:
        role_type = best_role

    must = [k for k in tech_keywords if k in text][:7]
    nice = [k for k in ["docker", "kubernetes", "git", "ci/cd", "jira", "matlab", "gazebo"] if k in text][:7]

    fit = 0
    if any(k in text for k in robot_keywords):
        fit += 3
    fit += min(4, len(must))
    if role_type != "기타":
        fit += 1
    if any(k in text for k in education_keywords):
        fit += 1
    if any(k in text for k in entry_keywords):
        fit += 1
    if any(k in text for k in senior_keywords):
        fit -= 2
    if "강사" in title or "해설사" in title:
        fit -= 3
    fit = max(0, min(10, fit))

    priority = "high" if fit >= 7 else "medium" if fit >= 4 else "low"
    fail_reasons = []
    pass_flag = fit >= 4
    if not pass_flag:
        fail_reasons.append("로봇SW 핵심 키워드 부족")

    return {
        "pass": pass_flag,
        "fail_reasons": fail_reasons,
        "is_robot_sw": role_type != "기타" or any(k in text for k in robot_keywords),
        "fit_score": fit,
        "priority": priority,
        "role_type": role_type,
        "must_have_skills": must,
        "nice_to_have_skills": nice,
        "why_fit": "로봇 도메인 키워드, 역할군, 기술스택, 신입/학사 조건을 종합해 점수화했습니다.",
        "risk_flags": [] if pass_flag else ["요구 스택 불명확"],
    }


def _call_openai(job: Dict[str, Any], cfg: Dict[str, Any], timeout: int) -> Dict[str, Any]:
    api_key = os.getenv(cfg.get("api_key_env", "OPENAI_API_KEY"), "")
    if not api_key:
        raise RuntimeError("missing api key")

    model = cfg.get("model", "gpt-4o-mini")
    prompt = {
        "title": job.get("title", ""),
        "company": job.get("company", ""),
        "location": job.get("location", ""),
        "employment_type": job.get("employment_type", ""),
        "description": job.get("description", ""),
    }

    schema_desc = (
        '{"pass":true/false,"fail_reasons":["..."],"is_robot_sw":true/false,'
        '"fit_score":0-10,"priority":"high|medium|low","role_type":"자율주행/SLAM|로봇제어|임베디드|플랫폼/미들웨어|비전/인지|기타",'
        '"must_have_skills":["max7"],"nice_to_have_skills":["max7"],"why_fit":"2~3문장","risk_flags":["max5"]}'
    )

    messages = [
        {
            "role": "system",
            "content": "너는 로봇SW 채용 분류기다. JSON 외 출력 금지.",
        },
        {
            "role": "user",
            "content": f"아래 공고를 스키마대로 JSON만 출력하세요. 스키마: {schema_desc}\\n공고: {json.dumps(prompt, ensure_ascii=False)}",
        },
    ]

    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={"model": model, "messages": messages, "temperature": 0.1},
        timeout=timeout,
    )
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"]
    parsed = json.loads(content)

    base = _empty_result()
    base.update(parsed)
    base["fit_score"] = int(max(0, min(10, base.get("fit_score", 0))))
    if base.get("priority") not in {"high", "medium", "low"}:
        base["priority"] = "low"
    return base


def analyze_candidates(jobs: List[Dict[str, Any]], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    enabled = bool(cfg.get("enabled", False))
    timeout = int(cfg.get("timeout_sec", 15))
    analyzed = []

    for job in jobs:
        ai_result: Dict[str, Any]
        if enabled:
            try:
                ai_result = _call_openai(job, cfg, timeout=timeout)
            except Exception:
                logger.exception("AI analysis failed; fallback to rule-based")
                ai_result = _rule_based(job)
                ai_result.setdefault("risk_flags", []).append("AI fallback 사용")
        else:
            ai_result = _rule_based(job)

        merged = dict(job)
        merged["analysis"] = ai_result
        analyzed.append(merged)

    analyzed.sort(key=lambda x: (x["analysis"].get("fit_score", 0), x["analysis"].get("priority") == "high"), reverse=True)
    return analyzed
