from datetime import datetime, timedelta
from typing import Any, Dict, List


def fetch_list(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    now = datetime.now().strftime("%Y-%m-%d")
    due = (datetime.now() + timedelta(days=10)).strftime("%Y-%m-%d")
    return [
        {
            "source_job_id": "stub-1",
            "url": "https://example.com/jobs/robotics-sw-1",
            "title": "로봇 자율주행 SW 엔지니어 (신입)",
            "company": "테스트로보틱스",
            "location": "성남시 분당구",
            "employment_type": "정규직",
            "posted_at": now,
            "deadline": due,
            "is_open": True,
            "status_text": "모집중",
            "description": "ROS2, SLAM, Python/C++, Navigation, Localization, 학사 이상, 신입 가능, Linux 기반 로봇 플랫폼 개발",
        },
        {
            "source_job_id": "stub-2",
            "url": "https://example.com/jobs/robotics-vision-2",
            "title": "로봇 비전/인지 엔지니어 인턴",
            "company": "샘플모빌리티",
            "location": "강남구",
            "employment_type": "인턴",
            "posted_at": now,
            "deadline": due,
            "is_open": True,
            "status_text": "모집중",
            "description": "컴퓨터비전, OpenCV, Python, 딥러닝 기반 인지/검출, 학사, 신입 우대, 카메라 센서 데이터 처리",
        },
        {
            "source_job_id": "stub-3",
            "url": "https://example.com/jobs/non-robot",
            "title": "백엔드 서버 개발자",
            "company": "샘플스타트업",
            "location": "서울",
            "employment_type": "정규직",
            "posted_at": now,
            "deadline": due,
            "is_open": True,
            "status_text": "모집중",
            "description": "Java, Spring, 클라우드",
        },
    ]


def fetch_detail(item: Dict[str, Any], opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> Dict[str, Any]:
    return item


def crawl(opts: Dict[str, Any], cfg: Dict[str, Any], logger) -> List[Dict[str, Any]]:
    return fetch_list(opts, cfg, logger)
