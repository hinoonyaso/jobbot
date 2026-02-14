from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict


@dataclass
class Job:
    source: str
    url: str
    title: str
    company: str
    location: str
    employment_type: str
    posted_at: str
    description: str
    source_job_id: str = ""
    deadline: str = ""
    is_open: bool = True
    status_text: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")
