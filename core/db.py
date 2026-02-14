import json
from datetime import datetime
from typing import Any, Dict, List, Tuple

try:
    import pymysql
except Exception as exc:  # pragma: no cover
    pymysql = None
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None

from core.normalize import desc_fingerprint, hash_text, title_company_hash


def init_db(cfg: Dict[str, Any]):
    if pymysql is None:
        raise RuntimeError(f"pymysql import failed: {_IMPORT_ERROR}")

    conn = pymysql.connect(
        host=cfg.get("host", "127.0.0.1"),
        port=int(cfg.get("port", 3306)),
        user=cfg.get("user", "root"),
        password=cfg.get("password", ""),
        database=cfg.get("database", "jobbot"),
        charset=cfg.get("charset", "utf8mb4"),
        autocommit=False,
    )

    ddl = """
    CREATE TABLE IF NOT EXISTS jobs (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        source VARCHAR(64),
        url VARCHAR(1024) NOT NULL,
        url_hash CHAR(64) NOT NULL,
        title VARCHAR(512),
        company VARCHAR(255),
        location VARCHAR(255),
        employment_type VARCHAR(64),
        posted_at VARCHAR(32),
        description LONGTEXT,
        tc_hash CHAR(64),
        desc_hash CHAR(64),
        analysis_json LONGTEXT,
        fit_score INT,
        priority VARCHAR(16),
        created_at VARCHAR(32),
        deadline VARCHAR(32),
        is_open TINYINT(1),
        status_text VARCHAR(128),
        UNIQUE KEY uk_jobs_url_hash (url_hash),
        KEY idx_jobs_fit_score (fit_score),
        KEY idx_jobs_created_at (created_at),
        KEY idx_jobs_company (company),
        KEY idx_jobs_url_prefix (url(255))
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
        cur.execute("SHOW COLUMNS FROM jobs LIKE 'url_hash'")
        has_url_hash = cur.fetchone() is not None
        if not has_url_hash:
            cur.execute("ALTER TABLE jobs ADD COLUMN url_hash CHAR(64) NOT NULL DEFAULT '' AFTER url")
        cur.execute("SHOW INDEX FROM jobs WHERE Key_name='uk_jobs_url_hash'")
        has_url_hash_idx = cur.fetchone() is not None
        if not has_url_hash_idx:
            cur.execute("ALTER TABLE jobs ADD UNIQUE KEY uk_jobs_url_hash (url_hash)")
    conn.commit()
    return conn


def upsert_jobs(conn, jobs: List[Dict[str, Any]]) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    rows: List[Tuple[Any, ...]] = []
    for job in jobs:
        analysis = job.get("analysis", {})
        rows.append(
            (
                job.get("source", ""),
                job.get("url", ""),
                hash_text(job.get("url", "")),
                job.get("title", ""),
                job.get("company", ""),
                job.get("location", ""),
                job.get("employment_type", ""),
                job.get("posted_at", ""),
                job.get("description", ""),
                title_company_hash(job.get("title", ""), job.get("company", "")),
                desc_fingerprint(job.get("description", "")),
                json.dumps(analysis, ensure_ascii=False),
                int(analysis.get("fit_score", 0)),
                analysis.get("priority", "low"),
                now,
                job.get("deadline", ""),
                1 if job.get("is_open", True) else 0,
                job.get("status_text", ""),
            )
        )

    update_by_tc_sql = """
    UPDATE jobs SET
        source=%s,
        url=%s,
        url_hash=%s,
        title=%s,
        company=%s,
        location=%s,
        employment_type=%s,
        posted_at=%s,
        description=%s,
        desc_hash=%s,
        analysis_json=%s,
        fit_score=%s,
        priority=%s,
        created_at=%s,
        deadline=%s,
        is_open=%s,
        status_text=%s
    WHERE tc_hash=%s
    LIMIT 1
    """

    insert_sql = """
    INSERT INTO jobs (
        source, url, url_hash, title, company, location, employment_type,
        posted_at, description, tc_hash, desc_hash, analysis_json,
        fit_score, priority, created_at, deadline, is_open, status_text
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        url=VALUES(url),
        title=VALUES(title),
        company=VALUES(company),
        location=VALUES(location),
        employment_type=VALUES(employment_type),
        posted_at=VALUES(posted_at),
        description=VALUES(description),
        analysis_json=VALUES(analysis_json),
        fit_score=VALUES(fit_score),
        priority=VALUES(priority),
        created_at=VALUES(created_at),
        deadline=VALUES(deadline),
        is_open=VALUES(is_open),
        status_text=VALUES(status_text)
    """
    with conn.cursor() as cur:
        for row in rows:
            tc_hash = row[9]
            cur.execute(
                update_by_tc_sql,
                (
                    row[0],  # source
                    row[1],  # url
                    row[2],  # url_hash
                    row[3],  # title
                    row[4],  # company
                    row[5],  # location
                    row[6],  # employment_type
                    row[7],  # posted_at
                    row[8],  # description
                    row[10],  # desc_hash
                    row[11],  # analysis_json
                    row[12],  # fit_score
                    row[13],  # priority
                    row[14],  # created_at
                    row[15],  # deadline
                    row[16],  # is_open
                    row[17],  # status_text
                    tc_hash,
                ),
            )
            if cur.rowcount == 0:
                cur.execute(insert_sql, row)
    conn.commit()


def prune_closed_jobs(conn, jobs: List[Dict[str, Any]]) -> int:
    closed = [j for j in jobs if not bool(j.get("is_open", True))]
    if not closed:
        return 0

    url_hashes = [hash_text(j.get("url", "")) for j in closed if j.get("url")]
    tc_hashes = [title_company_hash(j.get("title", ""), j.get("company", "")) for j in closed]
    deleted = 0
    with conn.cursor() as cur:
        if url_hashes:
            in_clause = ",".join(["%s"] * len(url_hashes))
            cur.execute(f"DELETE FROM jobs WHERE url_hash IN ({in_clause})", tuple(url_hashes))
            deleted += cur.rowcount
        if tc_hashes:
            in_clause = ",".join(["%s"] * len(tc_hashes))
            cur.execute(f"DELETE FROM jobs WHERE tc_hash IN ({in_clause})", tuple(tc_hashes))
            deleted += cur.rowcount
    conn.commit()
    return deleted
