import hashlib
import re
import unicodedata


SPACE_RE = re.compile(r"\s+")


def normalize_text(value: str) -> str:
    value = value or ""
    value = unicodedata.normalize("NFKC", value).lower().strip()
    return SPACE_RE.sub(" ", value)


def normalize_company(company: str) -> str:
    text = normalize_text(company)
    text = re.sub(r"\(주\)|주식회사|inc\.?|corp\.?|co\.?\,?\s?ltd\.?", "", text)
    return SPACE_RE.sub(" ", text).strip()


def hash_text(value: str) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def title_company_hash(title: str, company: str) -> str:
    return hash_text(f"{normalize_text(title)}|{normalize_company(company)}")


def desc_fingerprint(description: str, max_len: int = 500) -> str:
    normalized = normalize_text(description)[:max_len]
    return hash_text(normalized)
