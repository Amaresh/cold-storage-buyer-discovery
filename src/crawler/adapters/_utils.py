"""Small HTML parsing helpers for deterministic fixture-driven adapters."""

from __future__ import annotations

from html import unescape
import re
from urllib.parse import urljoin, urlparse

_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")
_PHONE_RE = re.compile(r"(?<![\w@])(?:\+?\d[\d\s().-]{7,}\d)(?![\w@])")
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}")
_STATE_PAIR_RE = re.compile(r"\b([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){0,2}),\s*([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){0,3})\b")
_IN_OR_AT_TOWN_RE = re.compile(r"\b(?:in|at)\s+([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){0,2})\b")
_DATE_HINT_RE = re.compile(r"(?:\d{4}[-/.]\d{1,2}[-/.]\d{1,2}|\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})")
_DECIMAL_HINT_RE = re.compile(r"\d+(?:\.\d+)+")


def collapse_whitespace(value: str) -> str:
    return _WHITESPACE_RE.sub(" ", value).strip()


def strip_tags(fragment: str) -> str:
    return collapse_whitespace(unescape(_TAG_RE.sub(" ", fragment)))


def _looks_like_phone_hint(value: str) -> bool:
    compact = value.strip()
    digit_count = sum(character.isdigit() for character in compact)
    if digit_count < 7:
        return False
    if _DATE_HINT_RE.fullmatch(compact):
        return False
    if _DECIMAL_HINT_RE.fullmatch(compact):
        return False
    return True


def extract_contact_hints(*texts: str) -> tuple[str, ...]:
    ordered: list[str] = []
    seen: set[str] = set()
    combined = " ".join(text for text in texts if text)
    for pattern in (_PHONE_RE, _EMAIL_RE):
        for match in pattern.finditer(combined):
            value = collapse_whitespace(match.group(0))
            if pattern is _PHONE_RE and not _looks_like_phone_hint(value):
                continue
            if value.casefold() in seen:
                continue
            seen.add(value.casefold())
            ordered.append(value)
    return tuple(ordered)


def resolve_href(href: str, base_url: str) -> str:
    resolved = urljoin(base_url, href.strip())
    parsed = urlparse(resolved)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    return resolved


def is_same_domain(url: str, *domains: str) -> bool:
    netloc = urlparse(url).netloc.casefold()
    return any(netloc == domain.casefold() or netloc.endswith(f".{domain.casefold()}") for domain in domains)


def detect_town(*texts: str, town_hints: tuple[str, ...] = ()) -> str:
    combined = collapse_whitespace(" ".join(text for text in texts if text))
    if not combined:
        return ""

    for town in town_hints:
        if re.search(rf"\b{re.escape(town)}\b", combined, re.IGNORECASE):
            return town

    state_pair = _STATE_PAIR_RE.search(combined)
    if state_pair:
        return collapse_whitespace(state_pair.group(1))

    for part in reversed([collapse_whitespace(item) for item in combined.split(",")]):
        if re.fullmatch(r"[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){0,2}", part):
            return part

    match = _IN_OR_AT_TOWN_RE.search(combined)
    if match:
        return collapse_whitespace(match.group(1))
    return ""
