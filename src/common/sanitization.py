"""Shared business-name quality gates for strict buyer lead sanitization."""

from __future__ import annotations

import re

_WHITESPACE_RE = re.compile(r"\s+")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_LISTICLE_RE = re.compile(r"^\d+\s+(?:best|top)\b", re.IGNORECASE)
_OFF_TOPIC_RE = re.compile(
    r"\b("
    r"gun metal|top gun|sports?\s+shops?|shopping places?|handicrafts?|"
    r"instagram(?:\s+photos?\s+and\s+videos?)?|youtube|wikipedia"
    r")\b",
    re.IGNORECASE,
)
_SEO_PHRASE_RE = re.compile(
    r"\b("
    r"near me|best prices?|wholesale price|what to buy|photos? and videos|"
    r"live rates?|popular|top|best"
    r")\b",
    re.IGNORECASE,
)
_MARKETING_COPY_RE = re.compile(
    r"\b("
    r"welcome to|we specialize|keep tracking|shop online|buy\s+[a-z0-9]"
    r")\b",
    re.IGNORECASE,
)
_GENERIC_CATEGORY_RE = re.compile(
    r"\b("
    r"wholesalers?|suppliers?|dealers?|distributors?|exporters?|"
    r"commission agents?"
    r")\b.*\b(in|at|from|near)\b",
    re.IGNORECASE,
)
_PAGE_LABEL_RE = re.compile(
    r"^(?:home|products?(?:\s*&\s*services)?|contact us|about us|welcome)$",
    re.IGNORECASE,
)
_URLISH_NAME_RE = re.compile(r"(?:https?://|www\.)", re.IGNORECASE)
_FILEISH_NAME_RE = re.compile(
    r"\b(?:html?|php|aspx?)\b|(?:\.(?:html?|php|aspx?))$",
    re.IGNORECASE,
)
_LONG_NUMERIC_TOKEN_RE = re.compile(r"\b\d{6,}\b")
_PRODUCT_PAGE_RE = re.compile(
    r"\b("
    r"without stem|\d+\s*(?:kg|g|gm|grams?)|products?\s*&\s*services|"
    r"dried red chilli|dry red chilli|chilli whole|chilli powder"
    r")\b",
    re.IGNORECASE,
)
_BUSINESS_HINT_RE = re.compile(
    r"\b("
    r"agencies?|agro|broker|brokers|company|commission(?:\s+agents?)?|"
    r"enterprises?|exports?|foods?|impex|industries?|merchants?|"
    r"spices?|traders?"
    r")\b",
    re.IGNORECASE,
)
_NON_ENTITY_PREFIX_RE = re.compile(r"^(?:us and(?:\s+the)?|we\b|our\b)", re.IGNORECASE)
_DOMAIN_OR_URL_PREFIX_RE = re.compile(
    r"^(?:[a-z0-9.-]+\.[a-z]{2,}|https?://\S+)(?:\s+https?://\S+)?\s+",
    re.IGNORECASE,
)
_SCHEMELESS_URL_PREFIX_RE = re.compile(r"^//\S+\s+")
_SLUG_PREFIX_DISPLAY_RE = re.compile(r"^[a-z0-9]+(?:[-_/][a-z0-9]+){1,}\s+(.+)$")
_GENERIC_TOKENS = {
    "and",
    "agent",
    "agents",
    "andhra",
    "broker",
    "brokers",
    "chili",
    "chilli",
    "chillies",
    "commission",
    "dealer",
    "dealers",
    "distributor",
    "distributors",
    "dry",
    "exporter",
    "exporters",
    "from",
    "guntur",
    "hyderabad",
    "india",
    "in",
    "at",
    "khammam",
    "mirchi",
    "nalgonda",
    "ongole",
    "powder",
    "pradesh",
    "red",
    "shop",
    "shops",
    "supplier",
    "suppliers",
    "teja",
    "vijayawada",
    "warangal",
    "wholesale",
    "wholesaler",
    "wholesalers",
    "whole",
}


def collapse_whitespace(value: str) -> str:
    return _WHITESPACE_RE.sub(" ", value).strip()


def significant_business_tokens(value: str) -> tuple[str, ...]:
    tokenized = _NON_ALNUM_RE.sub(" ", collapse_whitespace(value).casefold())
    return tuple(
        token
        for token in tokenized.split()
        if token
        and token not in _GENERIC_TOKENS
        and not token.isdigit()
        and token not in {"html", "htm", "php", "aspx"}
    )


def _deslugify_business_name(value: str) -> str:
    cleaned = collapse_whitespace(_DOMAIN_OR_URL_PREFIX_RE.sub("", value))
    cleaned = collapse_whitespace(_SCHEMELESS_URL_PREFIX_RE.sub("", cleaned))
    cleaned = re.sub(r"\.(?:html?|php|aspx?)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"[_/]+", " ", cleaned)
    cleaned = re.sub(r"(?<=\w)-(?=\w)", " ", cleaned)
    cleaned = collapse_whitespace(cleaned)
    if cleaned and cleaned == cleaned.casefold() and any(separator in value for separator in ("-", "_", "/")):
        return cleaned.title()
    return cleaned


def _business_name_selection_key(value: str) -> tuple[object, ...]:
    issues = set(business_name_quality_issues(value))
    severe_issues = {
        "file_slug",
        "generic_category",
        "listicle",
        "marketing_copy",
        "non_entity_prefix",
        "off_topic",
        "page_label",
        "product_page",
        "seo_phrase",
        "urlish_name",
    }
    significant_tokens = significant_business_tokens(value)
    return (
        len(issues & severe_issues),
        len(issues),
        0 if _BUSINESS_HINT_RE.search(value) else 1,
        0 if significant_tokens else 1,
        abs(len(value.split()) - 3),
        len(value),
        value.casefold(),
    )


def canonicalize_business_name(value: str) -> str:
    normalized = collapse_whitespace(value)
    if not normalized:
        return ""

    ordered: list[str] = []
    seen: set[str] = set()

    def add(candidate: str) -> None:
        cleaned = collapse_whitespace(candidate)
        if not cleaned:
            return
        key = cleaned.casefold()
        if key in seen:
            return
        seen.add(key)
        ordered.append(cleaned)

    add(normalized)
    add(_SCHEMELESS_URL_PREFIX_RE.sub("", _DOMAIN_OR_URL_PREFIX_RE.sub("", normalized)))
    add(_SCHEMELESS_URL_PREFIX_RE.sub("", normalized))
    slug_display = _SLUG_PREFIX_DISPLAY_RE.match(normalized)
    if slug_display:
        add(slug_display.group(1))
    add(_deslugify_business_name(normalized))

    for candidate in tuple(ordered):
        if "›" in candidate:
            left, _, right = candidate.partition("›")
            add(left)
            add(right)
        for separator in (" | ", " – ", " — ", " - ", ":"):
            if separator not in candidate:
                continue
            left, _, right = candidate.partition(separator)
            add(left)
            if separator != ":" and not collapse_whitespace(right).startswith("//"):
                add(right)

    return min(ordered, key=_business_name_selection_key)


def select_best_business_name(*values: str) -> str:
    candidates = [canonicalize_business_name(value) for value in values if collapse_whitespace(value)]
    if not candidates:
        return ""
    return min(candidates, key=_business_name_selection_key)


def business_name_quality_issues(value: str) -> tuple[str, ...]:
    normalized = collapse_whitespace(value)
    if not normalized:
        return ("blank",)

    lowered = normalized.casefold()
    tokenized = _NON_ALNUM_RE.sub(" ", lowered)
    issues: list[str] = []
    significant_tokens = list(significant_business_tokens(normalized))

    if _OFF_TOPIC_RE.search(normalized):
        issues.append("off_topic")
    if _LISTICLE_RE.search(normalized):
        issues.append("listicle")
    if _PAGE_LABEL_RE.fullmatch(normalized):
        issues.append("page_label")
    if _URLISH_NAME_RE.search(normalized):
        issues.append("urlish_name")
    if _FILEISH_NAME_RE.search(normalized):
        issues.append("file_slug")
    if _LONG_NUMERIC_TOKEN_RE.search(tokenized) and len(significant_tokens) < 2:
        issues.append("numeric_artifact")
    if _SEO_PHRASE_RE.search(normalized):
        issues.append("seo_phrase")
    if _MARKETING_COPY_RE.search(normalized):
        issues.append("marketing_copy")
    if _PRODUCT_PAGE_RE.search(normalized) or _PRODUCT_PAGE_RE.search(tokenized):
        issues.append("product_page")
    if _GENERIC_CATEGORY_RE.search(normalized):
        if len(significant_tokens) < 2:
            issues.append("generic_category")
    if _NON_ENTITY_PREFIX_RE.search(normalized):
        issues.append("non_entity_prefix")
    if not _BUSINESS_HINT_RE.search(normalized):
        if len(significant_tokens) < 2 or _NON_ENTITY_PREFIX_RE.search(normalized):
            issues.append("not_business_entity")

    return tuple(dict.fromkeys(issues))


def is_acceptable_business_name(value: str) -> bool:
    return not business_name_quality_issues(value)


def is_extractable_business_name(value: str) -> bool:
    extract_blockers = {
        "file_slug",
        "off_topic",
        "listicle",
        "seo_phrase",
        "marketing_copy",
        "generic_category",
        "non_entity_prefix",
        "numeric_artifact",
        "urlish_name",
    }
    return not any(issue in extract_blockers for issue in business_name_quality_issues(value))


def is_weak_business_name(value: str) -> bool:
    return bool(
        set(business_name_quality_issues(value))
        & {
            "file_slug",
            "generic_category",
            "non_entity_prefix",
            "not_business_entity",
            "page_label",
            "product_page",
            "urlish_name",
        }
    )
