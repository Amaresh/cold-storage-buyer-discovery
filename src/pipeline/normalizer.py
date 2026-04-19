"""Normalize buyer candidates and collapse deterministic duplicates."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
import re
import unicodedata
from urllib.parse import urlsplit, urlunsplit

from src.common.models import (
    BuyerCandidate,
    CandidateEvidence,
    NormalizedBuyerCandidate,
    slugify,
)
from src.common.sanitization import is_weak_business_name, significant_business_tokens
from src.crawler.source_policy import get_source_policy

_WHITESPACE_RE = re.compile(r"\s+")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_EMAIL_RE = re.compile(r"^[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}$")
_FIRST_PARTY_SOURCE_CLASSES = {"first_party_website"}
_SHARED_HOST_DOMAINS = {
    "dial4trade.com",
    "facebook.com",
    "instagram.com",
    "justdial.com",
    "linkedin.com",
    "tradeindia.com",
    "twitter.com",
    "vyapartimes.com",
    "x.com",
    "indiamart.com",
}


@dataclass(frozen=True, slots=True)
class _NormalizedObservation:
    business_name: str
    business_name_key: str
    town: str
    town_key: str
    source_key: str
    source_url: str
    website: str
    domain: str
    phones: tuple[str, ...]
    emails: tuple[str, ...]
    other_contact_hints: tuple[str, ...]
    source_confidence_class: str
    source_confidence_level: int

    @property
    def dedupe_identifiers(self) -> tuple[tuple[str, str], ...]:
        identifiers: list[tuple[str, str]] = []
        if self.domain:
            identifiers.append(("domain", self.domain))
        elif self.website:
            identifiers.append(("website", self.website))
        if self.business_name_key and self.town_key:
            identifiers.append(("name_town", f"{self.business_name_key}::{self.town_key}"))
        identifiers.extend(
            ("phone", f"{self.business_name_key}::{self.town_key}::{phone}")
            for phone in self.phones
            if self.business_name_key and self.town_key
        )
        identifiers.extend(
            ("email", f"{self.business_name_key}::{self.town_key}::{email}")
            for email in self.emails
            if self.business_name_key and self.town_key
        )
        return tuple(identifiers)


def _collapse_whitespace(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value)
    return _WHITESPACE_RE.sub(" ", normalized).strip()


def normalize_business_name(value: str) -> str:
    """Collapse obvious punctuation/casing noise for deterministic name dedupe."""

    normalized = _collapse_whitespace(value).casefold().replace("&", " and ")
    return _collapse_whitespace(_NON_ALNUM_RE.sub(" ", normalized))


def normalize_town(value: str) -> str:
    return _collapse_whitespace(value).casefold()


def normalize_phone(value: str) -> str:
    """Normalize phone numbers into a stable, dedupe-friendly representation."""

    cleaned = _collapse_whitespace(value)
    digits = re.sub(r"\D+", "", cleaned)
    if not digits:
        return ""
    if digits.startswith("00"):
        digits = digits[2:]
    if len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"+91{digits}"
    if len(digits) == 12 and digits.startswith("91"):
        return f"+{digits}"
    if 8 <= len(digits) <= 15:
        return f"+{digits}" if cleaned.startswith("+") or len(digits) > 10 else digits
    return ""


def normalize_email(value: str) -> str:
    return _collapse_whitespace(value).casefold()


def normalize_url(value: str) -> str:
    """Normalize URLs for deterministic comparison and payload export."""

    cleaned = _collapse_whitespace(value)
    if not cleaned:
        return ""
    if not re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", cleaned):
        cleaned = f"https://{cleaned.lstrip('/')}"
    parsed = urlsplit(cleaned)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return ""
    scheme = parsed.scheme.lower()
    hostname = (parsed.hostname or "").casefold()
    if hostname.startswith("www."):
        hostname = hostname[4:]
    if not hostname:
        return ""
    netloc = hostname
    try:
        port = parsed.port
    except ValueError:
        return ""
    if port and not (
        (scheme == "http" and port == 80)
        or (scheme == "https" and port == 443)
    ):
        netloc = f"{hostname}:{port}"
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    return urlunsplit((scheme, netloc, path or "/", "", ""))


def normalize_domain(value: str) -> str:
    """Normalize domains from a URL or bare host string."""

    cleaned = _collapse_whitespace(value)
    if not cleaned:
        return ""
    if not re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", cleaned):
        cleaned = f"https://{cleaned.lstrip('/')}"
    parsed = urlsplit(cleaned)
    hostname = (parsed.hostname or "").casefold()
    if hostname.startswith("www."):
        hostname = hostname[4:]
    return hostname


def is_shared_host_domain(domain: str) -> bool:
    return any(domain == host or domain.endswith(f".{host}") for host in _SHARED_HOST_DOMAINS)


def _merge_ordered(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if not value:
            continue
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(value)
    return tuple(ordered)


def _policy_metadata(source_key: str) -> tuple[str, int]:
    try:
        policy = get_source_policy(source_key)
    except KeyError:
        return ("unknown", 0)
    return (policy.confidence_class, policy.confidence_level)


def _canonical_candidate_website(candidate: BuyerCandidate, source_confidence_class: str) -> str:
    if candidate.website:
        normalized_website = normalize_url(candidate.website)
        if source_confidence_class != "discovery_seed":
            return normalized_website
        domain = normalize_domain(normalized_website)
        return "" if not domain or is_shared_host_domain(domain) else normalized_website
    if source_confidence_class == "discovery_seed":
        normalized_source_url = normalize_url(candidate.source_url)
        domain = normalize_domain(normalized_source_url)
        return "" if not domain or is_shared_host_domain(domain) else normalized_source_url
    if source_confidence_class in _FIRST_PARTY_SOURCE_CLASSES:
        return normalize_url(candidate.source_url)
    return ""


def _partition_contact_hints(hints: Iterable[str]) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    phones: list[str] = []
    emails: list[str] = []
    other_hints: list[str] = []
    seen_phones: set[str] = set()
    seen_emails: set[str] = set()
    seen_other_hints: set[str] = set()

    for hint in hints:
        cleaned = _collapse_whitespace(hint)
        if not cleaned:
            continue
        if _EMAIL_RE.fullmatch(cleaned):
            email = normalize_email(cleaned)
            if email not in seen_emails:
                seen_emails.add(email)
                emails.append(email)
            continue
        phone = normalize_phone(cleaned)
        if phone:
            if phone not in seen_phones:
                seen_phones.add(phone)
                phones.append(phone)
            continue
        key = cleaned.casefold()
        if key not in seen_other_hints:
            seen_other_hints.add(key)
            other_hints.append(cleaned)

    return (tuple(phones), tuple(emails), tuple(other_hints))


def _normalize_observation(candidate: BuyerCandidate) -> _NormalizedObservation | None:
    business_name = _collapse_whitespace(candidate.business_name)
    business_name_key = normalize_business_name(business_name)
    town = _collapse_whitespace(candidate.town)
    town_key = normalize_town(town)
    source_url = normalize_url(candidate.source_url)
    source_confidence_class, source_confidence_level = _policy_metadata(candidate.source_key)
    website = _canonical_candidate_website(candidate, source_confidence_class)
    domain = normalize_domain(website)
    if domain and is_shared_host_domain(domain):
        domain = ""
    phones, emails, other_contact_hints = _partition_contact_hints(candidate.contact_hints)
    if not business_name or not business_name_key or not town or not town_key or not source_url:
        return None
    return _NormalizedObservation(
        business_name=business_name,
        business_name_key=business_name_key,
        town=town,
        town_key=town_key,
        source_key=candidate.source_key,
        source_url=source_url,
        website=website,
        domain=domain,
        phones=phones,
        emails=emails,
        other_contact_hints=other_contact_hints,
        source_confidence_class=source_confidence_class,
        source_confidence_level=source_confidence_level,
    )


def _observation_sort_key(observation: _NormalizedObservation) -> tuple[object, ...]:
    return (
        observation.town_key,
        observation.business_name_key,
        observation.domain,
        observation.phones,
        observation.emails,
        observation.source_key,
        observation.source_url,
    )


def _find(parents: list[int], index: int) -> int:
    while parents[index] != index:
        parents[index] = parents[parents[index]]
        index = parents[index]
    return index


def _union(parents: list[int], left: int, right: int) -> int:
    left_root = _find(parents, left)
    right_root = _find(parents, right)
    if left_root == right_root:
        return left_root
    if right_root < left_root:
        left_root, right_root = right_root, left_root
    parents[right_root] = left_root
    return left_root


def _can_merge(left: _NormalizedObservation, right: _NormalizedObservation) -> bool:
    if left.town_key and right.town_key and left.town_key != right.town_key:
        return False
    if left.domain and right.domain and left.domain != right.domain:
        return False
    if (
        left.domain
        and right.domain
        and left.domain == right.domain
        and left.business_name_key
        and right.business_name_key
        and left.business_name_key != right.business_name_key
    ):
        left_tokens = set(significant_business_tokens(left.business_name))
        right_tokens = set(significant_business_tokens(right.business_name))
        if left_tokens and right_tokens and left_tokens & right_tokens:
            return True
        if is_weak_business_name(left.business_name) or is_weak_business_name(right.business_name):
            return True
        return False
    return True


def _preferred_observation(observations: Iterable[_NormalizedObservation]) -> _NormalizedObservation:
    return sorted(
        observations,
        key=lambda observation: (
            is_weak_business_name(observation.business_name),
            -observation.source_confidence_level,
            -bool(observation.domain),
            -bool(observation.phones or observation.emails),
            -len(observation.business_name_key.split()),
            -len(observation.business_name),
            observation.business_name_key,
            observation.source_url,
        ),
    )[0]


def _most_common_display(values: Iterable[tuple[str, str]]) -> str:
    counts: dict[str, int] = {}
    display_lookup: dict[str, str] = {}
    for key, display in values:
        if not key or not display:
            continue
        counts[key] = counts.get(key, 0) + 1
        display_lookup.setdefault(key, display)
    if not counts:
        return ""
    selected_key = sorted(counts, key=lambda key: (-counts[key], key))[0]
    return display_lookup[selected_key]


def _canonical_website(domain: str, observations: Iterable[_NormalizedObservation]) -> str:
    ordered = tuple(observations)
    if not domain:
        return next((observation.website for observation in ordered if observation.website), "")
    for observation in ordered:
        if observation.domain != domain or not observation.website:
            continue
        scheme = urlsplit(observation.website).scheme or "https"
        return urlunsplit((scheme, domain, "/", "", ""))
    return f"https://{domain}/"


def _build_candidate_ref(
    business_name_key: str,
    town_key: str,
    domain: str,
    website: str,
    phones: tuple[str, ...],
    emails: tuple[str, ...],
) -> str:
    identifier = (
        domain
        or website
        or (phones[0].removeprefix("+") if phones else "")
        or (emails[0] if emails else "review")
    )
    return (
        f"buyer:{slugify(town_key or 'unknown-town')}:"
        f"{slugify(business_name_key or 'unknown-buyer')}:"
        f"{slugify(identifier)}"
    )


def _build_dedupe_fields(
    *,
    business_name_key: str,
    town_key: str,
    domain: str,
    phones: tuple[str, ...],
    emails: tuple[str, ...],
) -> tuple[str, ...]:
    dedupe_fields: list[str] = []
    if domain:
        dedupe_fields.append(f"domain:{domain}")
    dedupe_fields.extend(f"phone:{phone}" for phone in phones)
    dedupe_fields.extend(f"email:{email}" for email in emails)
    if not dedupe_fields:
        dedupe_fields.append(f"name_town:{business_name_key}::{town_key}")
    return tuple(dedupe_fields)


def _collapse_group(observations: list[_NormalizedObservation]) -> NormalizedBuyerCandidate:
    ordered = sorted(
        observations,
        key=lambda observation: (
            -observation.source_confidence_level,
            observation.source_key,
            observation.source_url,
        ),
    )
    preferred = _preferred_observation(ordered)
    town = _most_common_display((observation.town_key, observation.town) for observation in ordered) or preferred.town
    town_key = normalize_town(town)
    phones = _merge_ordered(phone for observation in ordered for phone in observation.phones)
    emails = _merge_ordered(email for observation in ordered for email in observation.emails)
    other_contact_hints = _merge_ordered(
        hint for observation in ordered for hint in observation.other_contact_hints
    )
    domain = next((observation.domain for observation in ordered if observation.domain), "")
    website = _canonical_website(domain, ordered)
    source_keys = _merge_ordered(observation.source_key for observation in ordered)
    evidence = tuple(
        CandidateEvidence(
            source_key=observation.source_key,
            source_url=observation.source_url,
            website=observation.website,
            contact_hints=_merge_ordered(
                (*observation.phones, *observation.emails, *observation.other_contact_hints)
            ),
            source_confidence_class=observation.source_confidence_class,
            source_confidence_level=observation.source_confidence_level,
        )
        for observation in ordered
    )
    return NormalizedBuyerCandidate(
        candidate_ref=_build_candidate_ref(
            business_name_key=preferred.business_name_key,
            town_key=town_key,
            domain=domain,
            website=website,
            phones=phones,
            emails=emails,
        ),
        business_name=preferred.business_name,
        business_name_key=preferred.business_name_key,
        town=town,
        town_key=town_key,
        website=website,
        domain=domain,
        dedupe_fields=_build_dedupe_fields(
            business_name_key=preferred.business_name_key,
            town_key=town_key,
            domain=domain,
            phones=phones,
            emails=emails,
        ),
        phones=phones,
        emails=emails,
        other_contact_hints=other_contact_hints,
        source_keys=source_keys,
        evidence=evidence,
    )


def normalize_candidates(candidates: Iterable[BuyerCandidate]) -> list[NormalizedBuyerCandidate]:
    """Normalize and deterministically collapse duplicate buyer candidates."""

    observations = sorted(
        (
            normalized
            for candidate in candidates
            if (normalized := _normalize_observation(candidate)) is not None
        ),
        key=_observation_sort_key,
    )
    if not observations:
        return []

    parents = list(range(len(observations)))
    group_members: dict[int, set[int]] = {index: {index} for index in range(len(observations))}
    seen_identifiers: dict[tuple[str, str], list[int]] = {}
    for index, observation in enumerate(observations):
        for identifier in observation.dedupe_identifiers:
            prior_indices = seen_identifiers.setdefault(identifier, [])
            checked_roots: set[int] = set()
            for other_index in prior_indices:
                other_root = _find(parents, other_index)
                if other_root in checked_roots:
                    continue
                checked_roots.add(other_root)
                if not all(
                    _can_merge(observation, observations[group_index])
                    for group_index in group_members[other_root]
                ):
                    continue
                current_root = _find(parents, index)
                merged_root = _union(parents, index, other_root)
                if merged_root == current_root == other_root:
                    continue
                merged_members = group_members.pop(current_root, {current_root}) | group_members.pop(
                    other_root,
                    {other_root},
                )
                group_members[merged_root] = merged_members
            prior_indices.append(index)

    grouped: dict[int, list[_NormalizedObservation]] = {}
    for index, observation in enumerate(observations):
        root = _find(parents, index)
        grouped.setdefault(root, []).append(observation)

    return sorted(
        (_collapse_group(group) for _, group in sorted(grouped.items())),
        key=lambda candidate: (
            candidate.town_key,
            candidate.business_name_key,
            candidate.candidate_ref,
        ),
    )
