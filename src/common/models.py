"""Typed models shared across the buyer-discovery scaffold."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import re
from typing import Literal


def slugify(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", value.strip().lower())
    normalized = normalized.strip("-")
    return normalized or "item"


@dataclass(frozen=True, slots=True)
class SourceConfig:
    """Declares a crawler source that can later be backed by an adapter."""

    name: str
    enabled: bool = True
    priority: int = 100
    adapter_key: str = ""
    notes: str = ""

    @property
    def slug(self) -> str:
        return slugify(self.name)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class TownSeed:
    """A starter location used to fan out crawl jobs."""

    name: str
    state: str = ""
    priority: int = 100

    @property
    def slug(self) -> str:
        return slugify(" ".join(part for part in (self.name, self.state) if part))

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CrawlJob:
    """A deterministic unit of work for a source/town/query combination."""

    job_id: str
    source_name: str
    town_name: str
    town_state: str
    intent_query: str
    query_text: str
    priority: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class BuyerCandidate:
    """Normalized buyer candidate details extracted from a discovery source."""

    business_name: str
    source_url: str
    town: str
    website: str = ""
    contact_hints: tuple[str, ...] = ()
    source_key: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


ReviewState = Literal["needs_review", "auto_approved"]


@dataclass(frozen=True, slots=True)
class CandidateEvidence:
    """Evidence retained for a normalized candidate across crawl sources."""

    source_key: str
    source_url: str
    website: str = ""
    contact_hints: tuple[str, ...] = ()
    source_confidence_class: str = ""
    source_confidence_level: int = 0

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class NormalizedBuyerCandidate:
    """Deterministic buyer record after normalization and duplicate collapse."""

    candidate_ref: str
    business_name: str
    business_name_key: str
    town: str
    town_key: str
    website: str = ""
    domain: str = ""
    dedupe_fields: tuple[str, ...] = ()
    phones: tuple[str, ...] = ()
    emails: tuple[str, ...] = ()
    other_contact_hints: tuple[str, ...] = ()
    source_keys: tuple[str, ...] = ()
    evidence: tuple[CandidateEvidence, ...] = ()

    @property
    def evidence_count(self) -> int:
        return len(self.evidence)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ScoredBuyerCandidate:
    """Normalized candidate plus deterministic confidence scoring output."""

    candidate: NormalizedBuyerCandidate
    confidence_score: float
    review_state: ReviewState
    score_reasons: tuple[str, ...] = ()

    @property
    def auto_approved(self) -> bool:
        return self.review_state == "auto_approved"

    def to_dict(self) -> dict[str, object]:
        return asdict(self)
