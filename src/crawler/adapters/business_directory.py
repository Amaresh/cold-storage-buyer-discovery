"""Extract buyer candidates from business directory listing pages."""

from __future__ import annotations

from collections.abc import Sequence
import re

from src.common.models import BuyerCandidate
from src.crawler.adapters._utils import (
    collapse_whitespace,
    detect_town,
    extract_contact_hints,
    resolve_href,
    strip_tags,
)

_CARD_RE = re.compile(r'<article class="directory-card">(.*?)</article>', re.DOTALL)
_PRIMARY_LINK_RE = re.compile(
    r'<a[^>]*class="listing-link"[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
    re.DOTALL,
)
_WEBSITE_RE = re.compile(
    r'<a[^>]*class="listing-website"[^>]*href="([^"]+)"[^>]*>.*?</a>',
    re.DOTALL,
)
_LOCATION_RE = re.compile(r'<p[^>]*class="listing-location"[^>]*>(.*?)</p>', re.DOTALL)
_CONTACT_RE = re.compile(r'<p[^>]*class="listing-contact"[^>]*>(.*?)</p>', re.DOTALL)


class BusinessDirectoryAdapter:
    """Normalizes directory cards into candidate buyer records."""

    source_key = "business_directory"

    def __init__(self, town_hints: Sequence[str] = ()) -> None:
        self._town_hints = tuple(
            collapse_whitespace(town) for town in town_hints if collapse_whitespace(town)
        )

    def extract_candidates(
        self,
        html: str,
        directory_page_url: str = "https://directory.example/listings/chilli-buyers",
    ) -> list[BuyerCandidate]:
        candidates: list[BuyerCandidate] = []
        for block in _CARD_RE.findall(html):
            link_match = _PRIMARY_LINK_RE.search(block)
            if not link_match:
                continue

            source_url = resolve_href(link_match.group(1), directory_page_url)
            business_name = strip_tags(link_match.group(2))
            location = strip_tags(_LOCATION_RE.search(block).group(1)) if _LOCATION_RE.search(block) else ""
            contact_text = strip_tags(_CONTACT_RE.search(block).group(1)) if _CONTACT_RE.search(block) else ""
            website_match = _WEBSITE_RE.search(block)
            website = (
                resolve_href(website_match.group(1), directory_page_url) if website_match else ""
            )
            town = detect_town(business_name, location, contact_text, town_hints=self._town_hints)
            if not business_name or not source_url or not town:
                continue

            contact_hints = extract_contact_hints(location, contact_text)
            if not website and not contact_hints:
                continue

            candidates.append(
                BuyerCandidate(
                    business_name=business_name,
                    source_url=source_url,
                    town=town,
                    website=website,
                    contact_hints=contact_hints,
                    source_key=self.source_key,
                )
            )
        return candidates
