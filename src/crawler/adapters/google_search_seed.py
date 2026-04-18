"""Extract discovery seed candidates from Google-style result snapshots."""

from __future__ import annotations

from collections.abc import Sequence
import re

from src.common.models import BuyerCandidate
from src.crawler.adapters._utils import (
    collapse_whitespace,
    detect_town,
    extract_contact_hints,
    is_same_domain,
    resolve_href,
    strip_tags,
)

_RESULT_RE = re.compile(r'<article class="search-result">(.*?)</article>', re.DOTALL)
_LINK_RE = re.compile(r'<a[^>]*class="result-link"[^>]*href="([^"]+)"[^>]*>(.*?)</a>', re.DOTALL)
_TITLE_RE = re.compile(r'<h3[^>]*class="result-title"[^>]*>(.*?)</h3>', re.DOTALL)
_SNIPPET_RE = re.compile(r'<p[^>]*class="result-snippet"[^>]*>(.*?)</p>', re.DOTALL)


class GoogleSearchSeedAdapter:
    """Turns search results into seed-only buyer candidates."""

    source_key = "google_search_seed"

    def __init__(self, town_hints: Sequence[str] = ()) -> None:
        self._town_hints = tuple(
            collapse_whitespace(town) for town in town_hints if collapse_whitespace(town)
        )

    def extract_candidates(
        self,
        html: str,
        search_page_url: str = "https://www.google.com/search?q=chilli+buyers",
    ) -> list[BuyerCandidate]:
        candidates: list[BuyerCandidate] = []
        for block in _RESULT_RE.findall(html):
            link_match = _LINK_RE.search(block)
            title_match = _TITLE_RE.search(block)
            snippet_match = _SNIPPET_RE.search(block)
            if not link_match or not title_match:
                continue

            source_url = resolve_href(link_match.group(1), search_page_url)
            if not source_url or is_same_domain(source_url, "google.com"):
                continue

            business_name = strip_tags(title_match.group(1))
            snippet = strip_tags(snippet_match.group(1) if snippet_match else "")
            town = detect_town(business_name, snippet, town_hints=self._town_hints)
            if not business_name or not town:
                continue

            contact_hints = extract_contact_hints(snippet)
            candidates.append(
                BuyerCandidate(
                    business_name=business_name,
                    source_url=source_url,
                    town=town,
                    website=source_url,
                    contact_hints=contact_hints,
                    source_key=self.source_key,
                )
            )
        return candidates
