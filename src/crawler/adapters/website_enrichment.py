"""Extract first-party buyer signals from a website snapshot."""

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

_H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.DOTALL)
_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.DOTALL)
_BODY_RE = re.compile(r"<body[^>]*>(.*?)</body>", re.DOTALL)


class WebsiteEnrichmentAdapter:
    """Extracts a first-party candidate from a website landing page."""

    source_key = "website_enrichment"

    def __init__(self, town_hints: Sequence[str] = ()) -> None:
        self._town_hints = tuple(
            collapse_whitespace(town) for town in town_hints if collapse_whitespace(town)
        )

    def extract_candidates(self, html: str, website_url: str) -> list[BuyerCandidate]:
        source_url = resolve_href(website_url, website_url)
        if not source_url:
            return []

        body = strip_tags(_BODY_RE.search(html).group(1)) if _BODY_RE.search(html) else strip_tags(html)
        title = strip_tags(_TITLE_RE.search(html).group(1)) if _TITLE_RE.search(html) else ""
        heading = strip_tags(_H1_RE.search(html).group(1)) if _H1_RE.search(html) else ""
        business_name = heading or title
        town = detect_town(business_name, title, body, town_hints=self._town_hints)
        contact_hints = extract_contact_hints(title, body)
        if not business_name or not town or not contact_hints:
            return []

        return [
            BuyerCandidate(
                business_name=business_name,
                source_url=source_url,
                town=town,
                website=source_url,
                contact_hints=contact_hints,
                source_key=self.source_key,
            )
        ]
