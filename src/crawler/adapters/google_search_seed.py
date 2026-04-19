"""Extract discovery seed candidates from Google-style result snapshots."""

from __future__ import annotations

from collections.abc import Sequence
import re
from urllib.parse import parse_qs, unquote, urlsplit

from src.common.models import BuyerCandidate
from src.common.sanitization import canonicalize_business_name, is_extractable_business_name
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
_RSS_ITEM_RE = re.compile(r"<item>(.*?)</item>", re.DOTALL | re.IGNORECASE)
_RSS_TITLE_RE = re.compile(r"<title>(.*?)</title>", re.DOTALL | re.IGNORECASE)
_RSS_LINK_RE = re.compile(r"<link>(.*?)</link>", re.DOTALL | re.IGNORECASE)
_RSS_DESCRIPTION_RE = re.compile(r"<description>(.*?)</description>", re.DOTALL | re.IGNORECASE)
_GENERIC_LINK_RE = re.compile(
    r"<a[^>]*href=(['\"])(.*?)\1[^>]*>(.*?)</a>",
    re.DOTALL | re.IGNORECASE,
)
_DOMAIN_PREFIX_RE = re.compile(
    r"^(?:[a-z0-9.-]+\.[a-z]{2,}|https?://\S+)(?:\s+https?://\S+)?\s+(.+)$",
    re.IGNORECASE,
)
_SLUG_PREFIX_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+){1,}\s+([A-Z].+)$")
_NUMERIC_PREFIX_RE = re.compile(r"^\d{6,}\s+(.+)$")
_YAHOO_TARGET_RE = re.compile(r"(?:^|/)RU=([^/]+)")
_LOCATION_PHRASE_RE = re.compile(
    r"\b(?:in|at|from)\s+([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){0,2})\b"
)
_RELEVANT_RESULT_RE = re.compile(
    r"\b("
    r"agro|auction|broker|brokers|buyer|buyers|chili|chilli|commission|dalal|"
    r"export|exports|exporter|gunj|market yard|masala|mirchi|oleoresin|"
    r"paprika|spice|spices|trader|traders|wholesale|wholesaler|yard"
    r")\b",
    re.IGNORECASE,
)
_EXCLUDED_RESULT_DOMAINS = (
    "bing.com",
    "britannica.com",
    "duckduckgo.com",
    "facebook.com",
    "gov.in",
    "holidify.com",
    "incredibleindia.gov.in",
    "indiatimes.com",
    "instagram.com",
    "jagran.com",
    "jagranjosh.com",
    "linkedin.com",
    "makemytrip.com",
    "search.brave.com",
    "startpage.com",
    "thehindu.com",
    "wikipedia.org",
    "x.com",
    "yahoo.com",
    "youtube.com",
    "youtu.be",
)


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
        fallback_town: str = "",
    ) -> list[BuyerCandidate]:
        candidates = self._extract_structured_candidates(
            html,
            search_page_url=search_page_url,
            fallback_town=fallback_town,
        )
        if candidates:
            return candidates
        candidates = self._extract_rss_candidates(
            html,
            search_page_url=search_page_url,
            fallback_town=fallback_town,
        )
        if candidates:
            return candidates
        return self._extract_generic_candidates(
            html,
            search_page_url=search_page_url,
            fallback_town=fallback_town,
        )

    def _extract_structured_candidates(
        self,
        html: str,
        *,
        search_page_url: str,
        fallback_town: str,
    ) -> list[BuyerCandidate]:
        candidates: list[BuyerCandidate] = []
        for block in _RESULT_RE.findall(html):
            link_match = _LINK_RE.search(block)
            title_match = _TITLE_RE.search(block)
            snippet_match = _SNIPPET_RE.search(block)
            if not link_match or not title_match:
                continue

            source_url = resolve_href(link_match.group(1), search_page_url)
            if not source_url or self._is_excluded_result_url(source_url) or is_same_domain(source_url, "google.com"):
                continue

            business_name = canonicalize_business_name(strip_tags(title_match.group(1)))
            snippet = strip_tags(snippet_match.group(1) if snippet_match else "")
            town = self._resolve_candidate_town(
                business_name,
                snippet,
                fallback_town=fallback_town,
            )
            if (
                not business_name
                or not town
                or not self._is_relevant_result(business_name, snippet)
                or not is_extractable_business_name(business_name)
            ):
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

    def _extract_rss_candidates(
        self,
        html: str,
        *,
        search_page_url: str,
        fallback_town: str,
    ) -> list[BuyerCandidate]:
        candidates: list[BuyerCandidate] = []
        for item in _RSS_ITEM_RE.findall(html):
            title_match = _RSS_TITLE_RE.search(item)
            link_match = _RSS_LINK_RE.search(item)
            if not title_match or not link_match:
                continue

            business_name = canonicalize_business_name(strip_tags(title_match.group(1)))
            source_url = resolve_href(strip_tags(link_match.group(1)), search_page_url)
            description_match = _RSS_DESCRIPTION_RE.search(item)
            snippet = strip_tags(description_match.group(1) if description_match else "")
            town = self._resolve_candidate_town(
                business_name,
                snippet,
                fallback_town=fallback_town,
            )
            if (
                not business_name
                or not source_url
                or not town
                or self._is_excluded_result_url(source_url)
                or not self._is_relevant_result(business_name, snippet)
                or not is_extractable_business_name(business_name)
            ):
                continue

            candidates.append(
                BuyerCandidate(
                    business_name=business_name,
                    source_url=source_url,
                    town=town,
                    website=source_url,
                    contact_hints=extract_contact_hints(snippet),
                    source_key=self.source_key,
                )
            )
        return candidates

    def _extract_generic_candidates(
        self,
        html: str,
        *,
        search_page_url: str,
        fallback_town: str,
    ) -> list[BuyerCandidate]:
        candidates: list[BuyerCandidate] = []
        seen_urls: set[str] = set()
        for _, href, label in _GENERIC_LINK_RE.findall(html):
            source_url = self._resolve_result_url(href, search_page_url)
            if not source_url or self._is_excluded_result_url(source_url) or source_url.casefold() in seen_urls:
                continue

            business_name = self._normalize_result_title(strip_tags(label))
            if (
                len(business_name) < 5
                or not self._is_relevant_result(business_name)
                or not is_extractable_business_name(business_name)
            ):
                continue

            town = self._resolve_candidate_town(
                business_name,
                fallback_town=fallback_town,
            )
            if not town:
                continue

            seen_urls.add(source_url.casefold())
            candidates.append(
                BuyerCandidate(
                    business_name=business_name,
                    source_url=source_url,
                    town=town,
                    website=source_url,
                    contact_hints=(),
                    source_key=self.source_key,
                )
            )
        return candidates

    def _resolve_result_url(self, href: str, base_url: str) -> str:
        google_url = resolve_href(href, base_url)
        if not google_url:
            return ""

        parsed = urlsplit(google_url)
        if parsed.path == "/url":
            params = parse_qs(parsed.query)
            target_url = params.get("q", [""])[0] or params.get("url", [""])[0]
            resolved_target = resolve_href(target_url, base_url)
            if resolved_target and not is_same_domain(resolved_target, "google.com"):
                return resolved_target
            return ""

        if parsed.path.startswith("/l/") and is_same_domain(google_url, "duckduckgo.com"):
            params = parse_qs(parsed.query)
            target_url = params.get("uddg", [""])[0]
            resolved_target = resolve_href(target_url, base_url)
            if resolved_target and not is_same_domain(resolved_target, "duckduckgo.com"):
                return resolved_target
            return ""

        if is_same_domain(google_url, "yahoo.com"):
            match = _YAHOO_TARGET_RE.search(parsed.path)
            if match:
                target_url = unquote(match.group(1))
                resolved_target = resolve_href(target_url, base_url)
                if resolved_target and not is_same_domain(resolved_target, "yahoo.com"):
                    return resolved_target
                return ""

        if is_same_domain(google_url, "google.com", "bing.com", "duckduckgo.com", "yahoo.com"):
            return ""
        return google_url

    def _normalize_result_title(self, value: str) -> str:
        return canonicalize_business_name(collapse_whitespace(value))

    def _is_relevant_result(self, *texts: str) -> bool:
        combined = collapse_whitespace(" ".join(text for text in texts if text))
        if not combined:
            return False
        return _RELEVANT_RESULT_RE.search(combined) is not None

    def _is_excluded_result_url(self, source_url: str) -> bool:
        return is_same_domain(source_url, *_EXCLUDED_RESULT_DOMAINS)

    def _resolve_candidate_town(self, *texts: str, fallback_town: str) -> str:
        detected = detect_town(*texts, town_hints=self._town_hints)
        if detected:
            return detected
        if fallback_town and self._mentions_conflicting_location(*texts, fallback_town=fallback_town):
            return ""
        return fallback_town

    def _mentions_conflicting_location(self, *texts: str, fallback_town: str) -> bool:
        combined = collapse_whitespace(" ".join(text for text in texts if text))
        if not combined:
            return False
        fallback_key = fallback_town.casefold()
        for match in _LOCATION_PHRASE_RE.finditer(combined):
            location = collapse_whitespace(match.group(1))
            if not location:
                continue
            location_key = location.casefold()
            if location_key in {fallback_key, "india"}:
                continue
            return True
        return False
