"""Live HTML loading for search seeds, directory pages, and candidate websites."""

from __future__ import annotations

from collections.abc import Sequence
from urllib import error, request
from urllib.parse import quote_plus

from src.common.models import CrawlJob
from src.runtime.sample_catalog import HtmlSnapshot


class UrlFetcher:
    """Fetches HTML over HTTP with a browser-like user agent."""

    def __init__(self, *, user_agent: str, timeout_seconds: int) -> None:
        self._user_agent = user_agent
        self._timeout_seconds = timeout_seconds

    def fetch(self, url: str) -> str | None:
        req = request.Request(
            url,
            headers={
                "User-Agent": self._user_agent,
                "Accept-Language": "en-IN,en;q=0.9",
            },
        )
        try:
            with request.urlopen(req, timeout=self._timeout_seconds) as response:
                charset = response.headers.get_content_charset() or "utf-8"
                return response.read().decode(charset, errors="replace")
        except (error.HTTPError, error.URLError, TimeoutError):
            return None


class LiveSnapshotCatalog:
    """Loads real HTML for search pages and business websites."""

    mode_name = "live_web_fetch"

    def __init__(
        self,
        *,
        google_search_url_template: str,
        search_fallback_url_template: str,
        search_fallback_url_templates: Sequence[str] = (),
        business_directory_url_template: str,
        user_agent: str,
        timeout_seconds: int,
        fetcher: UrlFetcher | None = None,
    ) -> None:
        self._google_search_url_template = google_search_url_template
        self._search_fallback_url_templates = self._unique_templates(
            [*search_fallback_url_templates, search_fallback_url_template]
        )
        self._business_directory_url_template = business_directory_url_template
        self._fetcher = fetcher or UrlFetcher(
            user_agent=user_agent,
            timeout_seconds=timeout_seconds,
        )

    def snapshot_for_job(self, job: CrawlJob) -> HtmlSnapshot | None:
        if job.source_name == "google_search_seed":
            return self._search_snapshot_for_job(job)

        source_url = self._job_url(job)
        if not source_url:
            return None

        html = self._fetcher.fetch(source_url)

        if not html:
            return None

        return HtmlSnapshot(
            label=f"{job.source_name}_{job.town_name.casefold()}",
            source_url=source_url,
            html=html,
        )

    def website_snapshot(self, website_url: str) -> HtmlSnapshot | None:
        html = self._fetcher.fetch(website_url)
        if not html:
            return None
        return HtmlSnapshot(
            label="website_fetch",
            source_url=website_url,
            html=html,
        )

    def _search_snapshot_for_job(self, job: CrawlJob) -> HtmlSnapshot | None:
        for source_url in self._search_urls(job):
            html = self._fetcher.fetch(source_url)
            if not html or self._looks_like_unusable_search_page(html):
                continue
            return HtmlSnapshot(
                label=f"{job.source_name}_{job.town_name.casefold()}",
                source_url=source_url,
                html=html,
            )
        return None

    def _job_url(self, job: CrawlJob) -> str:
        if job.source_name == "google_search_seed":
            return self._render_template(self._google_search_url_template, job)
        if job.source_name == "business_directory":
            return self._render_template(self._business_directory_url_template, job)
        return ""

    def _search_urls(self, job: CrawlJob) -> tuple[str, ...]:
        urls: list[str] = []
        seen: set[str] = set()
        for template in (self._google_search_url_template, *self._search_fallback_url_templates):
            rendered = self._render_template(template, job)
            if not rendered or rendered.casefold() in seen:
                continue
            seen.add(rendered.casefold())
            urls.append(rendered)
        return tuple(urls)

    def _render_template(self, template: str, job: CrawlJob) -> str:
        if not template.strip():
            return ""
        return template.format(
            query=quote_plus(job.query_text),
            town=quote_plus(job.town_name),
            state=quote_plus(job.town_state),
            intent=quote_plus(job.intent_query),
        )

    def _looks_like_unusable_search_page(self, html: str) -> bool:
        return self._looks_like_blocked_search_page(html) or not self._looks_like_search_results_page(
            html
        )

    def _looks_like_search_results_page(self, html: str) -> bool:
        normalized = html.casefold()
        if "<rss" in normalized and "<item>" in normalized:
            return True
        if any(
            marker in normalized
            for marker in (
                'class="search-result"',
                'class="result-link"',
                'class="result-title"',
                'class="comptitle',
                'class="dd algo',
                'class="wgl-title',
                "/url?q=",
                'data-testid="gl-title-link"',
            )
        ):
            return True
        anchor_count = normalized.count("<a ") + normalized.count("<a>")
        return anchor_count >= 5

    def _looks_like_blocked_search_page(self, html: str) -> bool:
        normalized = html.casefold()
        return any(
            marker in normalized
            for marker in (
                "anomaly.js",
                "captcha-delivery.com",
                "confirm you’re a human being",
                "confirm you're a human being",
                "detected unusual traffic from your computer network",
                "/httpservice/retry/enablejs",
                "g-recaptcha",
                "our systems have detected unusual traffic",
                "proof of work captcha",
                "recaptcha",
                "unusual traffic",
                "enable javascript on your web browser",
            )
        )

    def _unique_templates(self, templates: Sequence[str]) -> tuple[str, ...]:
        ordered: list[str] = []
        seen: set[str] = set()
        for template in templates:
            normalized = template.strip()
            if not normalized or normalized.casefold() in seen:
                continue
            seen.add(normalized.casefold())
            ordered.append(normalized)
        return tuple(ordered)
