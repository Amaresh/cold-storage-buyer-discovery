from __future__ import annotations

from src.common.models import CrawlJob
from src.runtime.live_catalog import LiveSnapshotCatalog


class _StubFetcher:
    def __init__(self, responses: dict[str, str | None]) -> None:
        self._responses = responses
        self.urls: list[str] = []

    def fetch(self, url: str) -> str | None:
        self.urls.append(url)
        return self._responses.get(url)


def test_live_catalog_falls_back_when_google_returns_enablejs_shell() -> None:
    google_url = "https://www.google.com/search?q=Guntur+chilli+wholesaler"
    fallback_url = "https://html.duckduckgo.com/html/?q=Guntur+chilli+wholesaler"
    fetcher = _StubFetcher(
        {
            google_url: """
            <html>
              <body>
                <meta content="0;url=/httpservice/retry/enablejs?sei=abc" http-equiv="refresh">
              </body>
            </html>
            """,
            fallback_url: """
            <html>
              <body>
                <a href="https://example.com/1">one</a>
                <a href="https://example.com/2">two</a>
                <a href="https://example.com/3">three</a>
                <a href="https://example.com/4">four</a>
                <a href="https://example.com/5">five</a>
                <a href="https://example.com/6">six</a>
              </body>
            </html>
            """,
        }
    )
    catalog = LiveSnapshotCatalog(
        google_search_url_template="https://www.google.com/search?q={query}",
        business_directory_url_template="",
        user_agent="test-agent",
        timeout_seconds=5,
        fetcher=fetcher,  # type: ignore[arg-type]
        search_fallback_url_template="https://html.duckduckgo.com/html/?q={query}",
    )
    job = CrawlJob(
        job_id="google:guntur:chilli-wholesaler",
        source_name="google_search_seed",
        town_name="Guntur",
        town_state="Andhra Pradesh",
        intent_query="chilli wholesaler",
        query_text="Guntur chilli wholesaler",
        priority=1,
    )

    snapshot = catalog.snapshot_for_job(job)

    assert snapshot is not None
    assert snapshot.source_url == fallback_url
    assert fetcher.urls == [google_url, fallback_url]


def test_live_catalog_tries_multiple_fallbacks_until_search_results_look_usable() -> None:
    google_url = "https://www.google.com/search?q=Guntur+chilli+wholesaler"
    startpage_url = "https://www.startpage.com/do/dsearch?query=Guntur+chilli+wholesaler"
    yahoo_url = "https://search.yahoo.com/search?p=Guntur+chilli+wholesaler"
    fetcher = _StubFetcher(
        {
            google_url: """
            <html>
              <body>
                <meta content="0;url=/httpservice/retry/enablejs?sei=abc" http-equiv="refresh">
              </body>
            </html>
            """,
            startpage_url: "<html><body>Confirm you're a human being</body></html>",
            yahoo_url: """
            <html>
              <body>
                <a href="https://example.com/1">one</a>
                <a href="https://example.com/2">two</a>
                <a href="https://example.com/3">three</a>
                <a href="https://example.com/4">four</a>
                <a href="https://example.com/5">five</a>
                <a href="https://example.com/6">six</a>
              </body>
            </html>
            """,
        }
    )
    catalog = LiveSnapshotCatalog(
        google_search_url_template="https://www.google.com/search?q={query}",
        search_fallback_url_templates=(
            "https://www.startpage.com/do/dsearch?query={query}",
            "https://search.yahoo.com/search?p={query}",
        ),
        business_directory_url_template="",
        user_agent="test-agent",
        timeout_seconds=5,
        fetcher=fetcher,  # type: ignore[arg-type]
        search_fallback_url_template="https://html.duckduckgo.com/html/?q={query}",
    )
    job = CrawlJob(
        job_id="google:guntur:chilli-wholesaler",
        source_name="google_search_seed",
        town_name="Guntur",
        town_state="Andhra Pradesh",
        intent_query="chilli wholesaler",
        query_text="Guntur chilli wholesaler",
        priority=1,
    )

    snapshot = catalog.snapshot_for_job(job)

    assert snapshot is not None
    assert snapshot.source_url == yahoo_url
    assert fetcher.urls == [google_url, startpage_url, yahoo_url]
