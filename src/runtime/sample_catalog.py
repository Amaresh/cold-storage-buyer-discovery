"""Selected snapshot inputs used for deterministic end-to-end worker runs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus

from src.common.models import CrawlJob
from src.pipeline.normalizer import normalize_domain

_JOB_SNAPSHOTS = {
    ("google_search_seed", "guntur"): "google_search_seed_guntur.html",
    ("business_directory", "guntur"): "business_directory_guntur.html",
}

_WEBSITE_SNAPSHOTS = {
    "srilakshmi-gunj.example": "website_sri_lakshmi_gunj.html",
    "balaji-commission.example": "website_sri_balaji_commission.html",
}


@dataclass(frozen=True, slots=True)
class HtmlSnapshot:
    label: str
    source_url: str
    html: str


class SampleSnapshotCatalog:
    """Loads the curated HTML snapshots used by the live sample worker path."""

    mode_name = "selected_sample_snapshots"

    def __init__(self, snapshots_dir: Path) -> None:
        self._snapshots_dir = snapshots_dir

    def snapshot_for_job(self, job: CrawlJob) -> HtmlSnapshot | None:
        filename = _JOB_SNAPSHOTS.get((job.source_name, job.town_name.casefold()))
        if filename is None:
            return None

        if job.source_name == "google_search_seed":
            source_url = f"https://www.google.com/search?q={quote_plus(job.query_text)}"
        else:
            source_url = f"https://directory.example/search?q={quote_plus(job.query_text)}"

        return HtmlSnapshot(
            label=filename.removesuffix(".html"),
            source_url=source_url,
            html=self._read_html(filename),
        )

    def website_snapshot(self, website_url: str) -> HtmlSnapshot | None:
        filename = _WEBSITE_SNAPSHOTS.get(normalize_domain(website_url))
        if filename is None:
            return None
        return HtmlSnapshot(
            label=filename.removesuffix(".html"),
            source_url=website_url,
            html=self._read_html(filename),
        )

    def _read_html(self, filename: str) -> str:
        return (self._snapshots_dir / filename).read_text(encoding="utf-8")
