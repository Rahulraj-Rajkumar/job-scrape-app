from __future__ import annotations

import unittest

from main import _dedupe_in_run, _merge_slug_sources
from src.scrapers.base import JobListing


class MainHelperTests(unittest.TestCase):
    def _listing(self, source: str, job_id: str) -> JobListing:
        return JobListing(
            title="Software Engineer",
            company="Acme",
            location="Seattle, WA",
            url=f"https://example.com/{source}/{job_id}",
            description="Python",
            source=source,
            job_id=job_id,
        )

    def test_merge_slug_sources_dedupes_by_slug(self) -> None:
        merged = _merge_slug_sources(
            {"Discord": "discord", "Datadog": "datadog", "DuplicateName": "datadog"},
            ["discord", "newco", "datadog"],
        )

        self.assertEqual(set(merged.values()), {"discord", "datadog", "newco"})

    def test_dedupe_in_run_removes_duplicate_unique_keys(self) -> None:
        listings = [
            self._listing("greenhouse", "123"),
            self._listing("greenhouse", "123"),
            self._listing("greenhouse", "456"),
        ]

        deduped, duplicates = _dedupe_in_run(listings)
        self.assertEqual(len(deduped), 2)
        self.assertEqual(duplicates, 1)


if __name__ == "__main__":
    unittest.main()
