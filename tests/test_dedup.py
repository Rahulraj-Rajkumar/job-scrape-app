from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from src.dedup import DedupStore
from src.scrapers.base import JobListing


class DedupStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmpdir.name) / "jobs.db")
        self.store = DedupStore(self.db_path)

    def tearDown(self) -> None:
        self.store.close()
        self.tmpdir.cleanup()

    def _listing(self, source: str, job_id: str, title: str = "Software Engineer") -> JobListing:
        return JobListing(
            title=title,
            company="Acme",
            location="Seattle, WA",
            url=f"https://example.com/{source}/{job_id}",
            description="Python distributed systems",
            source=source,
            job_id=job_id,
        )

    def test_filter_new_removes_seen_and_in_run_duplicates(self) -> None:
        already_seen = self._listing("lever", "seen-1")
        self.store.mark_seen(already_seen, score=88.0)

        incoming = [
            already_seen,
            self._listing("lever", "seen-1"),  # duplicate of seen
            self._listing("greenhouse", "new-1"),
            self._listing("greenhouse", "new-1"),  # in-run duplicate
            self._listing("ashby", "new-2"),
        ]

        new_items = self.store.filter_new(incoming)
        keys = {item.unique_key() for item in new_items}

        self.assertEqual(keys, {"greenhouse:new-1", "ashby:new-2"})
        self.assertEqual(len(new_items), 2)

    def test_get_recent_jobs_sorts_by_posted_then_first_seen(self) -> None:
        older_posted = JobListing(
            title="Backend Engineer",
            company="Acme",
            location="Seattle, WA",
            url="https://example.com/older",
            description="Python",
            source="lever",
            job_id="older",
            posted_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        newer_posted = JobListing(
            title="Platform Engineer",
            company="Acme",
            location="Seattle, WA",
            url="https://example.com/newer",
            description="Go",
            source="lever",
            job_id="newer",
            posted_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )

        self.store.mark_seen(older_posted, score=70.0)
        self.store.mark_seen(newer_posted, score=90.0)

        recent = self.store.get_recent_jobs(limit=2)
        self.assertEqual(len(recent), 2)
        self.assertEqual(recent[0]["unique_key"], "lever:newer")
        self.assertEqual(recent[1]["unique_key"], "lever:older")

    def test_query_jobs_filters_by_company_source_and_posted_date(self) -> None:
        older = JobListing(
            title="Backend Engineer",
            company="Acme",
            location="Seattle, WA",
            url="https://example.com/acme-older",
            description="Python services",
            source="lever",
            job_id="acme-older",
            posted_date=datetime(2024, 1, 5, tzinfo=timezone.utc),
        )
        newer = JobListing(
            title="Platform Engineer",
            company="Acme Cloud",
            location="Remote",
            url="https://example.com/acme-newer",
            description="Distributed systems",
            source="greenhouse",
            job_id="acme-newer",
            posted_date=datetime(2025, 4, 10, tzinfo=timezone.utc),
        )
        other = JobListing(
            title="Data Engineer",
            company="Globex",
            location="Austin, TX",
            url="https://example.com/globex",
            description="Analytics pipelines",
            source="lever",
            job_id="globex",
            posted_date=datetime(2025, 4, 12, tzinfo=timezone.utc),
        )

        self.store.mark_seen(older, score=72.0)
        self.store.mark_seen(newer, score=91.0)
        self.store.mark_seen(other, score=84.0)

        result = self.store.query_jobs(
            {
                "company": "Acme",
                "source": "greenhouse",
                "posted_from": "2025-01-01",
                "sort": "score",
                "direction": "desc",
            }
        )

        self.assertEqual(result["filtered_count"], 1)
        self.assertEqual(result["items"][0]["unique_key"], "greenhouse:acme-newer")

    def test_query_jobs_supports_sorting_and_pagination(self) -> None:
        for index, score in enumerate([66.0, 88.0, 77.0], start=1):
            listing = JobListing(
                title=f"Engineer {index}",
                company="Acme",
                location="Seattle, WA",
                url=f"https://example.com/job-{index}",
                description="Platform work",
                source="lever",
                job_id=f"job-{index}",
                posted_date=datetime(2025, index, 1, tzinfo=timezone.utc),
            )
            self.store.mark_seen(listing, score=score)

        first_page = self.store.query_jobs(
            {
                "sort": "score",
                "direction": "desc",
                "page_size": "2",
                "page": "1",
            }
        )
        second_page = self.store.query_jobs(
            {
                "sort": "score",
                "direction": "desc",
                "page_size": "2",
                "page": "2",
            }
        )

        self.assertEqual([item["score"] for item in first_page["items"]], [88.0, 77.0])
        self.assertEqual([item["score"] for item in second_page["items"]], [66.0])
        self.assertTrue(first_page["has_next"])
        self.assertTrue(second_page["has_previous"])


if __name__ == "__main__":
    unittest.main()
