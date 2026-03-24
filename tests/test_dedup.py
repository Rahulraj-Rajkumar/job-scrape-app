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


if __name__ == "__main__":
    unittest.main()
