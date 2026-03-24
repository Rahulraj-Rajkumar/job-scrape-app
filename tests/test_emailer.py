from __future__ import annotations

import unittest
from datetime import datetime, timezone

from src.emailer import format_additional_jobs_report
from src.scrapers.base import JobListing


class EmailerTests(unittest.TestCase):
    def _scored(self, job_id: str, title: str, score: float) -> dict:
        listing = JobListing(
            title=title,
            company="Acme",
            location="Seattle, WA",
            url=f"https://example.com/{job_id}",
            description="Build backend systems and APIs",
            source="lever",
            job_id=job_id,
            posted_date=datetime(2026, 3, 24, tzinfo=timezone.utc),
        )
        return {
            "listing": listing,
            "total_score": score,
            "matching_skills": ["Python"],
        }

    def test_format_additional_jobs_report_excludes_top_and_dedupes(self) -> None:
        top = self._scored("top-1", "Top Role", 98.2)
        next_1 = self._scored("next-1", "Next Role 1", 95.0)
        next_2 = self._scored("next-2", "Next Role 2", 93.0)
        duplicate_next_1 = self._scored("next-1", "Next Role 1 Duplicate", 90.0)

        filename, html, count = format_additional_jobs_report(
            [top, next_1, next_2, duplicate_next_1],
            excluded_keys={top["listing"].unique_key()},
            max_jobs=2,
        )

        self.assertTrue(filename.startswith("additional_jobs_by_score_"))
        self.assertTrue(filename.endswith(".html"))
        self.assertEqual(count, 2)
        self.assertIn("Additional Jobs by Score", html)
        self.assertIn("Next Role 1", html)
        self.assertIn("Next Role 2", html)
        self.assertNotIn("Top Role", html)


if __name__ == "__main__":
    unittest.main()
