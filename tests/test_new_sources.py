from __future__ import annotations

import unittest

from src.scrapers.amazon import AmazonScraper
from src.scrapers.microsoft import MicrosoftScraper


class NewSourcesTests(unittest.TestCase):
    def test_amazon_uses_broad_default_queries_when_not_overridden(self) -> None:
        scraper = AmazonScraper(config={})
        self.assertEqual(
            scraper._build_queries(),
            ["software engineer", "software development engineer", "sde"],
        )

    def test_amazon_can_use_global_search_queries_when_enabled(self) -> None:
        scraper = AmazonScraper(
            config={
                "search_queries": ["backend software engineer", "backend software engineer", "full stack"],
                "amazon": {"use_global_search_queries": True},
            }
        )
        self.assertEqual(scraper._build_queries(), ["backend software engineer", "full stack"])

    def test_amazon_parse_job_maps_core_fields(self) -> None:
        scraper = AmazonScraper(config={"search_queries": ["software engineer"]})
        listing = scraper._parse_job(
            {
                "id_icims": "12345",
                "title": "Software Development Engineer",
                "company_name": "Amazon Web Services",
                "job_path": "/en/jobs/12345/software-development-engineer",
                "description": "<p>Build distributed systems.</p>",
                "location": "Seattle, WA",
                "updated_time": "2026-03-25T20:15:00.000Z",
                "team": "AWS Compute",
            }
        )

        self.assertIsNotNone(listing)
        assert listing is not None
        self.assertEqual(listing.source, "amazon")
        self.assertEqual(listing.job_id, "12345")
        self.assertEqual(listing.company, "Amazon Web Services")
        self.assertIn("distributed systems", listing.description.lower())
        self.assertTrue(listing.url.startswith("https://www.amazon.jobs/en/jobs/12345"))

    def test_microsoft_parse_position_summary_maps_core_fields(self) -> None:
        scraper = MicrosoftScraper(config={"search_queries": ["software engineer"]})
        listing = scraper._parse_position_summary(
            {
                "id": 123456,
                "name": "Software Engineer II",
                "locations": ["Redmond, WA, United States"],
                "postedTs": 1774300000,
                "department": "Software Engineering",
                "positionUrl": "/careers/job/123456",
                "atsJobId": "200000001",
                "displayJobId": "200000001",
            }
        )

        self.assertIsNotNone(listing)
        assert listing is not None
        self.assertEqual(listing.source, "microsoft")
        self.assertEqual(listing.job_id, "123456")
        self.assertEqual(listing.company, "Microsoft")
        self.assertIn("Redmond", listing.location)
        self.assertTrue(listing.url.startswith("https://apply.careers.microsoft.com/careers/job/123456"))

    def test_microsoft_uses_broad_default_queries_when_not_overridden(self) -> None:
        scraper = MicrosoftScraper(config={})
        self.assertEqual(
            scraper._build_queries(),
            [
                "software engineer",
                "software development engineer",
                "backend engineer",
                "full stack engineer",
                "sde",
            ],
        )

    def test_microsoft_can_use_global_search_queries_when_enabled(self) -> None:
        scraper = MicrosoftScraper(
            config={
                "search_queries": ["backend software engineer", "backend software engineer", "full stack"],
                "microsoft": {"use_global_search_queries": True},
            }
        )
        self.assertEqual(scraper._build_queries(), ["backend software engineer", "full stack"])

    def test_microsoft_merge_position_details_enriches_description(self) -> None:
        scraper = MicrosoftScraper(config={"search_queries": ["software engineer"]})
        summary = scraper._parse_position_summary(
            {
                "id": 2200000,
                "name": "Software Engineer II",
                "locations": ["Redmond, WA, United States"],
                "postedTs": 1774300000,
                "department": "Software Engineering",
                "positionUrl": "/careers/job/2200000",
            }
        )
        assert summary is not None
        listing = scraper._merge_position_details(
            summary,
            {
                "name": "Software Engineer II",
                "publicUrl": "https://apply.careers.microsoft.com/careers/job/2200000",
                "location": "Redmond, WA, United States",
                "jobDescription": "<div>Build cloud services.</div>",
                "department": "Software Engineering",
                "displayJobId": "2200000",
                "atsJobId": "2200000",
            },
        )

        self.assertIsNotNone(listing)
        self.assertEqual(listing.source, "microsoft")
        self.assertEqual(listing.job_id, "2200000")
        self.assertIn("cloud services", listing.description.lower())
        self.assertIn("/careers/job/2200000", listing.url)


if __name__ == "__main__":
    unittest.main()
