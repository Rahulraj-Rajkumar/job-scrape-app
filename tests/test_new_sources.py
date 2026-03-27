from __future__ import annotations

import unittest
from unittest.mock import patch

from src.scrapers.amazon import AmazonScraper
from src.scrapers.meta import MetaScraper
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

    def test_meta_parse_job_page_maps_core_fields(self) -> None:
        scraper = MetaScraper(config={"search_queries": ["software engineer"]})
        listing = scraper._parse_job_page(
            """
            <html>
              <head>
                <meta name="description" content="Fallback summary" />
                <script type="application/ld+json">
                {
                  "@context": "http://schema.org/",
                  "@type": "JobPosting",
                  "title": "Software Engineer, Infrastructure",
                  "description": "Build distributed systems &amp; developer tooling.",
                  "responsibilities": "Own backend services and improve reliability.",
                  "qualifications": "4+ years of experience with Python and distributed systems.",
                  "datePosted": "2026-03-20T10:00:00-07:00",
                  "validThrough": "2026-04-20T10:00:00-07:00",
                  "employmentType": "FULL_TIME",
                  "directApply": true,
                  "hiringOrganization": {"@type": "Organization", "name": "Meta"},
                  "jobLocation": [
                    {
                      "@type": "Place",
                      "name": "Seattle, WA",
                      "address": {
                        "@type": "PostalAddress",
                        "addressLocality": "Seattle",
                        "addressRegion": "WA",
                        "addressCountry": {"@type": "Country", "name": ["USA"]}
                      }
                    },
                    {
                      "@type": "Place",
                      "name": "Menlo Park, CA",
                      "address": {
                        "@type": "PostalAddress",
                        "addressLocality": "Menlo Park",
                        "addressRegion": "CA",
                        "addressCountry": {"@type": "Country", "name": ["USA"]}
                      }
                    }
                  ]
                }
                </script>
              </head>
            </html>
            """,
            "https://www.metacareers.com/profile/job_details/1234567890",
        )

        self.assertIsNotNone(listing)
        assert listing is not None
        self.assertEqual(listing.source, "meta")
        self.assertEqual(listing.company, "Meta")
        self.assertEqual(listing.job_id, "1234567890")
        self.assertIn("Seattle, WA", listing.location)
        self.assertIn("Menlo Park, CA", listing.location)
        self.assertIn("distributed systems", listing.description.lower())
        self.assertEqual(listing.metadata["employment_type"], "FULL_TIME")

    def test_meta_fetch_sitemap_entries_extracts_public_job_urls(self) -> None:
        scraper = MetaScraper(config={})

        class StubResponse:
            status_code = 200
            text = """
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
              <url>
                <loc>https://www.metacareers.com/profile/job_details/111</loc>
                <lastmod>2026-03-26T19:18:08-07:00</lastmod>
              </url>
              <url>
                <loc>https://www.metacareers.com/profile/job_details/222</loc>
                <lastmod>2026-03-20T08:00:00-07:00</lastmod>
              </url>
            </urlset>
            """

        with patch.object(scraper, "_request", return_value=StubResponse()):
            with scraper._get_client() as client:
                entries = scraper._fetch_sitemap_entries(client)

        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0][0], "https://www.metacareers.com/profile/job_details/111")
        self.assertIsNotNone(entries[0][1])


if __name__ == "__main__":
    unittest.main()
