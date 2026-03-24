from __future__ import annotations

import unittest

from src.matcher import _location_score, filter_listings
from src.scrapers.base import JobListing


class MatcherTests(unittest.TestCase):
    def test_location_score_does_not_treat_non_us_country_as_us(self) -> None:
        listing = JobListing(
            title="Software Engineer",
            company="Acme",
            location="Sydney, Australia",
            url="https://example.com/job",
            description="Python backend",
            source="lever",
        )
        score = _location_score(listing, {"country": "US", "preferred_locations": [], "include_remote": True})
        self.assertEqual(score, 20.0)

    def test_filter_respects_seniority_levels_when_inferred(self) -> None:
        config = {
            "excluded_companies": [],
            "excluded_company_types": [],
            "max_yoe_required": 10,
            "country": "US",
            "seniority_levels": ["mid"],
        }

        listings = [
            JobListing(
                title="Senior Software Engineer",
                company="Acme",
                location="Seattle, WA",
                url="https://example.com/senior",
                description="Distributed systems",
                source="lever",
            ),
            JobListing(
                title="Software Engineer II",
                company="Acme",
                location="Seattle, WA",
                url="https://example.com/mid",
                description="Distributed systems",
                source="lever",
            ),
            JobListing(
                title="Software Engineer",
                company="Acme",
                location="Seattle, WA",
                url="https://example.com/unspecified",
                description="Distributed systems",
                source="lever",
            ),
        ]

        filtered = filter_listings(listings, config)
        titles = {item.title for item in filtered}

        self.assertIn("Software Engineer II", titles)
        self.assertIn("Software Engineer", titles)
        self.assertNotIn("Senior Software Engineer", titles)


if __name__ == "__main__":
    unittest.main()
