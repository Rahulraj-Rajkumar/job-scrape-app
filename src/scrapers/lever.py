from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx
from loguru import logger

from .base import BaseScraper, JobListing


class LeverScraper(BaseScraper):
    name = "lever"

    def __init__(self, config: dict[str, Any], slugs: dict[str, str]):
        super().__init__(config)
        self.slugs = slugs  # {company_name: slug}

    def scrape(self) -> list[JobListing]:
        slug_items = list(self.slugs.items())
        return self._run_parallel(slug_items, self._fetch_company_with_client, "company/slug")

    def _fetch_company_with_client(self, item: tuple[str, str]) -> list[JobListing]:
        company, slug = item
        with self._get_client() as client:
            return self._fetch_company(client, company, slug)

    def _fetch_company(
        self, client: httpx.Client, company: str, slug: str
    ) -> list[JobListing]:
        url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
        resp = self._request(client, "GET", url)
        if resp is None:
            return []
        if resp.status_code == 404:
            logger.debug(f"[lever] No board found for {company} ({slug})")
            return []
        resp.raise_for_status()
        data = resp.json()

        if not isinstance(data, list):
            return []

        results: list[JobListing] = []
        for posting in data:
            listing = self._parse_posting(posting, company)
            if listing and self._matches_filters(listing):
                results.append(listing)
        return results

    def _parse_posting(self, posting: dict, company: str) -> JobListing | None:
        try:
            categories = posting.get("categories", {})
            location = categories.get("location", posting.get("workplaceType", ""))
            team = categories.get("team", "")

            description_parts = []
            for list_block in posting.get("lists", []):
                description_parts.append(list_block.get("text", ""))
                description_parts.append(list_block.get("content", ""))
            desc_text = posting.get("descriptionPlain", "") or " ".join(description_parts)

            created_at = posting.get("createdAt")
            posted_date = None
            if created_at:
                posted_date = datetime.fromtimestamp(created_at / 1000, tz=timezone.utc)

            return JobListing(
                title=posting.get("text", "Unknown"),
                company=company,
                location=location,
                url=posting.get("hostedUrl", posting.get("applyUrl", "")),
                description=desc_text,
                source="lever",
                posted_date=posted_date,
                job_id=posting.get("id", ""),
                team=team,
                metadata={"commitment": categories.get("commitment", "")},
            )
        except Exception:
            logger.exception("[lever] Failed to parse posting")
            return None
