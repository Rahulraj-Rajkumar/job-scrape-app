from __future__ import annotations

import html
import re
from datetime import datetime
from typing import Any

import httpx
from loguru import logger

from .base import BaseScraper, JobListing


class GreenhouseScraper(BaseScraper):
    name = "greenhouse"

    def __init__(self, config: dict[str, Any], slugs: dict[str, str]):
        super().__init__(config)
        self.slugs = slugs

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
        url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
        resp = self._request(client, "GET", url)
        if resp is None:
            return []
        if resp.status_code == 404:
            logger.debug(f"[greenhouse] No board found for {company} ({slug})")
            return []
        resp.raise_for_status()
        data = resp.json()

        jobs = data.get("jobs", [])
        results: list[JobListing] = []
        for job in jobs:
            listing = self._parse_job(job, company)
            if listing and self._matches_filters(listing):
                results.append(listing)
        return results

    def _parse_job(self, job: dict, company: str) -> JobListing | None:
        try:
            location_name = ""
            locations = job.get("location", {})
            if isinstance(locations, dict):
                location_name = locations.get("name", "")

            content = job.get("content", "")
            description = html.unescape(content)
            description = re.sub(r"<[^>]+>", " ", description)
            description = re.sub(r"\s+", " ", description).strip()

            updated_at = job.get("updated_at")
            posted_date = None
            if updated_at:
                try:
                    posted_date = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
                except ValueError:
                    pass

            departments = job.get("departments", [])
            team = departments[0].get("name", "") if departments else ""

            return JobListing(
                title=job.get("title", "Unknown"),
                company=company,
                location=location_name,
                url=job.get("absolute_url", ""),
                description=description,
                source="greenhouse",
                posted_date=posted_date,
                job_id=str(job.get("id", "")),
                team=team,
            )
        except Exception:
            logger.exception("[greenhouse] Failed to parse job")
            return None
