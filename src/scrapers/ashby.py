from __future__ import annotations

import html
import re
from datetime import datetime
from typing import Any

import httpx
from loguru import logger

from .base import BaseScraper, JobListing


class AshbyScraper(BaseScraper):
    name = "ashby"

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
        url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
        resp = self._request(client, "GET", url)
        if resp is None:
            return []
        if resp.status_code == 404:
            logger.debug(f"[ashby] No board found for {company} ({slug})")
            return []
        resp.raise_for_status()
        data = resp.json()

        jobs = data.get("jobs", [])
        results: list[JobListing] = []
        for job in jobs:
            listing = self._parse_job(job, company, slug)
            if listing and self._matches_filters(listing):
                results.append(listing)
        return results

    def _parse_job(self, job: dict, company: str, slug: str) -> JobListing | None:
        try:
            location = job.get("location", "")
            if isinstance(location, dict):
                location = location.get("name", str(location))

            description = job.get("descriptionPlain", "") or job.get("description", "")
            description = html.unescape(description)
            description = re.sub(r"<[^>]+>", " ", description)
            description = re.sub(r"\s+", " ", description).strip()

            published_at = job.get("publishedAt")
            posted_date = None
            if published_at:
                try:
                    posted_date = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                except ValueError:
                    pass

            team = job.get("departmentName", "") or job.get("team", "")
            job_id = str(job.get("id", ""))
            job_url = f"https://jobs.ashbyhq.com/{slug}/{job_id}"

            return JobListing(
                title=job.get("title", "Unknown"),
                company=company,
                location=location,
                url=job_url,
                description=description,
                source="ashby",
                posted_date=posted_date,
                job_id=job_id,
                team=team,
            )
        except Exception:
            logger.exception("[ashby] Failed to parse job")
            return None
