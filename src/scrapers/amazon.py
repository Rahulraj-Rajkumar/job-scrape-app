from __future__ import annotations

import html
import re
from datetime import datetime
from typing import Any

import httpx
from loguru import logger

from .base import BaseScraper, JobListing


class AmazonScraper(BaseScraper):
    name = "amazon"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        amazon_cfg = config.get("amazon", {}) if isinstance(config, dict) else {}

        self.search_url = str(amazon_cfg.get("search_url", "https://www.amazon.jobs/en/search.json"))
        self.job_base_url = str(amazon_cfg.get("job_base_url", "https://www.amazon.jobs"))
        self.max_pages_per_query = max(1, int(amazon_cfg.get("max_pages_per_query", 4)))
        self.page_size = max(1, min(100, int(amazon_cfg.get("page_size", 25))))
        self.sort = str(amazon_cfg.get("sort", "recent"))
        self.use_global_search_queries = bool(amazon_cfg.get("use_global_search_queries", False))
        self.amazon_queries = [str(q).strip() for q in amazon_cfg.get("queries", []) if str(q).strip()]
        extra_params = amazon_cfg.get("query_params", {})
        self.query_params = extra_params if isinstance(extra_params, dict) else {}

    def scrape(self) -> list[JobListing]:
        queries = self._build_queries()

        results: list[JobListing] = []
        seen_keys: set[str] = set()

        with self._get_client() as client:
            for query in queries:
                results.extend(self._fetch_query(client, query, seen_keys))

        return results

    def _build_queries(self) -> list[str]:
        if self.amazon_queries:
            return list(dict.fromkeys(self.amazon_queries))

        if self.use_global_search_queries:
            global_queries = [q.strip() for q in self.config.get("search_queries", []) if q.strip()]
            if global_queries:
                return list(dict.fromkeys(global_queries))

        # Amazon's search endpoint is very strict with long phrase matching.
        # Broad defaults recover substantially more SWE jobs.
        return [
            "software engineer",
            "software development engineer",
            "sde",
        ]

    def _fetch_query(
        self,
        client: httpx.Client,
        query: str,
        seen_keys: set[str],
    ) -> list[JobListing]:
        query_results: list[JobListing] = []
        seen_page_signatures: set[tuple[str, ...]] = set()

        for page in range(self.max_pages_per_query):
            offset = page * self.page_size
            params = {
                "base_query": query,
                "result_limit": str(self.page_size),
                "offset": str(offset),
                "sort": self.sort,
            }
            for key, value in self.query_params.items():
                if value is not None:
                    params[str(key)] = str(value)

            resp = self._request(client, "GET", self.search_url, params=params)
            if resp is None:
                break
            if resp.status_code == 404:
                logger.debug("[amazon] Search endpoint not found")
                break
            if resp.status_code >= 400:
                logger.warning(f"[amazon] Search failed for query='{query}' status={resp.status_code}")
                break

            data = resp.json()
            jobs = data.get("jobs", [])
            if not isinstance(jobs, list) or not jobs:
                break

            signature = tuple(str(job.get("id_icims") or job.get("id") or "") for job in jobs[:3])
            if signature in seen_page_signatures:
                break
            seen_page_signatures.add(signature)

            for job in jobs:
                listing = self._parse_job(job)
                if not listing:
                    continue
                key = listing.unique_key()
                if key in seen_keys:
                    continue
                if self._matches_filters(listing):
                    seen_keys.add(key)
                    query_results.append(listing)

            if len(jobs) < self.page_size:
                break

        return query_results

    def _parse_job(self, job: dict[str, Any]) -> JobListing | None:
        try:
            title = str(job.get("title") or "Unknown")
            company = str(job.get("company_name") or "Amazon")
            job_id = str(job.get("id_icims") or job.get("id") or "")

            location = str(job.get("location") or "")
            if not location:
                location_parts = [job.get("city"), job.get("state"), job.get("country_code")]
                location = ", ".join(str(part) for part in location_parts if part)

            description = html.unescape(str(job.get("description") or ""))
            description = re.sub(r"<[^>]+>", " ", description)
            description = re.sub(r"\s+", " ", description).strip()

            posted_date = None
            posted_date_raw = str(job.get("posted_date") or "").strip()
            if posted_date_raw:
                for fmt in ("%B %d, %Y", "%b %d, %Y"):
                    try:
                        posted_date = datetime.strptime(posted_date_raw, fmt)
                        break
                    except ValueError:
                        continue

            job_path = str(job.get("job_path") or "")
            url = str(job.get("public_url") or "")
            if not url and job_path:
                if job_path.startswith("/"):
                    url = f"{self.job_base_url}{job_path}"
                else:
                    url = f"{self.job_base_url}/{job_path}"
            if not url:
                url = str(job.get("url_next_step") or "")

            return JobListing(
                title=title,
                company=company,
                location=location,
                url=url,
                description=description,
                source="amazon",
                posted_date=posted_date,
                job_id=job_id,
                team=str(job.get("team") or job.get("job_category") or ""),
                metadata={
                    "country_code": job.get("country_code"),
                    "state": job.get("state"),
                    "city": job.get("city"),
                    "job_family": job.get("job_family"),
                    "job_schedule_type": job.get("job_schedule_type"),
                    "source_system": job.get("source_system"),
                    "url_next_step": job.get("url_next_step"),
                },
            )
        except Exception:
            logger.exception("[amazon] Failed to parse job")
            return None
