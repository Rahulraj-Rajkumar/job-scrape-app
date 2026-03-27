from __future__ import annotations

import html
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any

import httpx
from bs4 import BeautifulSoup
from loguru import logger

from .base import BaseScraper, JobListing


SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


class MetaScraper(BaseScraper):
    name = "meta"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        meta_cfg = config.get("meta", {}) if isinstance(config, dict) else {}

        self.sitemap_url = str(
            meta_cfg.get("sitemap_url", "https://www.metacareers.com/jobsearch/sitemap.xml")
        )
        self.max_jobs = max(0, int(meta_cfg.get("max_jobs", 0)))
        self.chunk_size = max(1, int(meta_cfg.get("chunk_size", 25)))

    def _get_client(self) -> httpx.Client:
        # Some desktop browser UA strings trigger 400 responses on Meta Careers
        # job detail pages, while a generic Mozilla UA works reliably.
        return httpx.Client(
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=self.request_timeout,
            follow_redirects=True,
        )

    def scrape(self) -> list[JobListing]:
        with self._get_client() as client:
            entries = self._fetch_sitemap_entries(client)

        if not entries:
            return []

        if self.max_jobs:
            entries = entries[: self.max_jobs]

        chunks = [entries[i : i + self.chunk_size] for i in range(0, len(entries), self.chunk_size)]
        return self._run_parallel(chunks, self._fetch_job_chunk, "meta sitemap chunk")

    def _fetch_sitemap_entries(self, client: httpx.Client) -> list[tuple[str, datetime | None]]:
        resp = self._request(client, "GET", self.sitemap_url)
        if resp is None:
            return []
        if resp.status_code >= 400:
            logger.warning(f"[meta] Failed to fetch sitemap: status={resp.status_code}")
            return []

        try:
            root = ET.fromstring(resp.text)
        except ET.ParseError:
            logger.exception("[meta] Failed to parse sitemap XML")
            return []

        entries: list[tuple[str, datetime | None]] = []
        seen_urls: set[str] = set()
        for url_el in root.findall("sm:url", SITEMAP_NS):
            loc_el = url_el.find("sm:loc", SITEMAP_NS)
            if loc_el is None or not loc_el.text:
                continue

            url = loc_el.text.strip()
            if not url or "/profile/job_details/" not in url or url in seen_urls:
                continue

            lastmod_el = url_el.find("sm:lastmod", SITEMAP_NS)
            lastmod = self._parse_iso_datetime(lastmod_el.text if lastmod_el is not None else "")

            seen_urls.add(url)
            entries.append((url, lastmod))

        return entries

    def _fetch_job_chunk(self, entries: list[tuple[str, datetime | None]]) -> list[JobListing]:
        results: list[JobListing] = []
        with self._get_client() as client:
            for url, lastmod in entries:
                listing = self._fetch_job_page(client, url, lastmod)
                if listing and self._matches_filters(listing):
                    results.append(listing)
        return results

    def _fetch_job_page(
        self,
        client: httpx.Client,
        url: str,
        lastmod: datetime | None = None,
    ) -> JobListing | None:
        resp = self._request(client, "GET", url)
        if resp is None:
            return None
        if resp.status_code == 404:
            logger.debug(f"[meta] Job detail page not found: {url}")
            return None
        if resp.status_code >= 400:
            logger.warning(f"[meta] Failed to fetch job detail page: status={resp.status_code} url={url}")
            return None
        return self._parse_job_page(resp.text, url, lastmod)

    def _parse_job_page(
        self,
        html_text: str,
        url: str,
        lastmod: datetime | None = None,
    ) -> JobListing | None:
        soup = BeautifulSoup(html_text, "html.parser")
        job_posting = self._extract_job_posting(soup)
        if not job_posting:
            logger.debug(f"[meta] No JobPosting structured data found for {url}")
            return None

        title = self._clean_text(job_posting.get("title")) or self._meta_content(soup, "title")
        if not title or title.lower() == "meta careers":
            return None

        description_parts: list[str] = []
        description = self._clean_text(job_posting.get("description"))
        if description:
            description_parts.append(description)

        responsibilities = self._clean_text(job_posting.get("responsibilities"))
        if responsibilities:
            description_parts.append(f"Responsibilities: {responsibilities}")

        qualifications = self._clean_text(job_posting.get("qualifications"))
        if qualifications:
            description_parts.append(f"Qualifications: {qualifications}")

        full_description = " ".join(part for part in description_parts if part).strip()
        if not full_description:
            full_description = self._meta_content(soup, 'meta[name="description"]')

        location = self._parse_locations(job_posting.get("jobLocation"))
        posted_date = self._parse_iso_datetime(job_posting.get("datePosted")) or lastmod

        hiring_org = job_posting.get("hiringOrganization", {})
        hiring_org_name = ""
        if isinstance(hiring_org, dict):
            hiring_org_name = self._clean_text(hiring_org.get("name"))

        employment_type = job_posting.get("employmentType")
        if isinstance(employment_type, list):
            employment_type = ", ".join(
                self._clean_text(item) for item in employment_type if self._clean_text(item)
            )
        else:
            employment_type = self._clean_text(employment_type)

        return JobListing(
            title=title,
            company="Meta",
            location=location,
            url=url,
            description=full_description,
            source="meta",
            posted_date=posted_date,
            job_id=self._job_id_from_url(url),
            team=hiring_org_name,
            metadata={
                "valid_through": self._clean_text(job_posting.get("validThrough")),
                "employment_type": employment_type,
                "direct_apply": job_posting.get("directApply"),
                "hiring_organization": hiring_org_name,
            },
        )

    def _extract_job_posting(self, soup: BeautifulSoup) -> dict[str, Any] | None:
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw = script.string or script.get_text()
            if not raw or not raw.strip():
                continue
            try:
                payload = json.loads(raw)
            except ValueError:
                continue

            for candidate in self._iter_structured_data(payload):
                if isinstance(candidate, dict) and candidate.get("@type") == "JobPosting":
                    return candidate

        return None

    def _iter_structured_data(self, payload: Any):
        if isinstance(payload, dict):
            yield payload
            graph = payload.get("@graph")
            if isinstance(graph, list):
                for item in graph:
                    yield from self._iter_structured_data(item)
        elif isinstance(payload, list):
            for item in payload:
                yield from self._iter_structured_data(item)

    def _parse_locations(self, raw_locations: Any) -> str:
        if raw_locations is None:
            return ""

        items = raw_locations if isinstance(raw_locations, list) else [raw_locations]
        locations: list[str] = []
        seen: set[str] = set()
        for item in items:
            if not isinstance(item, dict):
                continue

            name = self._clean_text(item.get("name"))
            if not name:
                address = item.get("address", {})
                if isinstance(address, dict):
                    locality = self._clean_text(address.get("addressLocality"))
                    region = self._clean_text(address.get("addressRegion"))
                    country = self._parse_country(address.get("addressCountry"))
                    parts = [part for part in [locality, region, country] if part]
                    name = ", ".join(parts)

            if name and name not in seen:
                seen.add(name)
                locations.append(name)

        return ", ".join(locations)

    def _parse_country(self, raw_country: Any) -> str:
        if isinstance(raw_country, dict):
            raw_country = raw_country.get("name", "")
        if isinstance(raw_country, list):
            return ", ".join(self._clean_text(item) for item in raw_country if self._clean_text(item))
        return self._clean_text(raw_country)

    def _meta_content(self, soup: BeautifulSoup, selector: str) -> str:
        node = soup.select_one(selector)
        if node is None:
            return ""

        if selector == "title":
            return self._clean_text(node.get_text())
        return self._clean_text(node.get("content", ""))

    def _clean_text(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, list):
            value = " ".join(str(item) for item in value if item)

        text = html.unescape(str(value))
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _parse_iso_datetime(self, value: Any) -> datetime | None:
        text = self._clean_text(value)
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None

    def _job_id_from_url(self, url: str) -> str:
        match = re.search(r"/job_details/(\d+)", url)
        return match.group(1) if match else ""
