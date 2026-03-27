from __future__ import annotations

import html
import json
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

import httpx
from loguru import logger

from .base import BaseScraper, JobListing


class MicrosoftScraper(BaseScraper):
    name = "microsoft"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        microsoft_cfg = config.get("microsoft", {}) if isinstance(config, dict) else {}

        self.domain = str(microsoft_cfg.get("domain", "microsoft.com"))
        self.language = str(microsoft_cfg.get("language", "en"))
        self.max_pages_per_query = max(1, int(microsoft_cfg.get("max_pages_per_query", 4)))
        self.page_size = max(1, int(microsoft_cfg.get("page_size", 10)))
        self.start_step = max(1, int(microsoft_cfg.get("start_step", 10)))
        self.sort_by = str(microsoft_cfg.get("sort_by", "")).strip()
        self.use_global_search_queries = bool(microsoft_cfg.get("use_global_search_queries", False))
        self.microsoft_queries = [str(q).strip() for q in microsoft_cfg.get("queries", []) if str(q).strip()]
        self.fetch_position_details = bool(microsoft_cfg.get("fetch_position_details", True))

        self.api_base_url = str(microsoft_cfg.get("api_base_url", "https://apply.careers.microsoft.com")).rstrip("/")
        self.search_url = f"{self.api_base_url}/api/pcsx/search"
        self.position_details_url = f"{self.api_base_url}/api/pcsx/position_details"
        self.warmup_url = str(
            microsoft_cfg.get("warmup_url", f"{self.api_base_url}/careers?hl={self.language}")
        )

        self.use_playwright_fallback = bool(microsoft_cfg.get("use_playwright_fallback", True))
        self.playwright_timeout_seconds = float(microsoft_cfg.get("playwright_timeout_seconds", 30))

    def scrape(self) -> list[JobListing]:
        queries = self._build_queries()

        results: list[JobListing] = []
        seen_keys: set[str] = set()
        had_api_error = False

        with self._get_client() as client:
            client.headers.update(
                {
                    "Accept": "application/json, text/plain, */*",
                    "Origin": self.api_base_url,
                    "Referer": self.warmup_url,
                }
            )
            self._warm_up_session(client)

            for query in queries:
                query_results, had_error = self._fetch_query(client, query, seen_keys)
                results.extend(query_results)
                had_api_error = had_api_error or had_error

        if results or not (had_api_error and self.use_playwright_fallback):
            return results

        fallback_results = self._scrape_with_playwright(queries, seen_keys)
        if fallback_results:
            logger.info(f"[microsoft] Playwright fallback recovered {len(fallback_results)} listings")
        return fallback_results

    def _build_queries(self) -> list[str]:
        if self.microsoft_queries:
            return list(dict.fromkeys(self.microsoft_queries))

        if self.use_global_search_queries:
            global_queries = [q.strip() for q in self.config.get("search_queries", []) if q.strip()]
            if global_queries:
                return list(dict.fromkeys(global_queries))

        # Broader defaults increase coverage; global query phrases can be too restrictive.
        return [
            "software engineer",
            "software development engineer",
            "backend engineer",
            "full stack engineer",
            "sde",
        ]

    def _warm_up_session(self, client: httpx.Client) -> None:
        resp = self._request(client, "GET", self.warmup_url)
        if resp is None:
            logger.debug("[microsoft] Warm-up request failed")
            return
        if resp.status_code >= 400:
            logger.debug(f"[microsoft] Warm-up status={resp.status_code}")

    def _fetch_query(
        self,
        client: httpx.Client,
        query: str,
        seen_keys: set[str],
    ) -> tuple[list[JobListing], bool]:
        query_results: list[JobListing] = []
        had_error = False
        seen_page_signatures: set[tuple[str, ...]] = set()

        for page in range(self.max_pages_per_query):
            start = page * self.start_step
            params = {
                "domain": self.domain,
                "query": query,
                "location": "",
                "start": str(start),
            }
            if self.sort_by:
                params["sort_by"] = self.sort_by

            resp = self._request(client, "GET", self.search_url, params=params)
            if resp is None:
                had_error = True
                break
            if resp.status_code >= 400:
                had_error = True
                logger.warning(
                    f"[microsoft] Search failed for query='{query}' start={start} status={resp.status_code}"
                )
                break

            try:
                payload = resp.json()
            except ValueError:
                had_error = True
                logger.warning(f"[microsoft] Non-JSON search response for query='{query}' start={start}")
                break

            data = payload.get("data", {}) if isinstance(payload, dict) else {}
            positions = data.get("positions", []) if isinstance(data, dict) else []
            if not isinstance(positions, list) or not positions:
                break

            signature = tuple(str(position.get("id") or position.get("atsJobId") or "") for position in positions[:3])
            if signature in seen_page_signatures:
                break
            seen_page_signatures.add(signature)

            for position in positions:
                summary = self._parse_position_summary(position)
                if not summary:
                    continue
                if summary.unique_key() in seen_keys:
                    continue

                # Early check on title/summary text so we do not call details for obviously irrelevant roles.
                if not self._matches_search_intent(summary):
                    continue

                listing = summary
                position_id = str(position.get("id") or "").strip()
                if self.fetch_position_details and position_id:
                    details = self._fetch_position_details(client, position_id, summary.location)
                    if details:
                        listing = self._merge_position_details(summary, details)

                if self._matches_filters(listing):
                    seen_keys.add(listing.unique_key())
                    query_results.append(listing)

            if len(positions) < self.page_size:
                break

        return query_results, had_error

    def _fetch_position_details(
        self,
        client: httpx.Client,
        position_id: str,
        queried_location: str = "",
    ) -> dict[str, Any] | None:
        params = {
            "position_id": position_id,
            "domain": self.domain,
            "hl": self.language,
        }
        if queried_location:
            params["queried_location"] = queried_location

        resp = self._request(client, "GET", self.position_details_url, params=params)
        if resp is None:
            return None
        if resp.status_code >= 400:
            logger.debug(f"[microsoft] Position details failed for {position_id}: {resp.status_code}")
            return None

        try:
            payload = resp.json()
        except ValueError:
            return None

        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        return data if isinstance(data, dict) else None

    def _parse_position_summary(self, position: dict[str, Any]) -> JobListing | None:
        try:
            title = str(position.get("name") or "Unknown")
            position_id = str(position.get("id") or position.get("atsJobId") or "")
            location = self._normalize_location(position.get("location"), position.get("locations"))
            url = self._normalize_url(position.get("publicUrl"), position.get("positionUrl"))
            posted_date = self._parse_epoch(position.get("postedTs"))

            description_parts = [
                str(position.get("department") or ""),
                str(position.get("workLocationOption") or ""),
                str(position.get("locationFlexibility") or ""),
            ]
            description = " ".join(part for part in description_parts if part).strip()

            metadata = {
                "domain": self.domain,
                "display_job_id": position.get("displayJobId"),
                "ats_job_id": position.get("atsJobId"),
                "standardized_locations": position.get("standardizedLocations"),
                "stars": position.get("stars"),
                "is_hot": position.get("isHot"),
                "solr_score": position.get("solrScore"),
                "work_location_option": position.get("workLocationOption"),
                "location_flexibility": position.get("locationFlexibility"),
            }

            return JobListing(
                title=title,
                company="Microsoft",
                location=location,
                url=url,
                description=description,
                source="microsoft",
                posted_date=posted_date,
                job_id=position_id,
                team=str(position.get("department") or ""),
                metadata=metadata,
            )
        except Exception:
            logger.exception("[microsoft] Failed to parse search position")
            return None

    def _merge_position_details(self, summary: JobListing, details: dict[str, Any]) -> JobListing:
        description_html = str(details.get("jobDescription") or summary.description or "")
        description = self._clean_text(description_html)

        location = self._normalize_location(details.get("location"), details.get("locations")) or summary.location
        url = self._normalize_url(details.get("publicUrl"), details.get("positionUrl")) or summary.url
        posted_date = self._parse_epoch(details.get("postedTs")) or summary.posted_date
        title = str(details.get("name") or summary.title)
        team = str(details.get("department") or summary.team)

        metadata = dict(summary.metadata)
        metadata.update(
            {
                "display_job_id": details.get("displayJobId", metadata.get("display_job_id")),
                "ats_job_id": details.get("atsJobId", metadata.get("ats_job_id")),
                "work_location_option": details.get("workLocationOption", metadata.get("work_location_option")),
                "location_flexibility": details.get("locationFlexibility", metadata.get("location_flexibility")),
                "efcustom_text_work_site": details.get("efcustomTextWorkSite"),
                "efcustom_text_required_travel": details.get("efcustomTextRequiredTravel"),
                "efcustom_text_current_profession": details.get("efcustomTextCurrentProfession"),
                "efcustom_text_ta_discipline_name": details.get("efcustomTextTaDisciplineName"),
                "efcustom_text_roletype": details.get("efcustomTextRoletype"),
                "efcustom_text_employment_type": details.get("efcustomTextEmploymentType"),
            }
        )

        return JobListing(
            title=title,
            company=summary.company,
            location=location,
            url=url,
            description=description,
            source=summary.source,
            posted_date=posted_date,
            job_id=summary.job_id,
            team=team,
            metadata=metadata,
        )

    def _normalize_location(self, location: Any, locations: Any) -> str:
        if isinstance(location, str) and location.strip():
            return location.strip()
        if isinstance(locations, list):
            return ", ".join(str(item).strip() for item in locations if str(item).strip())
        return ""

    def _normalize_url(self, public_url: Any, position_url: Any) -> str:
        if isinstance(public_url, str) and public_url.strip():
            return public_url.strip()
        if isinstance(position_url, str) and position_url.strip():
            if position_url.startswith("http://") or position_url.startswith("https://"):
                return position_url.strip()
            return f"{self.api_base_url}{position_url}"
        return ""

    def _parse_epoch(self, epoch_value: Any) -> datetime | None:
        try:
            if epoch_value is None:
                return None
            return datetime.fromtimestamp(int(epoch_value), tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            return None

    def _clean_text(self, text: str) -> str:
        clean = html.unescape(text)
        clean = re.sub(r"<[^>]+>", " ", clean)
        clean = re.sub(r"\s+", " ", clean).strip()
        return clean

    def _scrape_with_playwright(
        self,
        queries: list[str],
        seen_keys: set[str],
    ) -> list[JobListing]:
        try:
            from playwright.sync_api import sync_playwright
        except Exception:
            logger.warning("[microsoft] Playwright fallback unavailable (install playwright + browsers)")
            return []

        results: list[JobListing] = []

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(
                    self.warmup_url,
                    wait_until="domcontentloaded",
                    timeout=int(self.playwright_timeout_seconds * 1000),
                )

                for query in queries:
                    seen_page_signatures: set[tuple[str, ...]] = set()
                    for page_index in range(self.max_pages_per_query):
                        start = page_index * self.start_step
                        search_payload = self._browser_fetch_json(
                            page,
                            self.search_url,
                            {
                                "domain": self.domain,
                                "query": query,
                                "location": "",
                                "start": str(start),
                                **({"sort_by": self.sort_by} if self.sort_by else {}),
                            },
                        )
                        if not search_payload:
                            break

                        positions = (
                            search_payload.get("data", {}).get("positions", [])
                            if isinstance(search_payload, dict)
                            else []
                        )
                        if not isinstance(positions, list) or not positions:
                            break

                        signature = tuple(
                            str(position.get("id") or position.get("atsJobId") or "") for position in positions[:3]
                        )
                        if signature in seen_page_signatures:
                            break
                        seen_page_signatures.add(signature)

                        for position in positions:
                            summary = self._parse_position_summary(position)
                            if not summary:
                                continue
                            if summary.unique_key() in seen_keys:
                                continue
                            if not self._matches_search_intent(summary):
                                continue

                            listing = summary
                            position_id = str(position.get("id") or "").strip()
                            if self.fetch_position_details and position_id:
                                details_payload = self._browser_fetch_json(
                                    page,
                                    self.position_details_url,
                                    {
                                        "position_id": position_id,
                                        "domain": self.domain,
                                        "hl": self.language,
                                    },
                                )
                                details = details_payload.get("data", {}) if details_payload else {}
                                if isinstance(details, dict) and details:
                                    listing = self._merge_position_details(summary, details)

                            if self._matches_filters(listing):
                                seen_keys.add(listing.unique_key())
                                results.append(listing)

                        if len(positions) < self.page_size:
                            break

                browser.close()
        except Exception:
            logger.exception("[microsoft] Playwright fallback failed")
            return []

        return results

    def _browser_fetch_json(
        self,
        page: Any,
        endpoint: str,
        params: dict[str, str],
    ) -> dict[str, Any] | None:
        query = urlencode(params)
        url = endpoint if not query else f"{endpoint}?{query}"
        payload = page.evaluate(
            """
            async ({ url }) => {
                const response = await fetch(url, { credentials: "include" });
                const text = await response.text();
                return { status: response.status, text };
            }
            """,
            {"url": url},
        )
        if not isinstance(payload, dict):
            return None
        status = int(payload.get("status", 0))
        if status >= 400:
            return None
        text = str(payload.get("text", "")).strip()
        if not text:
            return None
        try:
            data = json.loads(text)
            return data if isinstance(data, dict) else None
        except json.JSONDecodeError:
            return None
