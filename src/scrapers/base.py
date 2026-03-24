from __future__ import annotations

import random
import re
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import httpx
from loguru import logger

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]

ENGINEERING_KEYWORDS = [
    "engineer",
    "developer",
    "software",
    "backend",
    "frontend",
    "full stack",
    "fullstack",
    "infrastructure",
    "platform",
    "data",
]

SEARCH_STOPWORDS = {
    "and",
    "or",
    "the",
    "for",
    "with",
    "senior",
    "junior",
    "entry",
    "mid",
    "level",
    "role",
}

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


@dataclass
class JobListing:
    title: str
    company: str
    location: str
    url: str
    description: str
    source: str
    posted_date: datetime | None = None
    job_id: str = ""
    seniority: str = ""
    yoe_required: int | None = None
    salary: str = ""
    team: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def unique_key(self) -> str:
        if self.job_id:
            return f"{self.source}:{self.job_id}"
        return f"{self.source}:{self.url}"


class BaseScraper(ABC):
    name: str = "base"

    def __init__(self, config: dict[str, Any]):
        self.config = config
        scrape_cfg = config.get("scrape", {}) if isinstance(config, dict) else {}

        self.request_timeout = float(scrape_cfg.get("request_timeout_seconds", 20.0))
        self.max_retries = max(0, int(scrape_cfg.get("max_retries", 2)))
        self.backoff_base = max(0.1, float(scrape_cfg.get("backoff_base_seconds", 1.0)))
        self.source_workers = max(1, int(scrape_cfg.get("source_workers", 8)))

        # Optional static rate limit. Default off because retries already back off on pressure.
        self.min_delay = max(0.0, float(scrape_cfg.get("min_delay_seconds", 0.0)))
        self.max_delay = max(self.min_delay, float(scrape_cfg.get("max_delay_seconds", self.min_delay)))

    def _get_client(self) -> httpx.Client:
        return httpx.Client(
            headers={"User-Agent": random.choice(USER_AGENTS)},
            timeout=self.request_timeout,
            follow_redirects=True,
        )

    def _rate_limit(self) -> None:
        if self.max_delay <= 0:
            return
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)

    def _backoff_sleep(self, attempt: int) -> None:
        # Exponential backoff with jitter.
        delay = self.backoff_base * (2 ** max(0, attempt - 1)) + random.uniform(0, self.backoff_base)
        time.sleep(delay)

    def _request(
        self,
        client: httpx.Client,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> httpx.Response | None:
        last_exception: Exception | None = None

        for attempt in range(1, self.max_retries + 2):
            try:
                self._rate_limit()
                response = client.request(method, url, **kwargs)
                if response.status_code in RETRYABLE_STATUS_CODES and attempt <= self.max_retries:
                    logger.warning(
                        f"[{self.name}] {response.status_code} from {url}; retrying "
                        f"({attempt}/{self.max_retries})"
                    )
                    self._backoff_sleep(attempt)
                    continue
                return response
            except httpx.RequestError as exc:
                last_exception = exc
                if attempt > self.max_retries:
                    break
                logger.warning(
                    f"[{self.name}] Request error for {url}: {exc}. "
                    f"Retrying ({attempt}/{self.max_retries})"
                )
                self._backoff_sleep(attempt)

        if last_exception is not None:
            logger.error(f"[{self.name}] Request failed for {url}: {last_exception}")
        return None

    def _run_parallel(
        self,
        items: list[Any],
        worker_fn: Callable[[Any], list[JobListing]],
        item_name: str,
    ) -> list[JobListing]:
        if not items:
            return []

        workers = max(1, min(self.source_workers, len(items)))
        if workers == 1:
            results: list[JobListing] = []
            for item in items:
                try:
                    results.extend(worker_fn(item))
                except Exception:
                    logger.exception(f"[{self.name}] Failed to fetch {item_name}: {item}")
            return results

        results: list[JobListing] = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(worker_fn, item): item for item in items}
            for future in as_completed(futures):
                item = futures[future]
                try:
                    results.extend(future.result())
                except Exception:
                    logger.exception(f"[{self.name}] Failed to fetch {item_name}: {item}")

        return results

    def _matches_filters(self, listing: JobListing) -> bool:
        return self._matches_search_intent(listing)

    def _matches_search_intent(self, listing: JobListing) -> bool:
        title_lower = listing.title.lower()
        desc_lower = listing.description.lower()
        text = f"{title_lower} {desc_lower}"

        queries = [q.strip().lower() for q in self.config.get("search_queries", []) if q.strip()]
        if queries:
            # Explicit phrase match from configured user intent.
            if any(q in text for q in queries):
                return True

            terms: set[str] = set()
            for query in queries:
                for token in re.findall(r"[a-z0-9+#.-]+", query):
                    if len(token) >= 3 and token not in SEARCH_STOPWORDS:
                        terms.add(token)

            title_term_hits = sum(1 for term in terms if term in title_lower)
            if title_term_hits >= 2:
                return True

        return any(keyword in title_lower for keyword in ENGINEERING_KEYWORDS)

    @abstractmethod
    def scrape(self) -> list[JobListing]:
        ...

    def safe_scrape(self) -> list[JobListing]:
        try:
            logger.info(f"[{self.name}] Starting scrape")
            results = self.scrape()
            logger.info(f"[{self.name}] Found {len(results)} listings")
            return results
        except Exception:
            logger.exception(f"[{self.name}] Scrape failed")
            return []
