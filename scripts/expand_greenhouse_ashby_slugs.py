#!/usr/bin/env python3
"""Expand Greenhouse and Ashby slug lists from public code-search sources.

Approach:
1) Collect candidate slugs from Sourcegraph global code matches.
2) Validate each slug against the official public API endpoint.
3) Merge validated slugs into ats_slugs.yaml extra_* lists.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from collections.abc import Iterable
from pathlib import Path

import httpx
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import load_ats_slugs, save_ats_slugs

SOURCEGRAPH_STREAM_URL = "https://sourcegraph.com/search/stream"

GREENHOUSE_QUERIES = [
    "context:global boards.greenhouse.io/ count:5000",
    "context:global job-boards.greenhouse.io/ count:5000",
    "context:global boards-api.greenhouse.io/v1/boards/ count:5000",
    "context:global boards.greenhouse.io/ archived:yes fork:yes count:5000",
    "context:global job-boards.greenhouse.io/ archived:yes fork:yes count:5000",
    "context:global boards-api.greenhouse.io/v1/boards/ archived:yes fork:yes count:5000",
]

ASHBY_QUERIES = [
    "context:global jobs.ashbyhq.com/ count:5000",
    "context:global api.ashbyhq.com/posting-api/job-board/ count:5000",
    "context:global jobs.ashbyhq.com/ archived:yes fork:yes count:5000",
    "context:global api.ashbyhq.com/posting-api/job-board/ archived:yes fork:yes count:5000",
]

GREENHOUSE_PATTERNS = [
    re.compile(r"boards\.greenhouse\.io/([A-Za-z0-9_-]+)"),
    re.compile(r"job-boards\.greenhouse\.io/([A-Za-z0-9_-]+)"),
    re.compile(r"boards-api\.greenhouse\.io/v1/boards/([A-Za-z0-9_-]+)"),
]

ASHBY_PATTERNS = [
    re.compile(r"jobs\.ashbyhq\.com/([A-Za-z0-9_-]+)"),
    re.compile(r"api\.ashbyhq\.com/posting-api/job-board/([A-Za-z0-9_-]+)"),
]

GREENHOUSE_IGNORED = {
    "embed",
    "job_app",
    "job-board",
    "boards",
    "jobs",
    "api",
    "v1",
}

ASHBY_IGNORED = {
    "jobs",
    "job",
    "job-board",
    "api",
    "posting-api",
}


# Trim punctuation and common query/fragments from captured tokens.
def _normalize_slug(slug: str) -> str:
    cleaned = slug.lower().strip().strip(").,;:\"'[]{}")
    cleaned = cleaned.split("?", 1)[0].split("#", 1)[0]
    return cleaned


def _iter_sourcegraph_matches(query: str, timeout: float) -> Iterable[dict]:
    params = {"q": query, "v": "V2"}
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        with client.stream("GET", SOURCEGRAPH_STREAM_URL, params=params) as response:
            response.raise_for_status()
            current_event = ""
            for raw_line in response.iter_lines():
                if not raw_line:
                    continue
                line = raw_line.strip()
                if line.startswith("event:"):
                    current_event = line.split(":", 1)[1].strip()
                    continue
                if not line.startswith("data:") or current_event != "matches":
                    continue

                payload = line.split(":", 1)[1].strip()
                try:
                    items = json.loads(payload)
                except json.JSONDecodeError:
                    continue

                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict):
                            yield item


def _collect_candidates(
    queries: list[str],
    patterns: list[re.Pattern[str]],
    ignored: set[str],
    timeout: float,
) -> set[str]:
    candidates: set[str] = set()

    for query in queries:
        logger.info(f"Collecting candidates from Sourcegraph query: {query}")
        for item in _iter_sourcegraph_matches(query, timeout=timeout):
            text = " ".join(match.get("line", "") for match in item.get("lineMatches", []))
            if not text:
                continue
            for pattern in patterns:
                for m in pattern.finditer(text):
                    slug = _normalize_slug(m.group(1))
                    if slug and slug not in ignored:
                        candidates.add(slug)

    return candidates


async def _check_greenhouse_slug(
    client: httpx.AsyncClient,
    slug: str,
    min_open_jobs: int,
) -> tuple[str, bool]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    try:
        resp = await client.get(url)
    except httpx.HTTPError:
        return slug, False

    if resp.status_code != 200:
        return slug, False

    try:
        data = resp.json()
    except json.JSONDecodeError:
        return slug, False

    jobs = data.get("jobs", []) if isinstance(data, dict) else []
    return slug, isinstance(jobs, list) and len(jobs) >= min_open_jobs


async def _check_ashby_slug(
    client: httpx.AsyncClient,
    slug: str,
    min_open_jobs: int,
) -> tuple[str, bool]:
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    try:
        resp = await client.get(url)
    except httpx.HTTPError:
        return slug, False

    if resp.status_code != 200:
        return slug, False

    try:
        data = resp.json()
    except json.JSONDecodeError:
        return slug, False

    jobs = data.get("jobs", []) if isinstance(data, dict) else []
    return slug, isinstance(jobs, list) and len(jobs) >= min_open_jobs


async def _validate_slugs(
    candidates: Iterable[str],
    checker,
    min_open_jobs: int,
    concurrency: int,
    timeout: float,
) -> list[str]:
    semaphore = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(
        timeout=timeout,
        headers={"User-Agent": "Mozilla/5.0"},
        follow_redirects=True,
    ) as client:

        async def guarded(slug: str) -> tuple[str, bool]:
            async with semaphore:
                return await checker(client, slug, min_open_jobs)

        results = await asyncio.gather(*(guarded(slug) for slug in candidates))

    return sorted({slug for slug, ok in results if ok})


def _merge_into_extra_key(slugs_path: str, key: str, validated: list[str]) -> tuple[int, int]:
    all_slugs = load_ats_slugs(slugs_path)
    existing = all_slugs.get(key, []) or []
    if not isinstance(existing, list):
        existing = []

    merged = sorted(set(existing) | set(validated))
    added = len(set(merged) - set(existing))

    all_slugs[key] = merged
    save_ats_slugs(all_slugs, slugs_path)
    return len(merged), added


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Expand Greenhouse/Ashby slugs from Sourcegraph")
    parser.add_argument(
        "--source",
        choices=["all", "greenhouse", "ashby"],
        default="all",
        help="Which source to expand",
    )
    parser.add_argument("--slugs-path", default="ats_slugs.yaml", help="Path to ats_slugs.yaml")
    parser.add_argument("--min-open-jobs", type=int, default=1, help="Minimum open jobs required")
    parser.add_argument("--concurrency", type=int, default=40, help="Validation concurrency")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout seconds")
    parser.add_argument("--dry-run", action="store_true", help="Do not write ats_slugs.yaml")
    return parser.parse_args()


def run_greenhouse(args: argparse.Namespace) -> tuple[int, int, int]:
    candidates = _collect_candidates(
        GREENHOUSE_QUERIES,
        GREENHOUSE_PATTERNS,
        GREENHOUSE_IGNORED,
        timeout=args.timeout,
    )
    logger.info(f"Collected {len(candidates)} Greenhouse candidates")

    validated = asyncio.run(
        _validate_slugs(
            candidates,
            _check_greenhouse_slug,
            min_open_jobs=args.min_open_jobs,
            concurrency=args.concurrency,
            timeout=args.timeout,
        )
    )
    logger.info(f"Validated {len(validated)} active Greenhouse slugs")

    if args.dry_run:
        logger.info(f"Greenhouse sample: {validated[:50]}")
        return len(candidates), len(validated), 0

    total, added = _merge_into_extra_key(args.slugs_path, "extra_greenhouse", validated)
    logger.info(f"Updated extra_greenhouse: total={total}, added={added}")
    return len(candidates), len(validated), added


def run_ashby(args: argparse.Namespace) -> tuple[int, int, int]:
    candidates = _collect_candidates(
        ASHBY_QUERIES,
        ASHBY_PATTERNS,
        ASHBY_IGNORED,
        timeout=args.timeout,
    )
    logger.info(f"Collected {len(candidates)} Ashby candidates")

    validated = asyncio.run(
        _validate_slugs(
            candidates,
            _check_ashby_slug,
            min_open_jobs=args.min_open_jobs,
            concurrency=args.concurrency,
            timeout=args.timeout,
        )
    )
    logger.info(f"Validated {len(validated)} active Ashby slugs")

    if args.dry_run:
        logger.info(f"Ashby sample: {validated[:50]}")
        return len(candidates), len(validated), 0

    total, added = _merge_into_extra_key(args.slugs_path, "extra_ashby", validated)
    logger.info(f"Updated extra_ashby: total={total}, added={added}")
    return len(candidates), len(validated), added


def main() -> None:
    args = parse_args()

    if args.source in ("all", "greenhouse"):
        run_greenhouse(args)

    if args.source in ("all", "ashby"):
        run_ashby(args)


if __name__ == "__main__":
    main()
