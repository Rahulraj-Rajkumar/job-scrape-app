#!/usr/bin/env python3
"""Expand Lever slug list from public code-search sources and validate with Lever API."""
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
SOURCEGRAPH_QUERIES = [
    "context:global jobs.lever.co/ count:5000",
    "context:global jobs.lever.co/ archived:yes fork:yes count:5000",
]

SLUG_PATTERNS = [
    re.compile(r"jobs\.lever\.co/([A-Za-z0-9_-]+)"),
    re.compile(r"api\.lever\.co/v0/postings/([A-Za-z0-9_-]+)"),
]

IGNORED_SLUGS = {"find", "jobs", "yourcompany"}


def _normalize_slug(slug: str) -> str:
    return slug.lower().strip().strip(").,;:\"'[]{}")


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


def collect_sourcegraph_candidates(timeout: float = 30.0) -> set[str]:
    candidates: set[str] = set()

    for query in SOURCEGRAPH_QUERIES:
        logger.info(f"Collecting candidates from Sourcegraph query: {query}")
        for item in _iter_sourcegraph_matches(query, timeout=timeout):
            text = " ".join(match.get("line", "") for match in item.get("lineMatches", []))
            if not text:
                continue

            for pattern in SLUG_PATTERNS:
                for match in pattern.finditer(text):
                    slug = _normalize_slug(match.group(1))
                    if slug and slug not in IGNORED_SLUGS:
                        candidates.add(slug)

    logger.info(f"Collected {len(candidates)} Lever slug candidates")
    return candidates


async def _check_slug(client: httpx.AsyncClient, slug: str, min_open_jobs: int) -> tuple[str, bool]:
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
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

    if not isinstance(data, list):
        return slug, False

    return slug, len(data) >= min_open_jobs


async def validate_lever_slugs(
    candidates: Iterable[str],
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
                return await _check_slug(client, slug, min_open_jobs)

        results = await asyncio.gather(*(guarded(slug) for slug in candidates))

    valid = sorted(slug for slug, ok in results if ok)
    logger.info(f"Validated {len(valid)} active Lever slugs")
    return valid


def merge_slugs_into_ats(valid_slugs: list[str], slugs_path: str) -> tuple[int, int]:
    slugs = load_ats_slugs(slugs_path)
    existing_extra = slugs.get("extra_lever", []) or []
    if not isinstance(existing_extra, list):
        existing_extra = []

    merged = sorted(set(existing_extra) | set(valid_slugs))
    added = len(set(merged) - set(existing_extra))

    slugs["extra_lever"] = merged
    save_ats_slugs(slugs, slugs_path)
    return len(merged), added


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Expand Lever slugs from public code search")
    parser.add_argument("--slugs-path", default="ats_slugs.yaml", help="Path to ats_slugs.yaml")
    parser.add_argument("--min-open-jobs", type=int, default=1, help="Minimum open jobs for a slug to be kept")
    parser.add_argument("--concurrency", type=int, default=40, help="Validation concurrency")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout seconds")
    parser.add_argument("--dry-run", action="store_true", help="Do not write ats_slugs.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    candidates = collect_sourcegraph_candidates(timeout=args.timeout)
    if not candidates:
        logger.warning("No candidates collected")
        return

    valid = asyncio.run(
        validate_lever_slugs(
            candidates,
            min_open_jobs=args.min_open_jobs,
            concurrency=args.concurrency,
            timeout=args.timeout,
        )
    )

    if args.dry_run:
        logger.info(f"Dry run complete. {len(valid)} valid slugs")
        logger.info(f"Sample: {valid[:50]}")
        return

    total, added = merge_slugs_into_ats(valid, args.slugs_path)
    logger.info(f"Updated {args.slugs_path}: total extra_lever={total}, newly added={added}")


if __name__ == "__main__":
    main()
