#!/usr/bin/env python3
"""Utility to discover ATS slugs for a given company name.

Tries common slug patterns across Lever, Greenhouse, and Ashby APIs.
"""
from __future__ import annotations

import re
import sys

import httpx
from loguru import logger

from src.config import load_ats_slugs, save_ats_slugs


def _normalize_slug(company: str) -> list[str]:
    """Generate candidate slugs from a company name."""
    base = company.lower().strip()
    base_clean = re.sub(r"[^a-z0-9]", "", base)
    base_hyphen = re.sub(r"[^a-z0-9]+", "-", base).strip("-")

    candidates = [
        base_clean,
        base_hyphen,
        f"{base_clean}careers",
        f"{base_clean}-careers",
        f"{base_clean}jobs",
        f"{base_clean}-jobs",
        f"{base_hyphen}-careers",
        f"{base_hyphen}-jobs",
    ]
    # Deduplicate while preserving order
    seen = set()
    unique = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            unique.append(c)
    return unique


def _check_lever(client: httpx.Client, slug: str) -> bool:
    try:
        resp = client.get(f"https://api.lever.co/v0/postings/{slug}?mode=json")
        if resp.status_code == 200:
            data = resp.json()
            return isinstance(data, list) and len(data) > 0
    except Exception:
        pass
    return False


def _check_greenhouse(client: httpx.Client, slug: str) -> bool:
    try:
        resp = client.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
        if resp.status_code == 200:
            data = resp.json()
            return len(data.get("jobs", [])) > 0
    except Exception:
        pass
    return False


def _check_ashby(client: httpx.Client, slug: str) -> bool:
    try:
        resp = client.get(f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
        if resp.status_code == 200:
            data = resp.json()
            return len(data.get("jobs", [])) > 0
    except Exception:
        pass
    return False


def discover_slugs(company_name: str, save: bool = True) -> dict[str, str]:
    """Discover ATS slugs for a company. Returns {ats_name: slug}."""
    candidates = _normalize_slug(company_name)
    found: dict[str, str] = {}

    logger.info(f"Discovering ATS slugs for '{company_name}'...")
    logger.info(f"Trying candidates: {candidates}")

    with httpx.Client(timeout=15.0, follow_redirects=True) as client:
        for slug in candidates:
            if "lever" not in found:
                logger.debug(f"  Checking Lever: {slug}")
                if _check_lever(client, slug):
                    logger.info(f"  Found Lever slug: {slug}")
                    found["lever"] = slug

            if "greenhouse" not in found:
                logger.debug(f"  Checking Greenhouse: {slug}")
                if _check_greenhouse(client, slug):
                    logger.info(f"  Found Greenhouse slug: {slug}")
                    found["greenhouse"] = slug

            if "ashby" not in found:
                logger.debug(f"  Checking Ashby: {slug}")
                if _check_ashby(client, slug):
                    logger.info(f"  Found Ashby slug: {slug}")
                    found["ashby"] = slug

            # Stop early if all found
            if len(found) == 3:
                break

    if not found:
        logger.warning(f"No ATS slugs found for '{company_name}'")
    else:
        logger.info(f"Found: {found}")
        if save:
            _save_discovered(company_name, found)

    return found


def _save_discovered(company_name: str, found: dict[str, str]) -> None:
    """Save discovered slugs to ats_slugs.yaml."""
    slugs = load_ats_slugs()

    for ats, slug in found.items():
        if ats not in slugs:
            slugs[ats] = {}
        if isinstance(slugs[ats], dict):
            slugs[ats][company_name] = slug

    save_ats_slugs(slugs)
    logger.info(f"Saved to ats_slugs.yaml")


def validate_all_slugs(remove_invalid: bool = True) -> dict[str, dict[str, str]]:
    """Validate every slug in ats_slugs.yaml. Returns {ats: {company: status}}."""
    slugs = load_ats_slugs()
    checkers = {
        "lever": _check_lever,
        "greenhouse": _check_greenhouse,
        "ashby": _check_ashby,
    }

    report: dict[str, dict[str, str]] = {}
    invalid_count = 0
    valid_count = 0

    with httpx.Client(timeout=15.0, follow_redirects=True) as client:
        for ats_name, checker in checkers.items():
            company_slugs = slugs.get(ats_name, {})
            if not isinstance(company_slugs, dict):
                continue

            report[ats_name] = {}
            to_remove: list[str] = []

            for company, slug in company_slugs.items():
                logger.info(f"  Checking {ats_name}/{company} ({slug})...")
                is_valid = checker(client, slug)

                if is_valid:
                    report[ats_name][company] = f"valid ({slug})"
                    valid_count += 1
                    print(f"  ✓ {ats_name}/{company} ({slug})")
                else:
                    report[ats_name][company] = f"INVALID ({slug})"
                    invalid_count += 1
                    to_remove.append(company)
                    print(f"  ✗ {ats_name}/{company} ({slug})")

            if remove_invalid and to_remove:
                for company in to_remove:
                    del company_slugs[company]
                slugs[ats_name] = company_slugs

            # Also validate extra_* lists
            extra_key = f"extra_{ats_name}"
            extra_slugs = slugs.get(extra_key, [])
            if isinstance(extra_slugs, list):
                valid_extras: list[str] = []
                for slug in extra_slugs:
                    logger.info(f"  Checking {extra_key}/{slug}...")
                    is_valid = checker(client, slug)
                    if is_valid:
                        valid_extras.append(slug)
                        valid_count += 1
                        print(f"  ✓ {extra_key}/{slug}")
                    else:
                        invalid_count += 1
                        print(f"  ✗ {extra_key}/{slug}")
                if remove_invalid:
                    slugs[extra_key] = valid_extras

    print(f"\nResults: {valid_count} valid, {invalid_count} invalid")

    if remove_invalid and invalid_count > 0:
        save_ats_slugs(slugs)
        print(f"Updated ats_slugs.yaml (removed {invalid_count} invalid slugs)")

    return report


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python -m scripts.discover_slugs <company_name>")
        print("  python -m scripts.discover_slugs --validate")
        sys.exit(1)

    if sys.argv[1] == "--validate":
        validate_all_slugs()
    else:
        company = " ".join(sys.argv[1:])
        discover_slugs(company)
