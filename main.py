#!/usr/bin/env python3
"""Daily Job Scraper & Email Digest — Entry Point."""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger

from src.config import load_config, load_ats_slugs
from src.dedup import DedupStore
from src.emailer import format_additional_jobs_report, format_email, print_digest, send_email
from src.matcher import filter_listings, rank_listings, score_listing
from src.resume_parser import parse_resume
from src.scrapers import (
    AshbyScraper,
    AmazonScraper,
    GreenhouseScraper,
    LeverScraper,
    MicrosoftScraper,
)

# Configure logging
logger.remove()
logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss} | {level:<7} | {message}")
logger.add("data/scraper.log", rotation="7 days", retention="30 days", level="DEBUG")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily Job Scraper & Email Digest")
    parser.add_argument("--dry-run", action="store_true", help="Print results instead of sending email")
    parser.add_argument("--backfill", action="store_true", help="Skip dedup for this run and re-score all fetched listings")
    parser.add_argument("--discover", metavar="COMPANY", help="Discover ATS slugs for a company")
    parser.add_argument("--validate-slugs", action="store_true", help="Validate all ATS slugs and remove invalid ones")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    return parser.parse_args()


def _source_enabled(config: dict, source: str, default: bool = True) -> bool:
    sources = config.get("sources", {})
    if not isinstance(sources, dict):
        return default
    value = sources.get(source)
    return default if value is None else bool(value)


def _merge_slug_sources(primary: dict[str, str], extras: list[str]) -> dict[str, str]:
    """Merge company->slug map and extra slug list while deduping by slug value."""
    merged: dict[str, str] = {}
    seen_slugs: set[str] = set()

    for company, slug in (primary or {}).items():
        normalized = str(slug).strip().lower()
        if not normalized or normalized in seen_slugs:
            continue
        merged[str(company)] = normalized
        seen_slugs.add(normalized)

    for slug in extras or []:
        normalized = str(slug).strip().lower()
        if not normalized or normalized in seen_slugs:
            continue
        # Use slug as display key for extras without canonical company names.
        merged[normalized] = normalized
        seen_slugs.add(normalized)

    return merged


def _dedupe_in_run(listings: list) -> tuple[list, int]:
    """Remove duplicate listings within the current run using listing unique_key."""
    unique: dict[str, object] = {}
    duplicates = 0
    for listing in listings:
        key = listing.unique_key()
        if key in unique:
            duplicates += 1
            continue
        unique[key] = listing
    return list(unique.values()), duplicates


def build_scrapers(config: dict, ats_slugs: dict) -> list:
    """Instantiate all scraper modules."""
    scrapers = []

    # Lever
    lever_slugs = ats_slugs.get("lever", {}) or {}
    lever_all = _merge_slug_sources(lever_slugs, ats_slugs.get("extra_lever", []) or [])
    if _source_enabled(config, "lever", True) and lever_all:
        scrapers.append(LeverScraper(config, lever_all))

    # Greenhouse
    gh_slugs = ats_slugs.get("greenhouse", {}) or {}
    gh_all = _merge_slug_sources(gh_slugs, ats_slugs.get("extra_greenhouse", []) or [])
    if _source_enabled(config, "greenhouse", True) and gh_all:
        scrapers.append(GreenhouseScraper(config, gh_all))

    # Ashby
    ashby_slugs = ats_slugs.get("ashby", {}) or {}
    ashby_all = _merge_slug_sources(ashby_slugs, ats_slugs.get("extra_ashby", []) or [])
    if _source_enabled(config, "ashby", True) and ashby_all:
        scrapers.append(AshbyScraper(config, ashby_all))

    # Amazon (direct source; no ATS slug required)
    if _source_enabled(config, "amazon", False):
        scrapers.append(AmazonScraper(config))

    # Microsoft (direct source; no ATS slug required)
    if _source_enabled(config, "microsoft", False):
        scrapers.append(MicrosoftScraper(config))

    return scrapers


def run_discovery(company_name: str) -> None:
    """Run slug discovery for a company."""
    from scripts.discover_slugs import discover_slugs

    discover_slugs(company_name)


def _run_scrapers(scrapers: list, max_workers: int) -> tuple[list, Counter[str]]:
    all_listings = []
    source_counts: Counter[str] = Counter()

    if not scrapers:
        return all_listings, source_counts

    workers = max(1, min(max_workers, len(scrapers)))
    logger.info(f"Running {len(scrapers)} scrapers with max_workers={workers}")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(scraper.safe_scrape): scraper for scraper in scrapers}
        for future in as_completed(futures):
            scraper = futures[future]
            try:
                results = future.result()
            except Exception:
                logger.exception(f"[{scraper.name}] Unhandled scraper failure")
                results = []
            all_listings.extend(results)
            source_counts[scraper.name] = len(results)

    return all_listings, source_counts


def run_pipeline(config: dict, ats_slugs: dict, dry_run: bool = False, backfill: bool = False) -> None:
    """Main scraping pipeline."""
    # 1. Parse resume
    logger.info("Parsing resume...")
    resume = parse_resume(
        config.get("resume_path", "./data/resume.pdf"),
        fallback_skills=config.get("skills"),
    )
    logger.info(f"Resume: {len(resume.get('skills', []))} skills, YOE={resume.get('yoe')}")

    # 2. Run scrapers
    scrapers = build_scrapers(config, ats_slugs)
    if not scrapers:
        logger.warning("No scrapers configured/enabled. Check ats_slugs.yaml and config sources.")
        return

    scrape_workers = int(config.get("scrape_workers", 3))
    all_listings, source_counts = _run_scrapers(scrapers, scrape_workers)

    total_scanned = len(all_listings)
    logger.info(f"Total scraped: {total_scanned} listings from {len(scrapers)} sources")

    if not all_listings:
        logger.warning("No listings found from any source")
        return

    # 3. Apply filters
    filtered = filter_listings(all_listings, config)
    filtered, in_run_dupes = _dedupe_in_run(filtered)
    if in_run_dupes:
        logger.info(f"Removed {in_run_dupes} duplicate listings from current scrape batch")

    # 4. Dedup
    dedup = DedupStore(config.get("db_path", "./data/jobs.db"))
    try:
        if backfill:
            logger.info("Backfill mode: skipping DB dedup for this run (in-run dedup still applied)")
            new_listings = filtered
        else:
            new_listings = dedup.filter_new(filtered)

        total_new = len(new_listings)
        logger.info(f"New listings after dedup: {total_new}")

        if not new_listings:
            logger.info("No new listings today")
            return

        # 5. Score and rank
        scored = [score_listing(listing, resume, config) for listing in new_listings]
        max_results = config.get("results_per_day", 10)
        top_results = rank_listings(scored, max_results)

        logger.info(f"Top {len(top_results)} results selected")

        # 6. Mark all new listings as seen (not just top results)
        dedup.mark_batch_seen(scored)

        # 7. Output
        stats = dedup.get_stats()
        if dry_run:
            print_digest(top_results, total_scanned, total_new, db_stats=stats)
        else:
            attachments: list[tuple[str, str, str]] = []
            attachment_cfg = config.get("report_attachment", {})
            if isinstance(attachment_cfg, dict) and attachment_cfg.get("enabled", False):
                max_jobs = int(attachment_cfg.get("max_jobs", 100))
                top_keys = {item["listing"].unique_key() for item in top_results}
                filename, report_html, attached_count = format_additional_jobs_report(
                    scored,
                    excluded_keys=top_keys,
                    max_jobs=max_jobs,
                )
                if attached_count:
                    attachments.append((filename, "text/html", report_html))

            subject, html = format_email(
                top_results,
                total_scanned,
                total_new,
                source_counts=dict(source_counts),
                db_stats=stats,
            )
            success = send_email(subject, html, config, attachments=attachments)
            if success:
                logger.info("Email digest sent successfully")
            else:
                logger.error("Failed to send email digest")
                # Fall back to console output
                print_digest(top_results, total_scanned, total_new, db_stats=stats)

        logger.info(
            f"DB stats: {stats['total_seen']} jobs across {stats['unique_companies']} companies, "
            f"{stats['added_today']} added today"
        )

    finally:
        dedup.close()


def main() -> None:
    args = parse_args()

    if args.discover:
        run_discovery(args.discover)
        return

    if args.validate_slugs:
        from scripts.discover_slugs import validate_all_slugs

        validate_all_slugs()
        return

    config = load_config(args.config)
    ats_slugs = load_ats_slugs()

    run_pipeline(config, ats_slugs, dry_run=args.dry_run, backfill=args.backfill)


if __name__ == "__main__":
    main()
