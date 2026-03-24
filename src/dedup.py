from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger

from .scrapers.base import JobListing

# Keep headroom under SQLite's default variable limit (999).
SQLITE_IN_CLAUSE_CHUNK = 900


class DedupStore:
    def __init__(self, db_path: str = "./data/jobs.db"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self._init_db()

    def _init_db(self) -> None:
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS seen_jobs (
                unique_key TEXT PRIMARY KEY,
                title TEXT,
                company TEXT,
                location TEXT,
                url TEXT,
                source TEXT,
                posted_date TEXT,
                first_seen TEXT NOT NULL,
                score REAL,
                description TEXT
            )
            """
        )
        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_seen_jobs_first_seen
            ON seen_jobs(first_seen)
            """
        )
        self.conn.commit()

    def _existing_keys(self, keys: list[str]) -> set[str]:
        seen: set[str] = set()
        if not keys:
            return seen

        for i in range(0, len(keys), SQLITE_IN_CLAUSE_CHUNK):
            chunk = keys[i : i + SQLITE_IN_CLAUSE_CHUNK]
            placeholders = ",".join("?" for _ in chunk)
            cursor = self.conn.execute(
                f"SELECT unique_key FROM seen_jobs WHERE unique_key IN ({placeholders})",
                chunk,
            )
            seen.update(row[0] for row in cursor.fetchall())
        return seen

    def is_seen(self, listing: JobListing) -> bool:
        cursor = self.conn.execute(
            "SELECT 1 FROM seen_jobs WHERE unique_key = ?",
            (listing.unique_key(),),
        )
        return cursor.fetchone() is not None

    def filter_new(self, listings: list[JobListing]) -> list[JobListing]:
        if not listings:
            return []

        unique_by_key: dict[str, JobListing] = {}
        for listing in listings:
            key = listing.unique_key()
            if key not in unique_by_key:
                unique_by_key[key] = listing

        keys = list(unique_by_key.keys())
        seen_keys = self._existing_keys(keys)
        new_listings = [unique_by_key[key] for key in keys if key not in seen_keys]

        duplicate_count = len(listings) - len(unique_by_key)
        logger.info(
            f"Dedup: {len(listings)} total ({duplicate_count} in-run duplicates) "
            f"-> {len(new_listings)} new"
        )
        return new_listings

    def mark_seen(self, listing: JobListing, score: float | None = None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        posted = listing.posted_date.isoformat() if listing.posted_date else None
        self.conn.execute(
            """INSERT OR IGNORE INTO seen_jobs
               (unique_key, title, company, location, url, source, posted_date, first_seen, score, description)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                listing.unique_key(),
                listing.title,
                listing.company,
                listing.location,
                listing.url,
                listing.source,
                posted,
                now,
                score,
                listing.description[:2000] if listing.description else None,
            ),
        )
        self.conn.commit()

    def mark_batch_seen(self, scored_listings: list[dict[str, Any]]) -> None:
        if not scored_listings:
            return

        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for item in scored_listings:
            listing = item["listing"]
            posted = listing.posted_date.isoformat() if listing.posted_date else None
            rows.append(
                (
                    listing.unique_key(),
                    listing.title,
                    listing.company,
                    listing.location,
                    listing.url,
                    listing.source,
                    posted,
                    now,
                    item.get("total_score"),
                    listing.description[:2000] if listing.description else None,
                )
            )
        self.conn.executemany(
            """INSERT OR IGNORE INTO seen_jobs
               (unique_key, title, company, location, url, source, posted_date, first_seen, score, description)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        self.conn.commit()
        logger.info(f"Marked {len(rows)} listings as seen")

    def get_all_listings(self) -> list[dict[str, Any]]:
        cursor = self.conn.execute(
            "SELECT unique_key, title, company, location, url, source, posted_date, first_seen, score FROM seen_jobs"
        )
        return [
            {
                "unique_key": row[0],
                "title": row[1],
                "company": row[2],
                "location": row[3],
                "url": row[4],
                "source": row[5],
                "posted_date": row[6],
                "first_seen": row[7],
                "score": row[8],
            }
            for row in cursor.fetchall()
        ]

    def get_recent_jobs(self, limit: int = 100) -> list[dict[str, Any]]:
        safe_limit = max(1, min(limit, 500))
        cursor = self.conn.execute(
            """
            SELECT
                unique_key,
                title,
                company,
                location,
                url,
                source,
                posted_date,
                first_seen,
                score,
                description
            FROM seen_jobs
            ORDER BY
                CASE
                    WHEN posted_date IS NOT NULL AND posted_date != '' THEN posted_date
                    ELSE first_seen
                END DESC
            LIMIT ?
            """,
            (safe_limit,),
        )
        return [
            {
                "unique_key": row[0],
                "title": row[1],
                "company": row[2],
                "location": row[3],
                "url": row[4],
                "source": row[5],
                "posted_date": row[6],
                "first_seen": row[7],
                "score": row[8],
                "description": row[9],
            }
            for row in cursor.fetchall()
        ]

    def get_stats(self) -> dict[str, Any]:
        total = self.conn.execute("SELECT COUNT(*) FROM seen_jobs").fetchone()[0]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        today_count = self.conn.execute(
            "SELECT COUNT(*) FROM seen_jobs WHERE first_seen LIKE ?",
            (f"{today}%",),
        ).fetchone()[0]
        company_count = self.conn.execute(
            "SELECT COUNT(DISTINCT company) FROM seen_jobs"
        ).fetchone()[0]
        return {
            "total_seen": total,
            "added_today": today_count,
            "unique_companies": company_count,
        }

    def close(self) -> None:
        self.conn.close()
