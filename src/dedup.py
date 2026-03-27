from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger

from .scrapers.base import JobListing

# Keep headroom under SQLite's default variable limit (999).
SQLITE_IN_CLAUSE_CHUNK = 900
DEFAULT_JOB_PAGE_SIZE = 50
MAX_JOB_PAGE_SIZE = 200
DEFAULT_JOB_SORT = "posted_date"
DEFAULT_SORT_DIRECTION = "desc"
JOB_SORT_FIELDS = (
    "posted_date",
    "first_seen",
    "score",
    "company",
    "title",
    "location",
    "source",
)


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

    def _normalize_date_filter(self, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        try:
            return date.fromisoformat(text).isoformat()
        except ValueError:
            return ""

    def _normalize_float_filter(self, value: Any) -> float | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    def _normalize_int_filter(self, value: Any, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _build_jobs_where_clause(self, query: dict[str, Any]) -> tuple[list[str], list[Any]]:
        where: list[str] = []
        params: list[Any] = []

        search = str(query.get("q") or "").strip()
        if search:
            like = f"%{search}%"
            where.append(
                """
                (
                    title LIKE ?
                    OR company LIKE ?
                    OR location LIKE ?
                    OR source LIKE ?
                    OR COALESCE(description, '') LIKE ?
                )
                """
            )
            params.extend([like, like, like, like, like])

        company = str(query.get("company") or "").strip()
        if company:
            where.append("company LIKE ?")
            params.append(f"%{company}%")

        location = str(query.get("location") or "").strip()
        if location:
            where.append("location LIKE ?")
            params.append(f"%{location}%")

        source = str(query.get("source") or "").strip()
        if source:
            where.append("source = ?")
            params.append(source)

        posted_from = str(query.get("posted_from") or "").strip()
        if posted_from:
            where.append("posted_date IS NOT NULL AND date(posted_date) >= date(?)")
            params.append(posted_from)

        posted_to = str(query.get("posted_to") or "").strip()
        if posted_to:
            where.append("posted_date IS NOT NULL AND date(posted_date) <= date(?)")
            params.append(posted_to)

        first_seen_from = str(query.get("first_seen_from") or "").strip()
        if first_seen_from:
            where.append("date(first_seen) >= date(?)")
            params.append(first_seen_from)

        first_seen_to = str(query.get("first_seen_to") or "").strip()
        if first_seen_to:
            where.append("date(first_seen) <= date(?)")
            params.append(first_seen_to)

        min_score = query.get("min_score")
        if min_score is not None:
            where.append("score IS NOT NULL AND score >= ?")
            params.append(min_score)

        max_score = query.get("max_score")
        if max_score is not None:
            where.append("score IS NOT NULL AND score <= ?")
            params.append(max_score)

        return where, params

    def _build_jobs_order_by(self, sort_by: str, direction: str) -> str:
        sort_key = sort_by if sort_by in JOB_SORT_FIELDS else DEFAULT_JOB_SORT
        sort_direction = "ASC" if direction == "asc" else "DESC"
        recent_fallback = (
            "CASE WHEN posted_date IS NULL OR TRIM(posted_date) = '' THEN 1 ELSE 0 END ASC, "
            "posted_date DESC, first_seen DESC, LOWER(COALESCE(company, '')) ASC"
        )

        if sort_key == "posted_date":
            return (
                "CASE WHEN posted_date IS NULL OR TRIM(posted_date) = '' THEN 1 ELSE 0 END ASC, "
                f"posted_date {sort_direction}, first_seen {sort_direction}, "
                "LOWER(COALESCE(company, '')) ASC"
            )
        if sort_key == "first_seen":
            return f"first_seen {sort_direction}, {recent_fallback}"
        if sort_key == "score":
            return (
                "CASE WHEN score IS NULL THEN 1 ELSE 0 END ASC, "
                f"score {sort_direction}, {recent_fallback}"
            )

        text_column = {
            "company": "company",
            "title": "title",
            "location": "location",
            "source": "source",
        }[sort_key]
        return f"LOWER(COALESCE({text_column}, '')) {sort_direction}, {recent_fallback}"

    def query_jobs(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        filters = dict(filters or {})
        query = {
            "q": str(filters.get("q") or filters.get("search") or "").strip(),
            "company": str(filters.get("company") or "").strip(),
            "location": str(filters.get("location") or "").strip(),
            "source": str(filters.get("source") or "").strip(),
            "posted_from": self._normalize_date_filter(filters.get("posted_from")),
            "posted_to": self._normalize_date_filter(filters.get("posted_to")),
            "first_seen_from": self._normalize_date_filter(filters.get("first_seen_from")),
            "first_seen_to": self._normalize_date_filter(filters.get("first_seen_to")),
            "min_score": self._normalize_float_filter(filters.get("min_score")),
            "max_score": self._normalize_float_filter(filters.get("max_score")),
            "sort": str(filters.get("sort") or DEFAULT_JOB_SORT).strip(),
            "direction": str(filters.get("direction") or DEFAULT_SORT_DIRECTION).strip().lower(),
            "page": max(1, self._normalize_int_filter(filters.get("page"), 1)),
            "page_size": max(
                1,
                min(
                    MAX_JOB_PAGE_SIZE,
                    self._normalize_int_filter(filters.get("page_size"), DEFAULT_JOB_PAGE_SIZE),
                ),
            ),
        }
        if query["sort"] not in JOB_SORT_FIELDS:
            query["sort"] = DEFAULT_JOB_SORT
        if query["direction"] not in {"asc", "desc"}:
            query["direction"] = DEFAULT_SORT_DIRECTION

        where, params = self._build_jobs_where_clause(query)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""

        total_count = self.conn.execute("SELECT COUNT(*) FROM seen_jobs").fetchone()[0]
        filtered_count = self.conn.execute(
            f"SELECT COUNT(*) FROM seen_jobs {where_sql}",
            params,
        ).fetchone()[0]

        page_count = max(1, (filtered_count + query["page_size"] - 1) // query["page_size"])
        query["page"] = min(query["page"], page_count)
        offset = (query["page"] - 1) * query["page_size"]
        order_by = self._build_jobs_order_by(query["sort"], query["direction"])

        cursor = self.conn.execute(
            f"""
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
            {where_sql}
            ORDER BY {order_by}
            LIMIT ? OFFSET ?
            """,
            [*params, query["page_size"], offset],
        )
        items = [
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

        return {
            "items": items,
            "query": query,
            "total_count": total_count,
            "filtered_count": filtered_count,
            "page_count": page_count,
            "has_previous": query["page"] > 1,
            "has_next": query["page"] < page_count,
        }

    def get_job_filter_options(self) -> dict[str, list[str]]:
        def _distinct_values(column: str, limit: int = 250) -> list[str]:
            cursor = self.conn.execute(
                f"""
                SELECT DISTINCT {column}
                FROM seen_jobs
                WHERE {column} IS NOT NULL AND TRIM({column}) != ''
                ORDER BY LOWER({column}) ASC
                LIMIT ?
                """,
                (limit,),
            )
            return [row[0] for row in cursor.fetchall()]

        return {
            "companies": _distinct_values("company", limit=500),
            "locations": _distinct_values("location", limit=500),
            "sources": _distinct_values("source", limit=50),
        }

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
