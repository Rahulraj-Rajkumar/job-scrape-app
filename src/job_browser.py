from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode
from wsgiref.simple_server import make_server

from jinja2 import Environment, FileSystemLoader, select_autoescape
from loguru import logger

from .dedup import DEFAULT_JOB_PAGE_SIZE, DEFAULT_JOB_SORT, DedupStore, JOB_SORT_FIELDS

DEFAULT_JOB_BROWSER_HOST = "127.0.0.1"
DEFAULT_JOB_BROWSER_PORT = 8000

SORT_OPTIONS = [
    ("posted_date", "Posted Date"),
    ("first_seen", "Saved Date"),
    ("score", "Score"),
    ("company", "Company"),
    ("title", "Job Title"),
    ("location", "Location"),
    ("source", "Source"),
]
PAGE_SIZE_OPTIONS = (25, 50, 100, 200)

_TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"
_TEMPLATES = Environment(
    loader=FileSystemLoader(str(_TEMPLATE_DIR)),
    autoescape=select_autoescape(["html", "xml"]),
)


def _clean_query_string(query: dict[str, Any], **updates: Any) -> str:
    merged: dict[str, str] = {}
    for key, value in {**query, **updates}.items():
        if value is None:
            continue
        text = str(value).strip()
        if text:
            merged[key] = text
    return urlencode(merged)


def _format_date_label(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return "Unknown"
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.time().isoformat() == "00:00:00":
            return parsed.date().isoformat()
        return parsed.strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return text[:19]


def _format_score(value: Any) -> str:
    return "--" if value is None else f"{float(value):.1f}"


def _compact_text(value: str | None, limit: int = 240) -> str:
    text = " ".join(str(value or "").split())
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3].rstrip()}..."


def _display_range(result: dict[str, Any]) -> str:
    filtered = int(result["filtered_count"])
    if filtered == 0:
        return "No matching jobs"
    query = result["query"]
    start = ((query["page"] - 1) * query["page_size"]) + 1
    end = start + len(result["items"]) - 1
    return f"Showing {start}-{end} of {filtered}"


def _prepare_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for item in items:
        score = item.get("score")
        score_value = float(score) if score is not None else None
        prepared.append(
            {
                **item,
                "title_label": item.get("title") or "Untitled role",
                "company_label": item.get("company") or "Unknown company",
                "location_label": item.get("location") or "Unknown location",
                "source_label": (item.get("source") or "unknown").title(),
                "posted_label": _format_date_label(item.get("posted_date")),
                "first_seen_label": _format_date_label(item.get("first_seen")),
                "score_label": _format_score(score),
                "score_band": (
                    "high"
                    if score_value is not None and score_value >= 85
                    else "medium"
                    if score_value is not None and score_value >= 65
                    else "low"
                ),
                "description_preview": _compact_text(item.get("description")),
            }
        )
    return prepared


def _render_html(
    db_path: str,
    result: dict[str, Any],
    filter_options: dict[str, list[str]],
    stats: dict[str, Any],
) -> str:
    query = dict(result["query"])
    query["min_score"] = "" if query["min_score"] is None else query["min_score"]
    query["max_score"] = "" if query["max_score"] is None else query["max_score"]
    items = _prepare_items(result["items"])
    api_query = _clean_query_string(query)
    api_url = "/api/jobs" if not api_query else f"/api/jobs?{api_query}"
    previous_url = None
    next_url = None
    if result["has_previous"]:
        previous_query = _clean_query_string(query, page=max(1, query["page"] - 1))
        previous_url = f"/jobs?{previous_query}"
    if result["has_next"]:
        next_query = _clean_query_string(query, page=query["page"] + 1)
        next_url = f"/jobs?{next_query}"

    template = _TEMPLATES.get_template("jobs_browser.html")
    return template.render(
        db_path=db_path,
        stats=stats,
        result=result,
        items=items,
        query=query,
        sort_options=SORT_OPTIONS,
        page_size_options=PAGE_SIZE_OPTIONS,
        source_options=filter_options.get("sources", []),
        company_options=filter_options.get("companies", []),
        location_options=filter_options.get("locations", []),
        display_range=_display_range(result),
        api_url=api_url,
        previous_url=previous_url,
        next_url=next_url,
    )


def _json_payload(
    db_path: str,
    result: dict[str, Any],
    filter_options: dict[str, list[str]],
    stats: dict[str, Any],
) -> dict[str, Any]:
    query = dict(result["query"])
    query["min_score"] = "" if query["min_score"] is None else query["min_score"]
    query["max_score"] = "" if query["max_score"] is None else query["max_score"]
    return {
        "db_path": db_path,
        "stats": stats,
        "filters": filter_options,
        "result": {
            **result,
            "items": _prepare_items(result["items"]),
            "query": query,
        },
    }


def build_job_browser_app(db_path: str):
    def app(environ: dict[str, Any], start_response):
        path = str(environ.get("PATH_INFO") or "/")
        method = str(environ.get("REQUEST_METHOD") or "GET").upper()
        if method != "GET":
            body = b"Method not allowed"
            start_response(
                "405 Method Not Allowed",
                [
                    ("Content-Type", "text/plain; charset=utf-8"),
                    ("Content-Length", str(len(body))),
                ],
            )
            return [body]

        if path == "/":
            start_response("302 Found", [("Location", "/jobs")])
            return [b""]

        if path not in {"/jobs", "/api/jobs"}:
            body = b"Not found"
            start_response(
                "404 Not Found",
                [
                    ("Content-Type", "text/plain; charset=utf-8"),
                    ("Content-Length", str(len(body))),
                ],
            )
            return [body]

        raw_query = parse_qs(str(environ.get("QUERY_STRING") or ""), keep_blank_values=False)
        query = {key: values[-1] for key, values in raw_query.items() if values}

        store = DedupStore(db_path)
        try:
            result = store.query_jobs(query)
            filter_options = store.get_job_filter_options()
            stats = store.get_stats()
        finally:
            store.close()

        if path == "/api/jobs":
            body = json.dumps(
                _json_payload(db_path, result, filter_options, stats),
                ensure_ascii=False,
                indent=2,
            ).encode("utf-8")
            start_response(
                "200 OK",
                [
                    ("Content-Type", "application/json; charset=utf-8"),
                    ("Content-Length", str(len(body))),
                ],
            )
            return [body]

        body = _render_html(db_path, result, filter_options, stats).encode("utf-8")
        start_response(
            "200 OK",
            [
                ("Content-Type", "text/html; charset=utf-8"),
                ("Content-Length", str(len(body))),
            ],
        )
        return [body]

    return app


def serve_job_browser(
    db_path: str,
    host: str = DEFAULT_JOB_BROWSER_HOST,
    port: int = DEFAULT_JOB_BROWSER_PORT,
) -> None:
    if port <= 0 or port > 65535:
        raise ValueError("Port must be between 1 and 65535")

    app = build_job_browser_app(db_path)
    url = f"http://{host}:{port}/jobs"
    logger.info(f"Serving saved jobs browser at {url}")
    logger.info(
        "Available sort keys: "
        + ", ".join(sort_key for sort_key in JOB_SORT_FIELDS if sort_key != DEFAULT_JOB_SORT)
    )
    logger.info(f"Default page size: {DEFAULT_JOB_PAGE_SIZE}")
    with make_server(host, port, app) as server:
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            logger.info("Stopping jobs browser")
