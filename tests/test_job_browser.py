from __future__ import annotations

import io
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from wsgiref.util import setup_testing_defaults

from src.dedup import DedupStore
from src.job_browser import build_job_browser_app
from src.scrapers.base import JobListing


class JobBrowserAppTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmpdir.name) / "jobs.db")
        self.store = DedupStore(self.db_path)
        self.store.mark_seen(
            JobListing(
                title="Platform Engineer",
                company="Acme",
                location="Seattle, WA",
                url="https://example.com/acme-platform",
                description="Python services and infrastructure",
                source="lever",
                job_id="acme-platform",
                posted_date=datetime(2025, 5, 1, tzinfo=timezone.utc),
            ),
            score=90.0,
        )
        self.store.mark_seen(
            JobListing(
                title="Data Engineer",
                company="Globex",
                location="Austin, TX",
                url="https://example.com/globex-data",
                description="Data platform and analytics",
                source="greenhouse",
                job_id="globex-data",
                posted_date=datetime(2025, 4, 2, tzinfo=timezone.utc),
            ),
            score=76.0,
        )
        self.app = build_job_browser_app(self.db_path)

    def tearDown(self) -> None:
        self.store.close()
        self.tmpdir.cleanup()

    def _request(self, path: str) -> tuple[str, dict[str, str], bytes]:
        path_info, _, query_string = path.partition("?")
        environ = {
            "REQUEST_METHOD": "GET",
            "PATH_INFO": path_info,
            "QUERY_STRING": query_string,
            "wsgi.input": io.BytesIO(b""),
            "wsgi.errors": io.StringIO(),
        }
        setup_testing_defaults(environ)
        captured: dict[str, Any] = {"status": "", "headers": {}}

        def start_response(status: str, headers: list[tuple[str, str]]):
            captured["status"] = status
            captured["headers"] = {key: value for key, value in headers}

        body = b"".join(self.app(environ, start_response))
        return captured["status"], captured["headers"], body

    def test_api_endpoint_returns_filtered_jobs(self) -> None:
        status, headers, body = self._request("/api/jobs?company=Acme&sort=score&direction=desc")

        self.assertEqual(status, "200 OK")
        self.assertEqual(headers["Content-Type"], "application/json; charset=utf-8")
        payload = json.loads(body.decode("utf-8"))

        self.assertEqual(payload["result"]["filtered_count"], 1)
        self.assertEqual(payload["result"]["items"][0]["title"], "Platform Engineer")

    def test_html_endpoint_renders_jobs_table(self) -> None:
        status, headers, body = self._request("/jobs?q=Platform")
        html = body.decode("utf-8")

        self.assertEqual(status, "200 OK")
        self.assertEqual(headers["Content-Type"], "text/html; charset=utf-8")
        self.assertIn("Saved Jobs Explorer", html)
        self.assertIn("Platform Engineer", html)
        self.assertIn("<table>", html)


if __name__ == "__main__":
    unittest.main()
