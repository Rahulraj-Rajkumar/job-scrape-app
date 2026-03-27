from __future__ import annotations

import base64
import html
import re
import smtplib
import time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import httpx
from jinja2 import Environment, select_autoescape
from loguru import logger


EMAIL_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; margin: 0; padding: 20px; }
  .container { max-width: 700px; margin: 0 auto; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
  .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 24px 30px; }
  .header h1 { margin: 0; font-size: 22px; font-weight: 600; }
  .header p { margin: 8px 0 0; opacity: 0.9; font-size: 14px; }
  .content { padding: 20px 30px; }
  .job-card { border: 1px solid #e2e8f0; border-radius: 8px; padding: 18px; margin-bottom: 16px; transition: border-color 0.2s; }
  .job-card:hover { border-color: #667eea; }
  .job-title { font-size: 17px; font-weight: 600; color: #1a202c; text-decoration: none; }
  .job-title:hover { color: #667eea; }
  .job-meta { display: flex; flex-wrap: wrap; gap: 12px; margin: 8px 0; font-size: 13px; color: #64748b; }
  .job-meta span { display: inline-flex; align-items: center; gap: 4px; }
  .score-badge { display: inline-block; background: #f0fdf4; color: #166534; padding: 2px 10px; border-radius: 12px; font-weight: 600; font-size: 13px; }
  .score-high { background: #f0fdf4; color: #166534; }
  .score-mid { background: #fefce8; color: #854d0e; }
  .score-low { background: #fef2f2; color: #991b1b; }
  .skills-list { margin: 8px 0; }
  .skill-tag { display: inline-block; background: #eef2ff; color: #4338ca; padding: 2px 8px; border-radius: 4px; font-size: 12px; margin: 2px; }
  .description { font-size: 13px; color: #475569; line-height: 1.5; margin-top: 8px; }
  .footer { background: #f8fafc; border-top: 1px solid #e2e8f0; padding: 16px 30px; font-size: 13px; color: #64748b; }
  .footer-stats { display: flex; gap: 20px; flex-wrap: wrap; }
  .stat { font-weight: 600; color: #334155; }
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>Daily Job Digest</h1>
    <p>{{ date }} - {{ count }} New Listings</p>
  </div>
  <div class="content">
    {% for item in listings %}
    <div class="job-card">
      <a class="job-title" href="{{ item.url }}">{{ item.title }}</a>
      <div class="job-meta">
        <span><strong>{{ item.company }}</strong></span>
        <span>&middot;</span>
        <span>{{ item.location }}</span>
        {% if item.posted_label %}
        <span>&middot;</span>
        <span>{{ item.posted_label }}</span>
        {% endif %}
        <span>&middot;</span>
        <span class="score-badge {{ 'score-high' if item.total_score >= 70 else ('score-mid' if item.total_score >= 40 else 'score-low') }}">
          {{ item.total_score }}% match
        </span>
      </div>
      <div class="skills-list">
        {% for skill in item.matching_skills[:8] %}
        <span class="skill-tag">{{ skill }}</span>
        {% endfor %}
      </div>
      {% if item.snippet %}
      <div class="description">{{ item.snippet }}</div>
      {% endif %}
    </div>
    {% endfor %}
  </div>
  <div class="footer">
    <div class="footer-stats">
      <span>Total scanned: <span class="stat">{{ total_scanned }}</span></span>
      <span>New listings: <span class="stat">{{ total_new }}</span></span>
      <span>Source: <span class="stat">{{ source_summary }}</span></span>
    </div>
    <p style="margin-top: 12px; font-size: 12px;">
      Filtered {{ total_scanned - total_new }} previously seen listings.
    </p>
    <p style="margin-top: 8px; font-size: 12px; border-top: 1px solid #e2e8f0; padding-top: 8px;">
      All time: <span class="stat">{{ db_total_seen }}</span> jobs tracked across <span class="stat">{{ db_unique_companies }}</span> companies
    </p>
  </div>
</div>
</body>
</html>
"""

ADDITIONAL_JOBS_REPORT_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f3f4f6; margin: 0; padding: 24px; color: #111827; }
  .wrap { max-width: 980px; margin: 0 auto; }
  .header { background: #0f172a; color: #f8fafc; border-radius: 10px; padding: 18px 22px; }
  .header h1 { margin: 0 0 6px; font-size: 22px; }
  .header p { margin: 0; opacity: 0.9; font-size: 13px; }
  .table-wrap { background: #fff; border: 1px solid #e5e7eb; border-radius: 10px; margin-top: 14px; overflow: hidden; }
  table { width: 100%; border-collapse: collapse; }
  thead th { text-align: left; background: #f8fafc; color: #374151; font-size: 12px; letter-spacing: 0.02em; text-transform: uppercase; border-bottom: 1px solid #e5e7eb; padding: 10px 12px; }
  tbody td { border-bottom: 1px solid #f1f5f9; padding: 11px 12px; vertical-align: top; font-size: 13px; }
  tbody tr:last-child td { border-bottom: none; }
  .title a { color: #1d4ed8; text-decoration: none; font-weight: 600; }
  .title a:hover { text-decoration: underline; }
  .meta { color: #475569; font-size: 12px; margin-top: 2px; }
  .score { font-weight: 600; color: #0f766e; }
  .source { font-size: 12px; color: #334155; }
  .snippet { color: #4b5563; margin-top: 4px; line-height: 1.35; }
  .footer { margin-top: 10px; color: #64748b; font-size: 12px; }
</style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <h1>Additional Jobs by Score</h1>
      <p>{{ generated_at }} - Showing {{ count }} jobs ranked after the top emailed results</p>
    </div>

    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th style="width: 45%;">Role</th>
            <th style="width: 16%;">Company</th>
            <th style="width: 16%;">Location</th>
            <th style="width: 9%;">Source</th>
            <th style="width: 8%;">Score</th>
            <th style="width: 12%;">Posted/Seen</th>
          </tr>
        </thead>
        <tbody>
          {% for row in rows %}
          <tr>
            <td>
              <div class="title"><a href="{{ row.url }}">{{ row.title }}</a></div>
              {% if row.snippet %}
              <div class="snippet">{{ row.snippet }}</div>
              {% endif %}
            </td>
            <td>{{ row.company }}</td>
            <td>{{ row.location }}</td>
            <td class="source">{{ row.source }}</td>
            <td>{% if row.score %}<span class="score">{{ row.score }}</span>{% endif %}</td>
            <td class="meta">{{ row.posted_label }}</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
    <div class="footer">Generated by job-scrape-app</div>
  </div>
</body>
</html>
"""


def _format_iso_timestamp(raw: Any) -> str:
    if not raw:
        return ""
    if isinstance(raw, datetime):
        return raw.strftime("%Y-%m-%d %H:%M")
    text = str(raw).strip()
    if not text:
        return ""
    try:
        # Accept both "...Z" and explicit offsets.
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return text


def _safe_url(url: str) -> str:
    if not url or not url.strip():
        return "#"
    url = url.strip()
    lowered = url.lower()
    if lowered.startswith("http://") or lowered.startswith("https://"):
        return url
    return "#"


def _get_snippet(description: str, max_len: int = 200) -> str:
    if not description:
        return ""

    # Clean any remaining HTML tags/entities
    clean = html.unescape(description)
    clean = re.sub(r"<[^>]+>", " ", clean)
    clean = re.sub(r"\s+", " ", clean).strip()

    # Take first few sentences
    sentences = clean.split(". ")
    snippet = ""
    for sentence in sentences:
        if len(snippet) + len(sentence) > max_len:
            break
        snippet += sentence + ". "
    return snippet.strip() or clean[:max_len] + "..."


def format_email(
    scored_listings: list[dict[str, Any]],
    total_scanned: int,
    total_new: int,
    source_counts: dict[str, int] | None = None,
    db_stats: dict[str, Any] | None = None,
) -> tuple[str, str]:
    """Format the email digest. Returns (subject, html_body)."""
    date_str = datetime.now().strftime("%B %d, %Y")

    render_items: list[dict[str, Any]] = []
    for item in scored_listings:
        listing = item["listing"]
        render_items.append(
            {
                "title": listing.title,
                "company": listing.company,
                "location": listing.location or "Remote",
                "url": _safe_url(listing.url),
                "posted_label": listing.posted_date.strftime("%b %d") if listing.posted_date else "",
                "total_score": item.get("total_score", 0),
                "matching_skills": item.get("matching_skills", []),
                "snippet": _get_snippet(listing.description),
            }
        )

    source_summary = ""
    if source_counts:
        parts = [f"{source}: {count}" for source, count in sorted(source_counts.items())]
        source_summary = ", ".join(parts)

    env = Environment(
        autoescape=select_autoescape(enabled_extensions=("html", "xml"), default_for_string=True)
    )
    template = env.from_string(EMAIL_TEMPLATE)
    html_body = template.render(
        date=date_str,
        count=len(render_items),
        listings=render_items,
        total_scanned=total_scanned,
        total_new=total_new,
        source_summary=source_summary,
        db_total_seen=(db_stats or {}).get("total_seen", 0),
        db_unique_companies=(db_stats or {}).get("unique_companies", 0),
    )

    subject = f"Daily Job Digest - {date_str} - {len(render_items)} New Listings"
    return subject, html_body


def format_additional_jobs_report(
    scored_listings: list[dict[str, Any]],
    excluded_keys: set[str] | None = None,
    max_jobs: int = 100,
) -> tuple[str, str, int]:
    """Build an HTML attachment with next-best scored jobs (excluding top emailed jobs)."""
    excluded = excluded_keys or set()
    sorted_scored = sorted(scored_listings, key=lambda item: item.get("total_score", 0), reverse=True)
    max_items = max(1, max_jobs)

    rows: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for item in sorted_scored:
        listing = item.get("listing")
        if not listing:
            continue

        key = listing.unique_key()
        if key in excluded or key in seen_keys:
            continue
        seen_keys.add(key)

        posted_label = _format_iso_timestamp(listing.posted_date) or "Unknown"
        rows.append(
            {
                "title": listing.title or "Unknown",
                "company": listing.company or "Unknown",
                "location": listing.location or "Remote/Unknown",
                "url": _safe_url(listing.url),
                "source": listing.source or "",
                "score": f"{item['total_score']:.1f}" if isinstance(item.get("total_score"), (int, float)) else "",
                "snippet": _get_snippet(listing.description, max_len=220),
                "posted_label": posted_label,
            }
        )
        if len(rows) >= max_items:
            break

    if not rows:
        return "", "", 0

    env = Environment(
        autoescape=select_autoescape(enabled_extensions=("html", "xml"), default_for_string=True)
    )
    template = env.from_string(ADDITIONAL_JOBS_REPORT_TEMPLATE)
    generated_at = datetime.now().strftime("%B %d, %Y %H:%M")
    html_body = template.render(
        generated_at=generated_at,
        count=len(rows),
        rows=rows,
    )
    filename = f"additional_jobs_by_score_{datetime.now().strftime('%Y%m%d_%H%M')}.html"
    return filename, html_body, len(rows)


def send_email(
    subject: str,
    html_body: str,
    config: dict[str, Any],
    attachments: list[tuple[str, str, str]] | None = None,
) -> bool:
    """Send the email digest via configured provider."""
    email_config = config.get("email", {})
    method = email_config.get("method", "gmail")

    sender = email_config.get("sender", "")
    recipient = email_config.get("recipient", "")
    if not sender or not recipient:
        logger.error("Email sender or recipient not configured")
        return False

    if method == "gmail":
        return _send_gmail(subject, html_body, email_config, sender, recipient, attachments=attachments)
    if method == "sendgrid":
        return _send_sendgrid(subject, html_body, email_config, sender, recipient, attachments=attachments)

    logger.error(f"Unknown email method: {method}")
    return False


def _send_gmail(
    subject: str,
    html_body: str,
    email_config: dict,
    sender: str,
    recipient: str,
    attachments: list[tuple[str, str, str]] | None = None,
) -> bool:
    app_password = email_config.get("app_password", "")
    if not app_password:
        logger.error("Gmail app password not configured")
        return False

    host = email_config.get("smtp_host", "smtp.gmail.com")
    port = int(email_config.get("smtp_port", 587))
    timeout = float(email_config.get("smtp_timeout_seconds", 20))
    retries = max(0, int(email_config.get("max_retries", 2)))

    for attempt in range(1, retries + 2):
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = sender
        msg["To"] = recipient
        msg.attach(MIMEText(html_body, "html"))
        for attachment in attachments or []:
            filename, mime_type, content = attachment
            subtype = "plain"
            if mime_type == "text/html":
                subtype = "html"
            part = MIMEText(content, subtype, "utf-8")
            part.add_header("Content-Disposition", "attachment", filename=filename)
            msg.attach(part)

        try:
            with smtplib.SMTP(host, port, timeout=timeout) as server:
                server.starttls()
                server.login(sender, app_password)
                server.sendmail(sender, recipient, msg.as_string())
            logger.info(f"Email sent to {recipient}")
            return True
        except (smtplib.SMTPException, OSError) as exc:
            if attempt > retries:
                logger.error(f"Failed to send email after {attempt} attempt(s): {exc}")
                return False
            delay = min(10, 2 ** attempt)
            logger.warning(
                f"Gmail send failed (attempt {attempt}/{retries + 1}): {exc}. Retrying in {delay}s"
            )
            time.sleep(delay)

    return False


def _send_sendgrid(
    subject: str,
    html_body: str,
    email_config: dict,
    sender: str,
    recipient: str,
    attachments: list[tuple[str, str, str]] | None = None,
) -> bool:
    api_key = email_config.get("sendgrid_api_key", "")
    if not api_key:
        logger.error("SendGrid API key not configured")
        return False

    payload = {
        "personalizations": [{"to": [{"email": recipient}]}],
        "from": {"email": sender},
        "subject": subject,
        "content": [{"type": "text/html", "value": html_body}],
    }
    if attachments:
        payload["attachments"] = []
        for filename, mime_type, content in attachments:
            payload["attachments"].append(
                {
                    "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
                    "type": mime_type,
                    "filename": filename,
                    "disposition": "attachment",
                }
            )

    retries = max(0, int(email_config.get("max_retries", 2)))
    timeout = float(email_config.get("http_timeout_seconds", 20.0))

    with httpx.Client(timeout=timeout) as client:
        for attempt in range(1, retries + 2):
            try:
                resp = client.post(
                    "https://api.sendgrid.com/v3/mail/send",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )

                if resp.status_code in (200, 202):
                    logger.info(f"Email sent via SendGrid to {recipient}")
                    return True

                if resp.status_code in (408, 429, 500, 502, 503, 504) and attempt <= retries:
                    delay = min(10, 2 ** attempt)
                    logger.warning(
                        f"SendGrid temporary error {resp.status_code} "
                        f"(attempt {attempt}/{retries + 1}); retrying in {delay}s"
                    )
                    time.sleep(delay)
                    continue

                logger.error(f"SendGrid error {resp.status_code}: {resp.text}")
                return False
            except httpx.HTTPError as exc:
                if attempt > retries:
                    logger.error(f"Failed to send via SendGrid after {attempt} attempt(s): {exc}")
                    return False
                delay = min(10, 2 ** attempt)
                logger.warning(
                    f"SendGrid request failed (attempt {attempt}/{retries + 1}): {exc}. "
                    f"Retrying in {delay}s"
                )
                time.sleep(delay)

    return False


def print_digest(
    scored_listings: list[dict[str, Any]],
    total_scanned: int,
    total_new: int,
    db_stats: dict[str, Any] | None = None,
) -> None:
    """Print results to console for --dry-run mode."""
    print(f"\n{'=' * 60}")
    print(f"  Daily Job Digest - {datetime.now().strftime('%B %d, %Y')}")
    print(f"  {len(scored_listings)} Top Listings")
    print(f"{'=' * 60}\n")

    for i, item in enumerate(scored_listings, 1):
        listing = item["listing"]
        skills_str = ", ".join(item["matching_skills"][:6])
        print(f"  {i}. {listing.title}")
        print(f"     {listing.company} | {listing.location or 'Remote'} | {item['total_score']}% match")
        print(f"     Skills: {skills_str}")
        print(f"     URL: {_safe_url(listing.url)}")
        if listing.posted_date:
            print(f"     Posted: {listing.posted_date.strftime('%Y-%m-%d')}")
        print()

    print(f"{'-' * 60}")
    print(f"  Total scanned: {total_scanned}")
    print(f"  New listings: {total_new}")
    if db_stats:
        print(f"  All time: {db_stats['total_seen']} jobs tracked across {db_stats['unique_companies']} companies")
    print(f"{'=' * 60}\n")
