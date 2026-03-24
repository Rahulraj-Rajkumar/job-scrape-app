# Daily Job Scraper & Email Digest

Scrapes jobs from Lever, Greenhouse, and Ashby, filters/ranks them against your resume, and sends a daily email digest.

## What This Does

- Scrapes public job boards from multiple ATS providers
- Filters and scores jobs using your resume + preferences in `config.yaml`
- Deduplicates listings using SQLite (`data/jobs.db`)
- Sends top results by email
- Optionally attaches an HTML report of additional high-scoring jobs

## Quickstart (Fresh Clone)

### 1. Clone and enter the project

```bash
git clone <your-repo-url>
cd job-scrape-app
```

### 2. Create a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Add your resume

Place your resume at:

```text
data/resume.pdf
```

If you want a different path, update `resume_path` in `config.yaml`.

### 4. Configure search preferences

Open `config.yaml` and edit:

- `search_queries`
- `target_companies`
- `preferred_locations`
- `max_yoe_required`
- `country`

### 5. Configure email credentials (optional but recommended)

Copy env template:

```bash
cp .env.example .env
```

Then fill `.env`:

```dotenv
EMAIL_SENDER=you@gmail.com
EMAIL_RECIPIENT=you@gmail.com
EMAIL_APP_PASSWORD=your_gmail_app_password
# or if using SendGrid:
SENDGRID_API_KEY=your_sendgrid_key
```

In `config.yaml` set:

- `email.method: "gmail"` for Gmail SMTP
- or `email.method: "sendgrid"` for SendGrid API

## Run Commands

### Dry run (no email sent)

```bash
./venv/bin/python main.py --dry-run
```

### Normal run (sends email)

```bash
./venv/bin/python main.py
```

### Backfill run (re-scores fetched jobs, skips DB dedup for this run)

```bash
./venv/bin/python main.py --backfill --dry-run
```

### Validate ATS slugs

```bash
./venv/bin/python main.py --validate-slugs
```

### Discover company slug candidates

```bash
./venv/bin/python main.py --discover "Stripe"
```

## Optional: HTML Attachment for Additional Jobs

Configure in `config.yaml`:

```yaml
report_attachment:
  enabled: true
  max_jobs: 100
```

When enabled, each non-dry-run email includes an HTML attachment with the next `max_jobs` jobs by score, excluding jobs already shown in the top email list.

## Scheduler (Cron)

Run every weekday at 8:00 AM:

```bash
# crontab -e
0 8 * * 1-5 cd /path/to/job-scrape-app && /path/to/job-scrape-app/venv/bin/python main.py >> data/cron.log 2>&1
```

## Public Repo Safety Checklist

These are intentionally ignored by git:

- `.env`
- all generated files under `data/` (DB/logs/parsed resume/attachments)
- local `venv/`

Before pushing, run:

```bash
git status --ignored
```

If any sensitive files were tracked before `.gitignore`, untrack them:

```bash
git rm --cached .env data/jobs.db data/*.log data/*.parsed.json data/*.pdf 2>/dev/null || true
git add .gitignore
git commit -m "Stop tracking local secrets and runtime data"
```

If secrets were ever committed in earlier commits, rotate credentials (email app password, API keys). If needed, rewrite history before publishing.

## Project Layout

```text
.
├── main.py
├── config.yaml
├── ats_slugs.yaml
├── .env.example
├── requirements.txt
├── src/
├── scripts/
├── tests/
└── data/               # local runtime data only (gitignored)
```
