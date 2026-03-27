from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path

import pdfplumber
from loguru import logger


PARSER_CACHE_VERSION = 2


COMMON_TECH_SKILLS = {
    # Languages
    "python", "java", "javascript", "typescript", "go", "golang", "rust", "c++", "c#",
    "ruby", "php", "swift", "kotlin", "scala", "r", "matlab", "perl", "shell", "bash",
    # Frontend
    "react", "angular", "vue", "svelte", "next.js", "nextjs", "nuxt", "gatsby",
    "html", "css", "sass", "tailwind", "webpack", "vite",
    # Backend
    "node.js", "nodejs", "express", "fastapi", "django", "flask", "spring boot",
    "spring", "rails", "laravel", ".net", "asp.net",
    # Databases
    "postgresql", "postgres", "mysql", "mongodb", "redis", "elasticsearch",
    "dynamodb", "cosmosdb", "cassandra", "sqlite", "sql", "nosql",
    "neo4j", "cockroachdb", "supabase",
    # Cloud & Infra
    "aws", "azure", "gcp", "google cloud", "docker", "kubernetes", "k8s",
    "terraform", "ansible", "jenkins", "ci/cd", "github actions",
    "cloudformation", "pulumi",
    # Data & ML
    "kafka", "spark", "hadoop", "airflow", "flink", "databricks",
    "tensorflow", "pytorch", "scikit-learn", "pandas", "numpy",
    # Tools & Practices
    "git", "linux", "rest apis", "graphql", "grpc", "microservices",
    "distributed systems", "event-driven", "message queues", "rabbitmq",
    "oauth", "jwt", "websockets",
    # Monitoring
    "datadog", "grafana", "prometheus", "splunk", "new relic",
}


def _file_hash(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


def _cache_path(pdf_path: Path) -> Path:
    return pdf_path.with_suffix(".parsed.json")


def _extract_text(pdf_path: Path) -> str:
    text_parts: list[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
    return "\n".join(text_parts)


def _extract_skills(text: str) -> list[str]:
    text_lower = text.lower()
    found: list[str] = []
    for skill in COMMON_TECH_SKILLS:
        # Word boundary match for short skills, substring for longer ones
        if len(skill) <= 3:
            if re.search(rf"\b{re.escape(skill)}\b", text_lower):
                found.append(skill)
        else:
            if skill in text_lower:
                found.append(skill)
    return sorted(set(found))


def _extract_yoe(text: str) -> int | None:
    # First try explicit statements
    patterns = [
        r"(\d+)\+?\s*years?\s*of\s*(?:professional\s*)?experience",
        r"(\d+)\+?\s*years?\s*(?:in|of|working)",
        r"experience[:\s]*(\d+)\+?\s*years?",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return int(match.group(1))

    # Fallback: infer from work history date ranges, excluding education/internships/projects
    # Split text into lines to check surrounding context
    lines = text.split("\n")
    month_pattern = r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
    date_range_pattern = re.compile(
        rf"(?:{month_pattern}\s+)?(\d{{4}})\s*[-–—]+\s*"
        rf"(?:(?:{month_pattern}\s+)?(\d{{4}})|([Pp]resent|[Cc]urrent|[Nn]ow))"
    )

    # Section headers that indicate non-work experience
    exclude_sections = re.compile(
        r"(education|university|college|school|degree|bachelor|master|phd|"
        r"intern(?:ship)?|project|personal|academic|certification|volunteer|"
        r"extracurricular|coursework|award|honor|fellowship)",
        re.IGNORECASE,
    )

    current_year = datetime.now().year
    current_section_excluded = False
    work_year_ranges: list[tuple[int, int]] = []

    for line in lines:
        stripped = line.strip()

        # Detect section headers: short lines, often uppercase or title-like
        if stripped and len(stripped) < 60 and not re.search(r"\d{4}", stripped):
            if exclude_sections.search(stripped):
                current_section_excluded = True
                continue
            # A new non-excluded section resets the flag
            # Heuristic: lines that look like headers (short, no dates)
            if len(stripped) < 40:
                current_section_excluded = False

        if current_section_excluded:
            continue

        # Also skip lines (and nearby context) that mention intern/project
        if re.search(r"\bintern\b", stripped, re.IGNORECASE):
            current_section_excluded = True
            continue

        for match in date_range_pattern.findall(stripped):
            start_year = int(match[0])
            end_year = int(match[1]) if match[1] else current_year

            if 1990 <= start_year <= end_year <= current_year:
                work_year_ranges.append((start_year, end_year))
                # Reset exclusion flag — we found a valid date in a work section
                current_section_excluded = False

    if not work_year_ranges:
        return None

    merged_ranges: list[tuple[int, int]] = []
    for start_year, end_year in sorted(work_year_ranges):
        if not merged_ranges:
            merged_ranges.append((start_year, end_year))
            continue

        last_start, last_end = merged_ranges[-1]
        if start_year <= last_end:
            merged_ranges[-1] = (last_start, max(last_end, end_year))
        else:
            merged_ranges.append((start_year, end_year))

    yoe = sum(end_year - start_year for start_year, end_year in merged_ranges)
    logger.debug(f"Inferred {yoe} YOE from work date ranges {merged_ranges}")
    return yoe


def _extract_job_titles(text: str) -> list[str]:
    title_patterns = [
        r"((?:senior|junior|lead|staff|principal)?\s*software\s+engineer(?:\s+\w+)?)",
        r"((?:senior|junior|lead|staff|principal)?\s*(?:backend|frontend|full[\s-]?stack)\s+(?:developer|engineer))",
        r"((?:senior|junior|lead|staff|principal)?\s*data\s+(?:engineer|scientist))",
        r"((?:senior|junior|lead|staff|principal)?\s*(?:devops|sre|platform)\s+engineer)",
    ]
    titles: list[str] = []
    for pattern in title_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        titles.extend(m.strip() for m in matches)
    return list(set(titles))


def _extract_keywords(text: str) -> list[str]:
    """Extract additional keywords beyond skills - company names, technologies, etc."""
    # Find capitalized phrases that might be technologies or tools
    caps_pattern = r"\b([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)\b"
    candidates = re.findall(caps_pattern, text)
    # Filter out common English words
    stopwords = {"The", "And", "For", "With", "From", "This", "That", "Have",
                 "Been", "Will", "Can", "May", "Also", "But", "Not", "All",
                 "Any", "Each", "Our", "His", "Her", "Their", "About", "Into"}
    keywords = [w for w in candidates if w not in stopwords and len(w) > 2]
    return list(set(keywords))


def parse_resume(
    pdf_path: str,
    fallback_skills: list[str] | None = None,
) -> dict:
    path = Path(pdf_path)
    if not path.exists():
        logger.warning(f"Resume not found at {pdf_path}")
        return {
            "skills": fallback_skills or [],
            "keywords": [],
            "job_titles": [],
            "yoe": None,
            "raw_text": "",
        }

    cache = _cache_path(path)
    current_hash = _file_hash(path)

    # Check cache
    if cache.exists():
        try:
            cached = json.loads(cache.read_text())
            if (
                cached.get("_hash") == current_hash
                and cached.get("_parser_version") == PARSER_CACHE_VERSION
            ):
                logger.info("Using cached resume parse")
                return cached
        except (json.JSONDecodeError, KeyError):
            pass

    logger.info(f"Parsing resume from {pdf_path}")
    raw_text = _extract_text(path)

    if not raw_text.strip():
        logger.warning("No text extracted from resume PDF")
        skills = fallback_skills or []
    else:
        skills = _extract_skills(raw_text)
        if not skills and fallback_skills:
            logger.info("No skills found in resume, using fallback skills")
            skills = fallback_skills

    result = {
        "skills": skills,
        "keywords": _extract_keywords(raw_text),
        "job_titles": _extract_job_titles(raw_text),
        "yoe": _extract_yoe(raw_text),
        "raw_text": raw_text,
        "_hash": current_hash,
        "_parser_version": PARSER_CACHE_VERSION,
    }

    # Cache result
    try:
        cache.write_text(json.dumps(result, indent=2))
        logger.info(f"Cached resume parse to {cache}")
    except OSError as e:
        logger.warning(f"Failed to cache resume parse: {e}")

    return result
