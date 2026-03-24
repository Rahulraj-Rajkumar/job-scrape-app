from __future__ import annotations

import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from loguru import logger

from .scrapers.base import JobListing


# Common parent company mappings — maps subsidiaries to a canonical name
# for diversity purposes
COMPANY_ALIASES: dict[str, list[str]] = {
    "amazon": ["amazon", "aws", "annapurna labs", "a2z", "amazon web services",
               "amazon.com", "amazon development center"],
    "google": ["google", "alphabet", "deepmind", "waymo", "youtube"],
    "meta": ["meta", "facebook", "instagram", "whatsapp", "oculus"],
    "microsoft": ["microsoft", "github", "linkedin", "azure"],
    "apple": ["apple"],
}


def _normalize_company(name: str) -> str:
    """Normalize company name for diversity grouping."""
    name_lower = name.lower().strip()
    for canonical, aliases in COMPANY_ALIASES.items():
        for alias in aliases:
            if alias in name_lower:
                return canonical
    # Fallback: take first word(s), strip legal suffixes
    cleaned = re.sub(
        r"\b(inc\.?|llc|ltd|corp\.?|co\.?|l\.?p\.?|services|u\.?s\.?)\b",
        "", name_lower,
    ).strip().rstrip(",. ")
    return cleaned or name_lower


def score_listing(
    listing: JobListing,
    resume: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Score a job listing 0-100 based on match criteria."""
    skill_score = _skill_match_score(listing, resume, config)
    company_score = _target_company_score(listing, config)
    location_score = _location_score(listing, config)
    yoe_score = _yoe_fit_score(listing, config)
    recency_score = _recency_score(listing)

    # Penalize listings with very thin descriptions
    # Full ATS descriptions are typically 500+ chars
    desc_len = len(listing.description)
    desc_confidence = min(desc_len / 500, 1.0)  # 0.0 to 1.0

    # Blend: if description is thin, reduce skill score weight and boost base
    effective_skill_score = skill_score * desc_confidence + 50.0 * (1 - desc_confidence)

    total = (
        effective_skill_score * 0.30
        + company_score * 0.20
        + location_score * 0.10
        + yoe_score * 0.15
        + recency_score * 0.25
    )

    matching_skills = _get_matching_skills(listing, resume, config)

    return {
        "listing": listing,
        "total_score": round(total, 1),
        "skill_score": round(skill_score, 1),
        "company_score": round(company_score, 1),
        "location_score": round(location_score, 1),
        "yoe_score": round(yoe_score, 1),
        "recency_score": round(recency_score, 1),
        "matching_skills": matching_skills,
    }


def _skill_match_score(listing: JobListing, resume: dict, config: dict) -> float:
    skills = resume.get("skills", []) or config.get("skills", [])
    if not skills:
        return 50.0
    desc_lower = (listing.description + " " + listing.title).lower()
    matched = sum(1 for s in skills if s.lower() in desc_lower)
    # Expect ~30% of skills to match a good listing, so scale accordingly
    ratio = matched / max(len(skills), 1)
    return min(ratio * 100 * 3, 100.0)


def _get_matching_skills(listing: JobListing, resume: dict, config: dict) -> list[str]:
    skills = resume.get("skills", []) or config.get("skills", [])
    desc_lower = (listing.description + " " + listing.title).lower()
    return [s for s in skills if s.lower() in desc_lower]


def _target_company_score(listing: JobListing, config: dict) -> float:
    targets = [c.lower() for c in config.get("target_companies", [])]
    company_lower = listing.company.lower()
    normalized = _normalize_company(listing.company)
    for target in targets:
        if (target in company_lower
                or company_lower in target
                or target in normalized
                or normalized in target):
            return 100.0
    return 0.0


def _location_score(listing: JobListing, config: dict) -> float:
    location_lower = listing.location.lower()
    if not location_lower:
        return 30.0  # Unknown location

    # Check remote
    if config.get("include_remote") and any(
        kw in location_lower for kw in ["remote", "anywhere", "distributed"]
    ):
        return 90.0

    # Check preferred locations
    preferred = config.get("preferred_locations", [])
    for i, loc in enumerate(preferred):
        loc_parts = [p.strip().lower() for p in loc.split(",")]
        if any(part in location_lower for part in loc_parts):
            # Earlier in list = more preferred
            return 100.0 - (i * 5)

    # Check country
    country = config.get("country", "US").lower()
    if country == "us":
        if _looks_like_us_location(location_lower):
            return 50.0
    elif re.search(rf"\b{re.escape(country)}\b", location_lower):
        return 50.0

    return 20.0


def _yoe_fit_score(listing: JobListing, config: dict) -> float:
    max_yoe = config.get("max_yoe_required", 4)

    # Try to extract YOE from description
    yoe_required = listing.yoe_required
    if yoe_required is None:
        yoe_required = _extract_yoe_from_desc(listing.description)

    if yoe_required is None:
        return 70.0  # Unknown = moderate score

    if yoe_required <= max_yoe:
        # Closer to our YOE = higher score
        closeness = 1 - abs(max_yoe - yoe_required) / max(max_yoe, 1)
        return 60.0 + closeness * 40.0
    else:
        # Penalize if over our max
        overage = yoe_required - max_yoe
        return max(0.0, 60.0 - overage * 15.0)


def _extract_yoe_from_desc(description: str) -> int | None:
    # Normalize non-breaking spaces and whitespace
    description = re.sub(r"[\xa0\u200b]", " ", description)
    patterns = [
        r"(\d+)\+?\s*years?\s*of\s*[\w\s]*experience",
        r"(\d+)\+?\s*years?\s*(?:relevant\s*)?experience",
        r"(\d+)\+?\s*yoe",
        r"experience[:\s]*(\d+)\+?\s*years?",
    ]
    matches: list[int] = []
    for pattern in patterns:
        for m in re.finditer(pattern, description, re.IGNORECASE):
            matches.append(int(m.group(1)))
    # Use max — if a listing mentions both "10+ years" and "5+ years",
    # the higher number is the actual requirement
    return max(matches) if matches else None


def _recency_score(listing: JobListing) -> float:
    if not listing.posted_date:
        return 40.0

    now = datetime.now(timezone.utc)
    posted = listing.posted_date
    if posted.tzinfo is None:
        posted = posted.replace(tzinfo=timezone.utc)

    days_ago = (now - posted).days

    if days_ago <= 0:
        return 100.0
    elif days_ago <= 1:
        return 90.0
    elif days_ago <= 3:
        return 70.0
    elif days_ago <= 7:
        return 50.0
    elif days_ago <= 14:
        return 30.0
    else:
        return 10.0


def rank_listings(
    scored: list[dict[str, Any]],
    max_results: int = 10,
) -> list[dict[str, Any]]:
    """Rank scored listings, preferring variety across companies."""
    sorted_results = sorted(scored, key=lambda x: x["total_score"], reverse=True)

    selected: list[dict[str, Any]] = []
    company_counts: Counter[str] = Counter()
    max_per_company = 2  # Hard cap: max 2 listings per company group

    # First pass: pick top results with company diversity
    for result in sorted_results:
        if len(selected) >= max_results:
            break
        company = _normalize_company(result["listing"].company)
        if company_counts[company] < max_per_company:
            selected.append(result)
            company_counts[company] += 1

    # If we still need more, relax to 3 per company
    if len(selected) < max_results:
        selected_ids = {id(r) for r in selected}
        for result in sorted_results:
            if len(selected) >= max_results:
                break
            if id(result) in selected_ids:
                continue
            company = _normalize_company(result["listing"].company)
            if company_counts[company] < 3:
                selected.append(result)
                selected_ids.add(id(result))
                company_counts[company] += 1

    return selected


US_STATES = {
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id", "il",
    "in", "ia", "ks", "ky", "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt",
    "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri",
    "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy", "dc",
}

NON_US_MARKERS = {
    "germany",
    "france",
    "spain",
    "italy",
    "netherlands",
    "sweden",
    "norway",
    "denmark",
    "finland",
    "switzerland",
    "austria",
    "belgium",
    "ireland",
    "poland",
    "uk",
    "united kingdom",
    "england",
    "scotland",
    "wales",
    "canada",
    "toronto",
    "vancouver",
    "montreal",
    "australia",
    "sydney",
    "melbourne",
    "india",
    "bangalore",
    "bengaluru",
    "singapore",
    "japan",
    "tokyo",
    "china",
    "hong kong",
    "uae",
    "dubai",
}


def _looks_like_us_location(loc: str) -> bool:
    """Heuristic check if a location string looks like it's in the US."""
    if any(marker in loc for marker in NON_US_MARKERS):
        return False
    if "united states" in loc or ", us" in loc:
        return True
    # Check for US state abbreviations (e.g. "Seattle, WA" or "NY")
    words = re.findall(r"\b([a-z]{2})\b", loc)
    if any(w in US_STATES for w in words):
        return True
    # Common US city names that appear without state
    us_markers = ["new york", "san francisco", "seattle", "chicago", "austin",
                  "los angeles", "boston", "denver", "portland", "atlanta"]
    if any(m in loc for m in us_markers):
        return True
    return False


def _is_engineering_role(title_lower: str) -> bool:
    """Check if the title is an actual software engineering role."""
    # Must-have: some engineering/developer indicator
    eng_terms = [
        "software engineer", "software developer", "backend engineer",
        "frontend engineer", "full stack", "fullstack", "full-stack",
        "platform engineer", "infrastructure engineer", "data engineer",
        "systems engineer", "site reliability", "sre ", "devops engineer",
        "machine learning engineer", "ml engineer", "ai engineer",
        "backend developer", "frontend developer", "web developer",
    ]
    if any(term in title_lower for term in eng_terms):
        # Exclude non-IC roles that happen to match
        exclude_terms = [
            "curriculum", "solutions engineer", "sales engineer",
            "support engineer", "field engineer", "customer engineer",
            "technical account", "forward deployed", "enablement",
        ]
        return not any(term in title_lower for term in exclude_terms)
    return False


def _infer_seniority(title_lower: str) -> str | None:
    """Infer role seniority from title."""
    entry_terms = [
        "junior",
        "entry",
        "associate",
        "apprentice",
        "graduate",
        "new grad",
        "level 1",
        "l1",
    ]
    senior_terms = [
        "senior",
        "staff",
        "principal",
        "lead",
        "manager",
        "director",
        "architect",
        "level 3",
        "l3",
        "iii",
    ]
    mid_terms = [
        "mid",
        "ii",
        "level 2",
        "l2",
    ]

    if any(term in title_lower for term in senior_terms):
        return "senior"
    if re.search(r"\b(iii|3)\b", title_lower):
        return "senior"
    if any(term in title_lower for term in mid_terms):
        return "mid"
    if re.search(r"\b(ii|2)\b", title_lower):
        return "mid"
    if any(term in title_lower for term in entry_terms):
        return "entry"
    if re.search(r"\b(i|1)\b", title_lower):
        return "entry"
    return None


def _has_mismatched_domain(desc_lower: str) -> bool:
    """Check if the job description requires domain expertise we don't have."""
    # Roles requiring specialized non-SWE backgrounds
    mismatch_indicators = [
        "detection engineering",
        "incident response",
        "security operations",
        "curriculum developer",
        "training material",
        "instructor-led",
        "hands-on labs",
        "sales quota",
        "quota carrying",
        "book of business",
        "customer-facing sales",
        "consulting role",
    ]
    matches = sum(1 for ind in mismatch_indicators if ind in desc_lower)
    # If 2+ indicators match, it's likely a mismatched domain
    return matches >= 2


def filter_listings(
    listings: list[JobListing],
    config: dict[str, Any],
) -> list[JobListing]:
    """Apply exclusion filters before scoring."""
    excluded_companies = {c.lower() for c in config.get("excluded_companies", [])}
    excluded_types = {t.lower() for t in config.get("excluded_company_types", [])}
    allowed_seniority = {s.lower() for s in config.get("seniority_levels", [])}
    max_yoe = config.get("max_yoe_required", 4)

    filtered: list[JobListing] = []
    for listing in listings:
        company_lower = listing.company.lower()
        normalized = _normalize_company(listing.company)

        # Skip excluded companies
        if company_lower in excluded_companies or normalized in excluded_companies:
            continue

        # Skip listings with broken/generic URLs (e.g. search pages)
        if listing.url and "/jobs/search" in listing.url and "gh_jid=" in listing.url:
            continue

        # Skip new grad, university grad, and internship roles
        title_lower = listing.title.lower()
        if re.search(r"\b(new\s*grad|university\s*grad|intern(ship)?|co-?op)\b", title_lower):
            continue

        # Skip excluded company types (check description for agency indicators)
        desc_lower = listing.description.lower()
        if any(t in desc_lower or t in company_lower for t in excluded_types):
            continue

        # Skip if YOE too high (strict: only 1 year buffer)
        yoe = listing.yoe_required or _extract_yoe_from_desc(listing.description)
        if yoe is not None and yoe > max_yoe + 1:
            continue

        # Skip non-SWE roles based on title
        if not _is_engineering_role(title_lower):
            continue

        # Skip roles requiring experience we clearly don't have
        if _has_mismatched_domain(desc_lower):
            continue

        # Respect configured seniority when we can infer it.
        if allowed_seniority:
            inferred_seniority = _infer_seniority(title_lower)
            if inferred_seniority and inferred_seniority not in allowed_seniority:
                continue

        # Skip stale listings (older than 30 days)
        if listing.posted_date:
            posted = listing.posted_date
            if posted.tzinfo is None:
                posted = posted.replace(tzinfo=timezone.utc)
            age_days = (datetime.now(timezone.utc) - posted).days
            if age_days > 30:
                continue

        # Skip non-US locations (unless remote or location unknown)
        country = config.get("country", "").lower()
        if country == "us" and listing.location:
            loc_lower = listing.location.lower()
            is_remote = any(kw in loc_lower for kw in ["remote", "anywhere", "distributed"])
            is_us = _looks_like_us_location(loc_lower)
            if not is_remote and not is_us:
                continue

        filtered.append(listing)

    logger.info(f"Filtered {len(listings)} -> {len(filtered)} listings")
    return filtered
