import logging
import re
from typing import Dict, List, Optional

import pandas as pd
import pycountry
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)


# ---------- Constants & Maps ----------

US_STATE_CODES = {
    s.name: s.code.split("-")[1] for s in pycountry.subdivisions.get(country_code="US")
}

CITY_NORMALIZATION_MAP = {
    # USA
    "nyc": "New York City",
    "new york": "New York City",
}

UNMAPPED_COUNTRY_CODES = {"turkey": "TR", "uk": "GB"}

CITY_STATES = {
    "SG",  # Singapore
    "HK",  # Hong Kong
    "MC",  # Monaco
    "VA",  # Vatican City
    "MO",  # Macau
}

CATEGORY_KEYWORDS = {
    "Software Engineering": [
        "software engineer",
        "developer",
        "development",
        "programmer",
        "application engineer",
        "frontend",
        "network engineer",
        "operations engineer",
        "backend",
        "full stack",
        "embedded engineer",
        "firmware engineer",
        "devops engineer",
        "site reliability engineer",
        "sre",
        "cyber security",
        "cloud engineer",
        "solutions engineer",
        "application engineer",
    ],
    "Data and Analytics": [
        "data",
        "analytics",
        "analysis",
        "analyst",
        "machine learning",
        "ml",
        "artificial intelligence",
        "ai",
        "business intelligence",
        "bi developer",
        "statistics",
        "scientist",
        "science",
        "operations research",
        "forecasting",
        "etl",
        "research engineer",
        "database",
        "modeler",
        "warehouse",
        "value",
        "aws",
        "azure",
        "gcp",
        "google cloud",
        "ehs",
        "sap",
    ],
    "Computer and IT": [
        " it ",
        "computer",
        "support",
        "helpdesk",
        "help desk",
        "infrastracture",
        "administrator",
        "sysadmin",
        "security",
        "technician",
        "product manager",
        "product owner",
        "it manager",
        "it director",
    ],
}


# ---------- Generic cleaning helpers functions ----------


def remove_duplicates(
    df: pd.DataFrame, subset: list[str], data_type: str
) -> pd.DataFrame:
    """Remove duplicate rows from a DataFrame based on a given subset of column names."""
    before = len(df)
    df = df.drop_duplicates(subset=subset, keep="first")
    removed = before - len(df)
    logger.info(f"{removed} duplicate {data_type} records removed.")
    return df


def drop_invalid_rows(
    df: pd.DataFrame, data_type: str, subset: List[str]
) -> pd.DataFrame:
    """Drops rows with missing values in any of the given subset of columns."""
    before = len(df)
    df = df.dropna(subset=subset)
    removed = before - len(df)
    logger.info(
        f"Removed {removed} '{data_type}' records due to missing values in {subset}."
    )
    return df


def clean_string(s):
    s = s.lower()
    s = re.sub(r"\([^)]*\)", "", s)  # Text in Klammern entfernen
    s = re.sub(r"[-/,]", " ", s)  # Sonderzeichen
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ---------- Location specific helpers functions ----------


def clean_location_muse(
    loc_list: List[Dict[str, str]]
) -> List[Dict[str, Optional[str]]]:
    """
    Normalize a list of location dictionaries into country, state, and city fields.
    Args:
        loc_list (list[dict]): List of locations with a "name" field (i.e., "Berlin, Germany", "Austin, TX", "Flexible / Remote").
    Returns:
        list[dict]: Normalized locations with keys: country_code, subdivision_code, city
        i.e. [{'country': None, 'state': None, 'city': 'Flexible/Remote'}, {'country_code': 'US', 'subdivision_code': 'US-DC', 'city': 'Washington'}]
    """

    cleaned = []

    if not loc_list:
        return []

    for location in loc_list:
        name = location.get("name", "").strip()
        if not name:
            continue

        # special case "remote/flexible"
        if "remote" in name.lower():
            cleaned.append(
                {
                    "country": "Flexible/Remote",
                    "state": "Flexible/Remote",
                    "city": "Flexible/Remote",
                }
            )
            continue

        # special case only country, no city, i.e. "Canada", "Singapore", "Hong Kong"
        country = normalize_country(name)
        if country:
            city = name if country in CITY_STATES else None
            cleaned.append(
                {"country_code": country, "subdivision_code": None, "city": city}
            )
            continue

        # other cases
        parts = [p.strip() for p in name.split(",")]
        if len(parts) != 2:
            # Fallback, if format not as expected
            cleaned.append({"country": None, "state": None, "city": name})
            continue

        country = None
        state = None
        city, region = parts

        if region in US_STATE_CODES.values():
            country = "US"
            state = f"US-{region}"
        else:
            country = normalize_country(region)
            if not country:
                country = region
            state = None

        city = normalize_city(city)
        cleaned.append(
            {"country_code": country, "subdivision_code": state, "city": city}
        )
    return cleaned


def clean_location_adzuna(loc_list: List[str]) -> List[Dict[str, Optional[str]]]:
    """
    Convert an Adzuna-style location list into normalized country, state, and city fields.
    Args:
        loc_list (list[str]): Location parts (i.e. ["US", "Kansas", "Johnson County", "Lenexa"]).
    Returns:
        list[dict]: Normalized location with keys: country_code, subdivision_code, city
        i.e. [{'country_code': 'US', 'subdivision_code': 'US-KS', 'city': 'Lenexa'}]
    """
    if not loc_list:
        return []

    country_raw = loc_list[0]
    country = normalize_country(country_raw)

    state = None
    if len(loc_list) >= 2:
        state_raw = loc_list[1]
        if state_raw in US_STATE_CODES:
            state = US_STATE_CODES[state_raw]
            state = f"US-{state}"
        elif len(state_raw) == 2 and state_raw.isupper():
            state = f"US-{state_raw}"

    city = None
    if len(loc_list) >= 2:
        for part in reversed(loc_list[1:]):
            if not any(
                x in part.lower() for x in ["county", "district"]
            ) and part not in [country_raw, state_raw]:
                city = normalize_city(part)
                break

    cleaned = [{"country_code": country, "subdivision_code": state, "city": city}]
    return cleaned


def normalize_country(country_name: Optional[str]) -> Optional[str]:
    """Normalize a country name to its ISO alpha-2 code."""
    if not country_name:
        return None
    key = re.sub(r"[^\w\s]", "", country_name.lower()).strip()
    if key in UNMAPPED_COUNTRY_CODES:
        return UNMAPPED_COUNTRY_CODES[key]
    try:
        return pycountry.countries.lookup(key).alpha_2
    except LookupError:
        return None


def normalize_city(city: Optional[str]) -> Optional[str]:
    """Normalize a city name using a predefined mapping."""
    if not city:
        return None
    c = city.strip().lower()
    if c in CITY_NORMALIZATION_MAP:
        return CITY_NORMALIZATION_MAP[c]
    else:
        return city


# ---------- Title specific helpers functions ----------


def extract_category(title):
    # keyword based matching
    for cat, kws in CATEGORY_KEYWORDS.items():
        if any(kw in title for kw in kws):
            return cat

    # Fuzzy fallback
    all_keys = [(kw, cat) for cat, kws in CATEGORY_KEYWORDS.items() for kw in kws]
    best_kw, best_cat = None, None
    best_score = 0

    for kw, cat in all_keys:
        score = fuzz.partial_ratio(title, kw)
        if score > best_score:
            best_kw, best_cat, best_score = kw, cat, score

    if best_score >= 80:
        return best_cat
    return "Software Engineering"


def extract_level(title):
    title = title.lower()
    if any(x in title for x in ["intern", "internship", "trainee", "student"]):
        return "Internship"
    elif any(x in title for x in ["junior", "jr", "entry", "graduate", "apprentice"]):
        return "Entry Level"
    elif any(
        x in title
        for x in [
            "senior",
            "sr ",
            "sr.",
            "sr-",
            "lead",
            "principal",
            "head",
            "director",
            "vice president",
            "vp",
            "manager",
            "chief",
            "architect",
            "mgr",
            "management",
        ]
    ):
        return "Senior Level"
    else:
        return "Mid Level"


# ---------- Logging Function ----------


def log_null_values(df, data_type):
    null_counts = df.isnull().sum()
    total_nulls = int(null_counts.sum())
    logger.info(f"Null value summary for '{data_type}':")
    for col, n in null_counts.items():
        if n > 0:
            logger.info(f"  - {col}: {n} nulls")
    if total_nulls == 0:
        logger.info(f"No null values found in '{data_type}'.")
    return None
