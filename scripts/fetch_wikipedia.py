#!/usr/bin/env python3
"""Fetch Wikipedia articles for all featured StoryMap languages.

Uses the Wikipedia API to download article content as markdown-formatted
text files for use as reference material when writing curated descriptions.

Usage:
    uv run scripts/fetch_wikipedia.py

Output:
    references/wikipedia/<iso_code>_<name>.md for each featured language
"""

import re
import time
from pathlib import Path

import pandas as pd
import requests

# --- Configuration ---

DATA_DIR = Path("data")
LANGUAGES_CSV = DATA_DIR / "languages_dataset.csv"
OUTPUT_DIR = Path("references") / "wikipedia"

THRESHOLDS = {
    "Mayan": 100,
    "Quechua": 5,
}

RESTRICTED_THRESHOLD = 0.50

# Wikipedia API endpoint
WIKI_API = "https://en.wikipedia.org/w/api.php"

# Rate limiting: be polite to Wikipedia
REQUEST_DELAY = 1.0

# User-Agent per Wikipedia API policy
HEADERS = {
    "User-Agent": "AILLALanguageAtlas/1.0 (academic research; LBDS fellowship project)"
}

# Manual mapping for languages whose Wikipedia article title doesn't match
# a simple "<name> language" search. Keyed by language_id.
WIKI_TITLE_OVERRIDES: dict[int, str] = {
    14: "Ch'orti' language",
    145: "Ch'ol language",
    156: "Wastek language",
    157: "Chuj language",
    174: "Tzotzil language",
    175: "Tzeltal language",
    177: "Awakatek language",
    181: "Sakapultek language",
    183: "Uspantek language",
    131: "Kʼicheʼ language",
    29: "Qʼanjobʼal language",
    32: "Popti' language",
    34: "Poqomchi' language",
    46: "Ixil language",
    85: "Yucatec Maya language",
    39: "Mocho language",
    33: "Mam language",
    30: "Kaqchikel language",
    15: "Pastaza Quechua",
    213: "South Bolivian Quechua",
    272: "Yauyos Quechua",
    133: "Ancash Quechua",
    27: "Inga language",
    539: "Chachapoyas Quechua",
    232: "Cañar–Azuay Quechua",
    89: "Cusco Quechua",
    233: "Huallaga Quechua",
}


def search_wikipedia(query: str) -> str | None:
    """Search Wikipedia for an article title matching the query.

    Args:
        query: Search string (e.g., "Tzeltal language").

    Returns:
        The best-matching article title, or None if not found.
    """
    params = {
        "action": "query",
        "list": "search",
        "srsearch": query,
        "srlimit": 5,
        "format": "json",
    }
    resp = requests.get(WIKI_API, params=params, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    results = resp.json().get("query", {}).get("search", [])

    if not results:
        return None

    # Prefer results with "language" in the title
    for r in results:
        title = r["title"]
        if "language" in title.lower() or "quechua" in title.lower():
            return title

    return results[0]["title"]


def fetch_article(title: str) -> dict | None:
    """Fetch a Wikipedia article's content as plain text.

    Args:
        title: Exact Wikipedia article title.

    Returns:
        Dict with 'title', 'extract' (plain text), and 'url', or None.
    """
    params = {
        "action": "query",
        "titles": title,
        "prop": "extracts|info",
        "explaintext": True,
        "inprop": "url",
        "format": "json",
    }
    resp = requests.get(WIKI_API, params=params, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    pages = resp.json().get("query", {}).get("pages", {})

    for page_id, page in pages.items():
        if page_id == "-1":
            return None
        return {
            "title": page.get("title", title),
            "extract": page.get("extract", ""),
            "url": page.get("fullurl", ""),
        }

    return None


def sanitize_filename(name: str) -> str:
    """Convert a language name to a safe filename component."""
    name = name.lower()
    name = re.sub(r"[''ʼ]", "", name)
    name = re.sub(r"[,\s]+", "_", name)
    name = re.sub(r"[^a-z0-9_-]", "", name)
    return name.strip("_")


def get_featured_languages(languages: pd.DataFrame) -> pd.DataFrame:
    """Get all featured languages using the same logic as build_storymaps.py."""
    featured_rows = []

    for family_name, threshold in THRESHOLDS.items():
        family = languages[languages["language_family"] == family_name].copy()

        public_col = "public_items" if "public_items" in family.columns else "total_items"
        family["restricted_pct"] = family.apply(
            lambda r: (r["total_items"] - r[public_col]) / r["total_items"]
            if pd.notna(r["total_items"]) and r["total_items"] > 0 else 0.0,
            axis=1,
        )

        qualifies_public = family[public_col] >= threshold
        qualifies_total = (
            (family["total_items"] >= threshold)
            & (family["restricted_pct"] < RESTRICTED_THRESHOLD)
        )

        featured = family[qualifies_public | qualifies_total]
        featured_rows.append(featured)

    return pd.concat(featured_rows, ignore_index=True).sort_values(
        ["language_family", "earliest_item_year"], na_position="last"
    )


def main() -> None:
    """Fetch Wikipedia articles for all featured languages."""
    print("=" * 60)
    print("WIKIPEDIA REFERENCE FETCHER")
    print("=" * 60)

    languages = pd.read_csv(LANGUAGES_CSV)
    featured = get_featured_languages(languages)
    print(f"Featured languages: {len(featured)}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    fetched = 0
    failed = []

    for _, row in featured.iterrows():
        lang_id = int(row["language_id"])
        name = row["name_en"]
        iso = row.get("iso_639_3_code", "")
        iso_str = str(iso).strip() if pd.notna(iso) else ""

        print(f"\n--- {name} (ID {lang_id}, ISO {iso_str}) ---")

        # Determine search query
        if lang_id in WIKI_TITLE_OVERRIDES:
            search_query = WIKI_TITLE_OVERRIDES[lang_id]
        else:
            search_query = f"{name} language"

        # Search for the article
        print(f"  Searching: {search_query}")
        title = search_wikipedia(search_query)
        time.sleep(REQUEST_DELAY)

        if not title:
            print(f"  No Wikipedia article found")
            failed.append(name)
            continue

        print(f"  Found: {title}")

        # Fetch the article
        article = fetch_article(title)
        time.sleep(REQUEST_DELAY)

        if not article or not article["extract"]:
            print(f"  Failed to fetch article content")
            failed.append(name)
            continue

        # Save to file
        filename = f"{iso_str}_{sanitize_filename(name)}.md" if iso_str else f"{sanitize_filename(name)}.md"
        filepath = OUTPUT_DIR / filename

        content = f"# {article['title']}\n\n"
        content += f"**Source:** {article['url']}\n"
        content += f"**Retrieved:** 2026-03-15\n"
        content += f"**AILLA Language ID:** {lang_id}\n"
        content += f"**ISO 639-3:** {iso_str}\n\n"
        content += "---\n\n"
        content += article["extract"]

        filepath.write_text(content, encoding="utf-8")
        print(f"  Saved: {filepath} ({len(article['extract']):,} chars)")
        fetched += 1

    print(f"\n{'=' * 60}")
    print(f"Fetched: {fetched}/{len(featured)}")
    if failed:
        print(f"Failed: {', '.join(failed)}")
    print(f"Output: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
