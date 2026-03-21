#!/usr/bin/env python3
"""
Live AILLA Site Item Count Scraper
====================================

Paginates through all items on the live AILLA API to count items per language,
then compares against AILLA2 pre-migration data to identify discrepancies.

The AILLA2 spreadsheets are pre-migration snapshots and may not reflect items
added or retagged post-migration. This script provides live counts by:
1. Paginating all /items (19,786+ items at 10/page, API hard cap)
2. Counting items per language via subject_languages and media_languages
3. Fetching collection counts per featured language via /languages/{id}/collections
4. Comparing against data/languages_dataset.csv

Dual counting approach:
- subject_languages: what the item documents (what the AILLA site organizes by)
- media_languages: what languages appear in the content (comparable to AILLA2 methodology)

Usage:
    uv run scripts/scrape_live_counts.py              # Full run (~50 min, 10 items/page)
    uv run scripts/scrape_live_counts.py --resume      # Resume from checkpoint
    uv run scripts/scrape_live_counts.py --report-only  # Regenerate report from saved extract

Output:
    data/live_items_extract.json   - lightweight item-language mapping
    data/live_counts.csv           - per-language comparison data
    data/live_vs_ailla2_report.txt - formatted discrepancy report

Author: LBDS Fellow, Benson Latin American Collection
Date: 2026-03-16
"""

import argparse
import json
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests

# Configuration
BASE_URL = "https://ailla-backend-prod.gsc1-pub.lib.utexas.edu"
HEADERS = {
    "User-Agent": "AILLA-Language-Atlas-Scraper/2.0 (LBDS Fellowship Project; Benson Collection)"
}
RATE_LIMIT_DELAY = 1.5  # seconds between requests
REQUEST_TIMEOUT = 60  # seconds
ITEMS_PER_PAGE = 100  # request 100; API hard-caps at 10 per page
CHECKPOINT_INTERVAL = 100  # save checkpoint every N pages
CHECKPOINT_FILE = Path(__file__).parent.parent / "data" / "live_items_checkpoint.json"

# Meta-language IDs to exclude from media_languages counting
# (matches AILLA2 hybrid counting methodology in extract_ailla2.py)
META_LANGUAGE_IDS = {8, 9, 399, 641}  # English, Spanish, Portuguese, No linguistic content

# Project paths
DATA_DIR = Path(__file__).parent.parent / "data"
EXTRACT_FILE = DATA_DIR / "live_items_extract.json"
COUNTS_CSV = DATA_DIR / "live_counts.csv"
REPORT_FILE = DATA_DIR / "live_vs_ailla2_report.txt"
LANGUAGES_CSV = DATA_DIR / "languages_dataset.csv"
DESCRIPTIONS_JSON = DATA_DIR / "curated_descriptions.json"

# StoryMap thresholds (must match build_storymaps.py)
FAMILY_THRESHOLDS = {"Mayan": 100, "Quechua": 5}
RESTRICTED_THRESHOLD = 0.50


def get_featured_language_ids() -> set[int]:
    """Identify all 27 featured languages using build_storymaps.py threshold logic.

    A language is featured if:
    - public_items >= family threshold, OR
    - total_items >= family threshold AND restricted % < 50%

    Also includes any language with a curated description.

    Returns:
        Set of language_id values for featured languages.
    """
    df = pd.read_csv(LANGUAGES_CSV)
    featured = set()

    # From threshold logic
    for family, threshold in FAMILY_THRESHOLDS.items():
        family_df = df[df["language_family"] == family].copy()
        if family_df.empty:
            continue

        family_df["restricted_pct"] = family_df.apply(
            lambda r: (r["total_items"] - r["public_items"]) / r["total_items"]
            if r["total_items"] > 0 else 0.0,
            axis=1,
        )

        for _, row in family_df.iterrows():
            qualifies_public = row["public_items"] >= threshold
            qualifies_total = (
                row["total_items"] >= threshold
                and row["restricted_pct"] < RESTRICTED_THRESHOLD
            )
            if qualifies_public or qualifies_total:
                featured.add(int(row["language_id"]))

    # From curated descriptions
    if DESCRIPTIONS_JSON.exists():
        with open(DESCRIPTIONS_JSON, encoding="utf-8") as f:
            descs = json.load(f)
        for key in descs:
            if key != "_comment":
                featured.add(int(key))

    return featured


def fetch_all_items(session: requests.Session, resume: bool = False) -> list[dict]:
    """Paginate through all /items and extract language mappings.

    For each item, extracts: id, subject_language_ids, media_language_ids,
    collection_id.

    Args:
        session: Configured requests session.
        resume: If True, attempt to resume from checkpoint.

    Returns:
        List of lightweight item dicts.
    """
    items: list[dict] = []
    start_page = 1

    # Resume from checkpoint if requested
    if resume and CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, encoding="utf-8") as f:
                checkpoint = json.load(f)
            items = checkpoint.get("items", [])
            start_page = checkpoint.get("next_page", 1)
            print(f"\nResuming from checkpoint:")
            print(f"  Items recovered: {len(items)}")
            print(f"  Resuming from page: {start_page}")
        except (json.JSONDecodeError, KeyError) as e:
            print(f"\nCheckpoint corrupted, starting fresh: {e}")
            items = []
            start_page = 1
    elif resume:
        print("\nNo checkpoint found, starting fresh.")

    # The /items endpoint uses next/previous URL pagination (no total_pages field)
    # and hard-caps at 10 results per page regardless of per_page parameter.
    print(f"\nFetching items from {BASE_URL}/items...")

    page = start_page
    extraction_start = time.time()
    pages_fetched = 0
    total_count: int | None = None  # from API 'count' field
    actual_per_page: int | None = None  # detected from first response
    consecutive_failures = 0
    max_consecutive_failures = 10

    while True:
        url = f"{BASE_URL}/items?page={page}&per_page={ITEMS_PER_PAGE}"

        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])

            if not results:
                print(f"\n  Page {page}: empty, stopping")
                break

            # Extract lightweight records
            _extract_items(results, items)

            pages_fetched += 1
            consecutive_failures = 0

            # Detect pagination parameters on first response
            if total_count is None:
                total_count = data.get("count", 0)
                actual_per_page = len(results)
                est_total_pages = (
                    (total_count + actual_per_page - 1) // actual_per_page
                    if actual_per_page > 0 else 0
                )
                est_remaining = est_total_pages - start_page + 1
                est_minutes = est_remaining * RATE_LIMIT_DELAY / 60
                print(f"  API reports: {total_count} total items")
                print(f"  Actual page size: {actual_per_page} items/page")
                print(f"  Estimated pages: ~{est_total_pages}")
                print(f"  Estimated time: ~{est_minutes:.0f} minutes")

            # Progress reporting every 50 pages
            if pages_fetched % 50 == 0:
                elapsed = time.time() - extraction_start
                est_total_pages = (
                    (total_count + actual_per_page - 1) // actual_per_page
                    if actual_per_page and actual_per_page > 0 else 2000
                )
                pages_remaining = est_total_pages - page
                avg_time = elapsed / pages_fetched
                eta_min = max(0, pages_remaining * avg_time / 60)
                print(
                    f"  Page {page}/~{est_total_pages} "
                    f"({len(items)} items, ~{eta_min:.0f} min remaining)",
                    flush=True,
                )

            # Checkpoint
            if pages_fetched % CHECKPOINT_INTERVAL == 0:
                _save_checkpoint(items, page + 1)

            # Stop if no next page
            next_url = data.get("next")
            if not next_url:
                break

            page += 1
            time.sleep(RATE_LIMIT_DELAY)

        except requests.exceptions.RequestException as e:
            print(f"\n  ERROR on page {page}: {e}")
            retried = _retry_page(session, url, items)

            if retried:
                pages_fetched += 1
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                print(f"  Skipping page {page} (consecutive failures: {consecutive_failures})")
                _save_checkpoint(items, page + 1)

                if consecutive_failures >= max_consecutive_failures:
                    print(f"\n  {max_consecutive_failures} consecutive failures, stopping.")
                    print(f"  Use --resume to continue later.")
                    break

            page += 1
            time.sleep(RATE_LIMIT_DELAY)

    elapsed = time.time() - extraction_start
    print(f"\nExtraction complete: {len(items)} items in {elapsed/60:.1f} minutes")

    # Clean up checkpoint on successful completion
    if total_count and len(items) >= total_count * 0.95 and CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        print("  Checkpoint file removed (extraction complete)")

    return items


def _extract_items(results: list[dict], items: list[dict]) -> None:
    """Extract lightweight records from API results and append to items list."""
    for item in results:
        item_id = item.get("id")

        # subject_languages: list of dicts with id, name, language_code
        subj_langs = [
            lang["id"]
            for lang in item.get("subject_languages", [])
            if isinstance(lang, dict) and "id" in lang
        ]

        # media_languages: same structure
        media_langs = [
            lang["id"]
            for lang in item.get("media_languages", [])
            if isinstance(lang, dict) and "id" in lang
        ]

        # collection_item_id: dict with id
        coll = item.get("collection_item_id")
        coll_id = coll["id"] if isinstance(coll, dict) and "id" in coll else None

        items.append({
            "item_id": item_id,
            "subject_language_ids": subj_langs,
            "media_language_ids": media_langs,
            "collection_id": coll_id,
        })


def _retry_page(
    session: requests.Session, url: str, items: list[dict]
) -> bool:
    """Retry a failed page request with exponential backoff (3 attempts).

    Returns True if retry succeeded.
    """
    for attempt in range(1, 4):
        wait = attempt * 5
        print(f"  Retrying in {wait}s (attempt {attempt}/3)...")
        time.sleep(wait)
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            if results:
                _extract_items(results, items)
                print(f"  Retry succeeded")
                return True
        except requests.exceptions.RequestException:
            continue
    return False


def _save_checkpoint(items: list[dict], next_page: int) -> None:
    """Save extraction checkpoint for resume capability."""
    checkpoint = {
        "items": items,
        "next_page": next_page,
        "timestamp": datetime.now().isoformat(),
    }
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f)
    print(f"  [checkpoint saved: {len(items)} items, next page {next_page}]")


def count_items_per_language(items: list[dict]) -> dict[int, dict]:
    """Count items per language using both subject and media language fields.

    Args:
        items: List of lightweight item dicts from extraction.

    Returns:
        Dict keyed by language_id with counts and collection sets.
    """
    lang_data: dict[int, dict] = defaultdict(lambda: {
        "subject_items": 0,
        "media_items": 0,
        "collection_ids": set(),
    })

    for item in items:
        coll_id = item.get("collection_id")

        # Count by subject_languages
        for lang_id in item["subject_language_ids"]:
            lang_data[lang_id]["subject_items"] += 1
            if coll_id is not None:
                lang_data[lang_id]["collection_ids"].add(coll_id)

        # Count by media_languages (excluding meta-languages)
        for lang_id in item["media_language_ids"]:
            if lang_id not in META_LANGUAGE_IDS:
                lang_data[lang_id]["media_items"] += 1

    # Convert sets to counts for JSON serialization
    for lang_id in lang_data:
        lang_data[lang_id]["collection_ids"] = len(lang_data[lang_id]["collection_ids"])

    return dict(lang_data)


def fetch_collection_counts(
    session: requests.Session, language_ids: set[int]
) -> dict[int, int]:
    """Fetch collection counts per language via /languages/{id}/collections.

    Args:
        session: Configured requests session.
        language_ids: Set of language IDs to query.

    Returns:
        Dict mapping language_id to collection count from API.
    """
    counts: dict[int, int] = {}
    total = len(language_ids)
    print(f"\nFetching collection counts for {total} featured languages...")

    for i, lang_id in enumerate(sorted(language_ids), 1):
        url = f"{BASE_URL}/languages/{lang_id}/collections"
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            # Response is an array of collection objects
            if isinstance(data, list):
                counts[lang_id] = len(data)
            elif isinstance(data, dict) and "results" in data:
                counts[lang_id] = len(data["results"])
            else:
                counts[lang_id] = 0
            if i % 10 == 0 or i == total:
                print(f"  {i}/{total} languages queried", flush=True)
        except requests.exceptions.RequestException as e:
            print(f"  ERROR for language {lang_id}: {e}")
            counts[lang_id] = -1  # indicate error

        time.sleep(RATE_LIMIT_DELAY)

    return counts


def generate_report(
    lang_counts: dict[int, dict],
    collection_counts: dict[int, int],
    featured_ids: set[int],
) -> None:
    """Generate comparison report and CSV output.

    Creates:
    - data/live_counts.csv: all languages with >0 items
    - data/live_vs_ailla2_report.txt: formatted report for featured languages

    Args:
        lang_counts: Per-language item counts from API.
        collection_counts: Per-language collection counts from API.
        featured_ids: Set of featured language IDs.
    """
    # Load comparison data
    df = pd.read_csv(LANGUAGES_CSV)
    lang_lookup = df.set_index("language_id")

    # Build rows for all languages with >0 items
    rows = []
    for lang_id, counts in sorted(lang_counts.items()):
        row_data = {
            "language_id": lang_id,
            "language_name": "",
            "language_family": "",
            "live_subject_items": counts["subject_items"],
            "live_media_items": counts["media_items"],
            "ailla2_total_items": 0,
            "ailla2_public_items": 0,
            "subject_diff": 0,
            "media_diff": 0,
            "live_collections": collection_counts.get(lang_id, ""),
            "csv_collections": 0,
            "collection_diff": "",
            "featured": lang_id in featured_ids,
        }

        if lang_id in lang_lookup.index:
            lang_row = lang_lookup.loc[lang_id]
            # Handle potential duplicate indices
            if isinstance(lang_row, pd.DataFrame):
                lang_row = lang_row.iloc[0]
            row_data["language_name"] = lang_row.get("name_en", "")
            row_data["language_family"] = lang_row.get("language_family", "")
            row_data["ailla2_total_items"] = (
                int(lang_row["total_items"]) if pd.notna(lang_row.get("total_items")) else 0
            )
            row_data["ailla2_public_items"] = (
                int(lang_row["public_items"]) if pd.notna(lang_row.get("public_items")) else 0
            )
            row_data["csv_collections"] = (
                int(lang_row["collection_count"]) if pd.notna(lang_row.get("collection_count")) else 0
            )

        row_data["subject_diff"] = row_data["live_subject_items"] - row_data["ailla2_total_items"]
        row_data["media_diff"] = row_data["live_media_items"] - row_data["ailla2_total_items"]

        if isinstance(row_data["live_collections"], int) and row_data["live_collections"] >= 0:
            row_data["collection_diff"] = row_data["live_collections"] - row_data["csv_collections"]

        rows.append(row_data)

    out_df = pd.DataFrame(rows)

    # Save full CSV
    out_df.to_csv(COUNTS_CSV, index=False)
    print(f"\nSaved: {COUNTS_CSV} ({len(out_df)} languages with >0 items)")

    # Generate formatted report for featured languages
    featured_df = out_df[out_df["featured"]].sort_values("language_name")

    lines = []
    lines.append("=" * 78)
    lines.append("LIVE AILLA vs AILLA2 ITEM COUNT COMPARISON")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("=" * 78)
    lines.append("")
    lines.append("METHODOLOGY")
    lines.append("-" * 40)
    lines.append(f"API endpoint: {BASE_URL}/items")
    lines.append(f"Total API items: {sum(r['live_subject_items'] for r in rows if r.get('live_subject_items', 0) > 0)}")
    lines.append(f"  (Note: sum of per-language subject counts may exceed total items")
    lines.append(f"   if any items have multiple subject_languages)")
    lines.append("")
    lines.append("Counting methods:")
    lines.append("  live_subject_items: count by subject_languages field")
    lines.append("    (what the AILLA site displays per language page)")
    lines.append("  live_media_items: count by media_languages field,")
    lines.append("    excluding English (8), Spanish (9), Portuguese (399),")
    lines.append("    No linguistic content (641)")
    lines.append("    (comparable to AILLA2 hybrid counting methodology)")
    lines.append("")
    lines.append("IMPORTANT CAVEATS:")
    lines.append("  1. The AILLA frontend shows 'folders', not items. Each folder may")
    lines.append("     contain multiple API items. Folder count != item count.")
    lines.append("  2. API items (~19,786) vs AILLA2 items (18,548): API likely includes")
    lines.append("     post-migration additions.")
    lines.append("  3. API does not expose visibility (RST/EMB/PUB/LOG), so we compare")
    lines.append("     live counts against ailla2_total_items (all visibilities).")
    lines.append("  4. Multi-language items counted for each language (consistent with AILLA2).")
    lines.append("")
    lines.append("")

    # Summary statistics
    has_subject_diff = featured_df[featured_df["subject_diff"] != 0]
    has_media_diff = featured_df[featured_df["media_diff"] != 0]
    lines.append("SUMMARY")
    lines.append("-" * 40)
    lines.append(f"Featured languages analyzed: {len(featured_df)}")
    lines.append(f"Languages with subject_diff != 0: {len(has_subject_diff)}")
    lines.append(f"Languages with media_diff != 0: {len(has_media_diff)}")
    lines.append("")

    # Detailed per-language table
    lines.append("")
    lines.append("FEATURED LANGUAGES: DETAILED COMPARISON")
    lines.append("-" * 78)
    lines.append("")

    # Column header
    header = (
        f"{'Language':<28} {'Subj':>5} {'Media':>5} {'AILLA2':>6} "
        f"{'S.Diff':>6} {'M.Diff':>6} {'Coll':>4} {'CSV':>4} {'C.Diff':>6}"
    )
    lines.append(header)
    lines.append("-" * len(header))

    for _, row in featured_df.iterrows():
        name = str(row["language_name"])[:27]
        coll_str = str(int(row["live_collections"])) if row["live_collections"] != "" and row["live_collections"] >= 0 else "ERR"
        cdiff_str = str(int(row["collection_diff"])) if row["collection_diff"] != "" else "N/A"
        line = (
            f"{name:<28} {int(row['live_subject_items']):>5} {int(row['live_media_items']):>5} "
            f"{int(row['ailla2_total_items']):>6} "
            f"{int(row['subject_diff']):>+6} {int(row['media_diff']):>+6} "
            f"{coll_str:>4} {int(row['csv_collections']):>4} {cdiff_str:>6}"
        )
        lines.append(line)

    lines.append("")
    lines.append("Column key:")
    lines.append("  Subj     = live_subject_items (API subject_languages count)")
    lines.append("  Media    = live_media_items (API media_languages count, excl. meta)")
    lines.append("  AILLA2   = ailla2_total_items (pre-migration total)")
    lines.append("  S.Diff   = subject_diff (Subj - AILLA2)")
    lines.append("  M.Diff   = media_diff (Media - AILLA2)")
    lines.append("  Coll     = live_collections (API /languages/{id}/collections)")
    lines.append("  CSV      = csv_collections (languages_dataset.csv)")
    lines.append("  C.Diff   = collection_diff (Coll - CSV)")

    # Flag large discrepancies
    large_diffs = featured_df[featured_df["subject_diff"].abs() > 50].sort_values(
        "subject_diff", ascending=False
    )
    if not large_diffs.empty:
        lines.append("")
        lines.append("")
        lines.append("NOTABLE DISCREPANCIES (|subject_diff| > 50)")
        lines.append("-" * 78)
        for _, row in large_diffs.iterrows():
            pct = (
                row["subject_diff"] / row["ailla2_total_items"] * 100
                if row["ailla2_total_items"] > 0
                else float("inf")
            )
            lines.append(
                f"  {row['language_name']} (id={int(row['language_id'])}): "
                f"live={int(row['live_subject_items'])}, ailla2={int(row['ailla2_total_items'])}, "
                f"diff={int(row['subject_diff']):+d} ({pct:+.0f}%)"
            )

    lines.append("")
    lines.append("=" * 78)
    lines.append(f"Full data: {COUNTS_CSV}")
    lines.append(f"Raw extract: {EXTRACT_FILE}")
    lines.append("=" * 78)

    report_text = "\n".join(lines)

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(report_text)
    print(f"Saved: {REPORT_FILE}")

    # Print report to console
    print("\n" + report_text)


def main() -> None:
    """Run the live AILLA item count scraper."""
    parser = argparse.ArgumentParser(
        description="Scrape live AILLA item counts and compare against AILLA2 data"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume extraction from checkpoint",
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Skip scraping; regenerate report from saved extract",
    )
    args = parser.parse_args()

    print("AILLA Live Item Count Scraper")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    # Identify featured languages
    featured_ids = get_featured_language_ids()
    print(f"Featured languages: {len(featured_ids)}")

    if args.report_only:
        # Load saved extract
        if not EXTRACT_FILE.exists():
            print(f"Error: {EXTRACT_FILE} not found. Run without --report-only first.")
            return
        print(f"\nLoading saved extract from {EXTRACT_FILE}...")
        with open(EXTRACT_FILE, encoding="utf-8") as f:
            extract_data = json.load(f)
        items = extract_data["items"]
        print(f"  Loaded {len(items)} items")
    else:
        # Set up session and fetch items
        session = requests.Session()
        session.headers.update(HEADERS)

        items = fetch_all_items(session, resume=args.resume)

        # Save extract
        extract_data = {
            "extracted_at": datetime.now().isoformat(),
            "total_items": len(items),
            "api_base": BASE_URL,
            "items": items,
        }
        with open(EXTRACT_FILE, "w", encoding="utf-8") as f:
            json.dump(extract_data, f)
        print(f"Saved extract: {EXTRACT_FILE} ({len(items)} items)")

    # Count items per language
    print("\nCounting items per language...")
    lang_counts = count_items_per_language(items)
    print(f"  Languages with >0 subject items: {sum(1 for v in lang_counts.values() if v['subject_items'] > 0)}")
    print(f"  Languages with >0 media items: {sum(1 for v in lang_counts.values() if v['media_items'] > 0)}")

    # Fetch collection counts for featured languages
    if args.report_only:
        # Try loading from CSV if it exists
        collection_counts: dict[int, int] = {}
        if COUNTS_CSV.exists():
            prev = pd.read_csv(COUNTS_CSV)
            for _, row in prev.iterrows():
                if row["language_id"] in featured_ids and pd.notna(row.get("live_collections")):
                    try:
                        collection_counts[int(row["language_id"])] = int(row["live_collections"])
                    except (ValueError, TypeError):
                        pass
        if not collection_counts:
            print("  No cached collection counts found; skipping collection comparison.")
    else:
        session_for_colls = requests.Session()
        session_for_colls.headers.update(HEADERS)
        collection_counts = fetch_collection_counts(session_for_colls, featured_ids)

    # Generate report
    generate_report(lang_counts, collection_counts, featured_ids)

    print("\nDone.")


if __name__ == "__main__":
    main()
