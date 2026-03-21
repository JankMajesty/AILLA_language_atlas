#!/usr/bin/env python3
"""Generate description source material for featured StoryMap languages.

Queries AILLA2 spreadsheets to produce a per-language reference table showing
collections, collectors, genre breakdowns, date ranges, and AILLA staff
descriptions. Output is a readable Markdown file for use when writing
curated slide descriptions.

Only uses verifiable data from the AILLA2 spreadsheets. Does not include
speaker populations, endangerment status, or any external data.

Usage:
    uv run scripts/description_source.py

Output:
    data/description_source_material.md
"""

import ast
import re
from pathlib import Path

import pandas as pd


# --- Configuration ---

AILLA2_DIR = Path("AILLA2")
AILLA2_PATTERN = "all-MODS-priority-*.xlsx"
DATA_DIR = Path("data")
LANGUAGES_CSV = DATA_DIR / "languages_dataset.csv"
OUTPUT_FILE = DATA_DIR / "description_source_material.md"

# Featured language thresholds (must match build_storymaps.py)
THRESHOLDS = {
    "Mayan": 100,
    "Quechua": 5,
}


def parse_list_field(value: object) -> list[str]:
    """Parse a string representation of a list into a list of strings.

    Handles formats like:
      - '[Narrative, Song]' (unquoted strings, common in AILLA2)
      - "['Narrative', 'Song']" (quoted strings)
      - '[39, 175]' (integers)
      - NaN

    Args:
        value: Cell value (string, list, or NaN).

    Returns:
        List of strings, or empty list if unparseable.
    """
    if pd.isna(value):
        return []
    s = str(value).strip()
    if not s or s == "[]":
        return []

    # Try ast.literal_eval first (handles quoted strings and numbers)
    try:
        parsed = ast.literal_eval(s)
        if isinstance(parsed, list):
            return [str(x).strip() for x in parsed if x is not None]
        return [str(parsed).strip()]
    except (ValueError, SyntaxError):
        pass

    # Fallback: strip brackets and split on comma (for unquoted strings)
    if s.startswith("[") and s.endswith("]"):
        inner = s[1:-1].strip()
        if not inner:
            return []
        return [item.strip() for item in inner.split(",") if item.strip()]

    return []


def parse_int_list(value: object) -> list[int]:
    """Parse a string list into integer IDs."""
    if pd.isna(value):
        return []
    s = str(value).strip()
    if not s or s == "[]":
        return []
    try:
        parsed = ast.literal_eval(s)
        if isinstance(parsed, list):
            return [int(x) for x in parsed if x is not None]
        if isinstance(parsed, (int, float)):
            return [int(parsed)]
    except (ValueError, SyntaxError):
        pass
    return []


def normalize_folder_pid(pid: str) -> str:
    """Normalize a folder PID by stripping -res and numeric suffixes."""
    if pd.isna(pid):
        return ""
    s = str(pid).strip()
    if s.endswith("-res"):
        return s[:-4]
    match = re.match(r"^(ailla:\d+)-\d+$", s)
    if match:
        return match.group(1)
    return s


def parse_year(date_str: object) -> int | None:
    """Extract a valid year (> 1000) from a date value."""
    if pd.isna(date_str):
        return None
    s = str(date_str).strip()
    if not s:
        return None
    match = re.match(r"(\d{4})", s)
    if not match:
        return None
    year = int(match.group(1))
    return year if year > 1000 else None


def load_all_ailla2() -> dict[str, pd.DataFrame]:
    """Load Items, Folders, and Collection sheets from all AILLA2 files.

    Returns:
        Dict with keys 'items', 'folders', 'collections', each a combined DataFrame.
    """
    files = sorted(AILLA2_DIR.glob(AILLA2_PATTERN))
    files = [f for f in files if not f.name.startswith("~$")]
    print(f"Found {len(files)} AILLA2 files")

    all_items, all_folders, all_collections = [], [], []

    for filepath in files:
        print(f"  Reading {filepath.name}...")
        items_df = pd.read_excel(filepath, sheet_name="Items")
        folders_df = pd.read_excel(filepath, sheet_name="Folders")
        coll_df = pd.read_excel(filepath, sheet_name="Collection")

        items_df["source_file"] = filepath.name
        folders_df["source_file"] = filepath.name
        coll_df["source_file"] = filepath.name

        all_items.append(items_df)
        all_folders.append(folders_df)
        all_collections.append(coll_df)

    items = pd.concat(all_items, ignore_index=True)
    folders = pd.concat(all_folders, ignore_index=True)
    collections = pd.concat(all_collections, ignore_index=True)

    # Include all items regardless of visibility (RST, EMB, PUB, LOG)
    # Restricted metadata is visible on the AILLA website
    print(f"\nTotal items (all visibilities): {len(items)}")
    print(f"Total folders: {len(folders)}")
    print(f"Total collections: {len(collections)}")

    return {"items": items, "folders": folders, "collections": collections}


def build_folder_language_map(folders: pd.DataFrame) -> dict[str, list[int]]:
    """Map folder PID -> list of language IDs from Subject Languages column."""
    folder_map: dict[str, list[int]] = {}
    for _, row in folders.iterrows():
        pid = row.get("Islandora PID")
        if pd.isna(pid):
            continue
        pid_str = str(pid).strip()
        if not pid_str:
            continue
        lang_ids = parse_int_list(row.get("Subject Languages"))
        if lang_ids:
            folder_map[pid_str] = lang_ids
    return folder_map


def build_folder_collection_map(folders: pd.DataFrame) -> dict[str, str]:
    """Map folder PID -> collection title."""
    folder_coll: dict[str, str] = {}
    for _, row in folders.iterrows():
        pid = row.get("Islandora PID")
        coll = row.get("Collection")
        if pd.isna(pid) or pd.isna(coll):
            continue
        folder_coll[str(pid).strip()] = str(coll).strip()
    return folder_coll


def get_collection_metadata(collections: pd.DataFrame) -> dict[str, dict]:
    """Build a lookup from collection title to metadata (description, collectors)."""
    coll_meta: dict[str, dict] = {}
    for _, row in collections.iterrows():
        title = row.get("Collection Title EN")
        if pd.isna(title):
            continue
        title = str(title).strip()
        desc = row.get("Description EN", "")
        desc = str(desc).strip() if pd.notna(desc) else ""
        coll_meta[title] = {"description": desc}
    return coll_meta


def generate_language_profile(
    lang_id: int,
    lang_row: pd.Series,
    items: pd.DataFrame,
    folder_lang_map: dict[str, list[int]],
    folder_coll_map: dict[str, str],
    coll_meta: dict[str, dict],
) -> str:
    """Generate a Markdown profile for one featured language.

    Args:
        lang_id: The AILLA language ID.
        lang_row: Row from languages_dataset.csv.
        items: All public items from AILLA2.
        folder_lang_map: Folder PID -> language IDs.
        folder_coll_map: Folder PID -> collection title.
        coll_meta: Collection title -> metadata dict.

    Returns:
        Markdown-formatted profile string.
    """
    name = lang_row["name_en"]
    family = lang_row.get("language_family", "")
    iso = lang_row.get("iso_639_3_code", "")

    # Find all folder PIDs associated with this language
    lang_folder_pids = set()
    for pid, lids in folder_lang_map.items():
        if lang_id in lids:
            lang_folder_pids.add(pid)

    # Find all items in those folders
    lang_items = items[
        items["Folder"].apply(lambda f: normalize_folder_pid(f) in lang_folder_pids)
    ].copy()

    lines = []
    lines.append(f"### {name}")
    lines.append("")
    lines.append(f"- **Language ID:** {lang_id}")
    if pd.notna(iso) and str(iso).strip():
        lines.append(f"- **ISO 639-3:** {iso}")
    lines.append(f"- **Family:** {family}")

    # Countries
    countries = lang_row.get("countries", "")
    if pd.notna(countries) and str(countries).strip():
        lines.append(f"- **Countries:** {countries}")

    # Indigenous/alternative names
    indigenous = lang_row.get("indigenous_name", "")
    if pd.notna(indigenous) and str(indigenous).strip():
        lines.append(f"- **Indigenous name:** {indigenous}")
    alt = lang_row.get("alternative_name", "")
    if pd.notna(alt) and str(alt).strip():
        lines.append(f"- **Alternative names:** {alt}")

    # Item counts: total (all visibilities) and public (PUB+LOG)
    total_from_csv = lang_row.get("total_items")
    public_from_csv = lang_row.get("public_items")
    total_count = int(total_from_csv) if pd.notna(total_from_csv) else len(lang_items)
    public_count = int(public_from_csv) if pd.notna(public_from_csv) else total_count

    lines.append(f"- **Total items:** {total_count}")
    if public_count < total_count:
        lines.append(f"- **Public items (PUB+LOG):** {public_count}")
        restricted_pct = (total_count - public_count) / total_count * 100
        lines.append(f"- **Restricted/embargoed:** {restricted_pct:.0f}%")
    lines.append("")

    # AILLA staff-authored description (from languages_dataset.csv)
    desc = lang_row.get("description", "")
    if pd.notna(desc) and str(desc).strip():
        lines.append("**AILLA Language Description:**")
        lines.append(f"> {str(desc).strip()}")
        lines.append("")

    # Date range
    years = lang_items["Date Created"].apply(parse_year).dropna()
    if len(years) > 0:
        lines.append(f"**Creation Date Range:** {int(years.min())}-{int(years.max())}")
    else:
        lines.append("**Creation Date Range:** No dates available")
    lines.append("")

    # Collections associated with this language
    coll_names = set()
    for pid in lang_folder_pids:
        coll = folder_coll_map.get(pid)
        if coll:
            coll_names.add(coll)

    if coll_names:
        lines.append(f"**Collections ({len(coll_names)}):**")
        for coll_name in sorted(coll_names):
            lines.append(f"- {coll_name}")
            # Include collection description if available
            meta = coll_meta.get(coll_name, {})
            coll_desc = meta.get("description", "")
            if coll_desc:
                # Truncate long descriptions
                if len(coll_desc) > 300:
                    coll_desc = coll_desc[:297] + "..."
                lines.append(f"  > {coll_desc}")
        lines.append("")

    # Genre breakdown
    genre_col = "Genre - AILLA 2"
    if genre_col in lang_items.columns:
        all_genres: list[str] = []
        for val in lang_items[genre_col].dropna():
            all_genres.extend(parse_list_field(val))

        if all_genres:
            genre_counts = pd.Series(all_genres).value_counts()
            lines.append(f"**Genre Breakdown ({len(all_genres)} genre tags across {len(lang_items)} items):**")
            for genre, count in genre_counts.items():
                lines.append(f"- {genre}: {count}")
            lines.append("")

    # Contributor/collector info from items
    contrib_col = "Contributor Persons and Roles - AILLA 2"
    if contrib_col in lang_items.columns:
        all_contribs: list[str] = []
        for val in lang_items[contrib_col].dropna():
            all_contribs.extend(parse_list_field(val))

        if all_contribs:
            # Parse role info: format is "ailla:NNNNNN:Role" or "Name:Role"
            role_counts: dict[str, int] = {}
            for contrib in all_contribs:
                # Extract role (last colon-separated segment)
                parts = contrib.split(":")
                if len(parts) >= 2:
                    role = parts[-1].strip()
                    if role:
                        role_counts[role] = role_counts.get(role, 0) + 1

            if role_counts:
                lines.append("**Contributor Roles:**")
                for role, count in sorted(role_counts.items(), key=lambda x: -x[1]):
                    lines.append(f"- {role}: {count}")
                lines.append("")

    # Items by year (compact table for temporal distribution)
    if len(years) > 0:
        year_counts = years.astype(int).value_counts().sort_index()
        lines.append("**Items by Year:**")
        year_strs = [f"{yr}: {ct}" for yr, ct in year_counts.items()]
        # Show as compact inline list
        lines.append(", ".join(year_strs))
        lines.append("")

    lines.append("---")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    """Generate description source material for all featured languages."""
    print("=" * 60)
    print("DESCRIPTION SOURCE MATERIAL GENERATOR")
    print("=" * 60)

    # Load languages dataset
    languages = pd.read_csv(LANGUAGES_CSV)
    print(f"Loaded {len(languages)} languages from {LANGUAGES_CSV}")

    # Load AILLA2 data
    data = load_all_ailla2()
    items = data["items"]
    folders = data["folders"]
    collections = data["collections"]

    # Build lookup maps
    folder_lang_map = build_folder_language_map(folders)
    folder_coll_map = build_folder_collection_map(folders)
    coll_meta = get_collection_metadata(collections)

    print(f"\nFolder-language map: {len(folder_lang_map)} folders")
    print(f"Folder-collection map: {len(folder_coll_map)} folders")
    print(f"Collection metadata: {len(coll_meta)} collections")

    # Generate output
    output_lines = []
    output_lines.append("# AILLA Language Atlas: Description Source Material")
    output_lines.append("")
    output_lines.append(
        "Reference data for writing curated StoryMap slide descriptions. "
        "All data sourced from AILLA2 pre-migration spreadsheets. "
        "All items included regardless of visibility (PUB, LOG, RST, EMB); "
        "restricted metadata is visible on the AILLA website."
    )
    output_lines.append("")
    output_lines.append(
        "**Content policy:** Use only information verifiable from this data. "
        "Do not include speaker populations, endangerment classifications, "
        "or linguistic claims from external sources."
    )
    output_lines.append("")

    # Restricted threshold: languages 50%+ restricted are excluded from featured
    RESTRICTED_THRESHOLD = 0.50

    for family_name, threshold in THRESHOLDS.items():
        family_all = languages[languages["language_family"] == family_name].copy()

        # Compute restricted percentage
        public_col = "public_items" if "public_items" in family_all.columns else "total_items"
        family_all["restricted_pct"] = family_all.apply(
            lambda r: (r["total_items"] - r[public_col]) / r["total_items"]
            if pd.notna(r["total_items"]) and r["total_items"] > 0 else 0.0,
            axis=1,
        )

        # Dual-threshold: public_items >= threshold OR (total >= threshold AND < 50% restricted)
        qualifies_public = family_all[public_col] >= threshold
        qualifies_total = (family_all["total_items"] >= threshold) & (family_all["restricted_pct"] < RESTRICTED_THRESHOLD)
        family_langs = family_all[qualifies_public | qualifies_total].sort_values(
            "earliest_item_year", na_position="last"
        )

        output_lines.append(f"## {family_name} Family ({len(family_langs)} featured languages)")
        output_lines.append("")

        for _, lang_row in family_langs.iterrows():
            lang_id = int(lang_row["language_id"])
            print(f"\nProcessing: {lang_row['name_en']} (ID {lang_id})...")
            profile = generate_language_profile(
                lang_id, lang_row, items,
                folder_lang_map, folder_coll_map, coll_meta,
            )
            output_lines.append(profile)

    output_text = "\n".join(output_lines)
    OUTPUT_FILE.write_text(output_text, encoding="utf-8")
    print(f"\nOutput saved to {OUTPUT_FILE}")
    print(f"Total size: {len(output_text):,} characters")


if __name__ == "__main__":
    main()
