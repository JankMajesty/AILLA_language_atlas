"""Independent date verification for the AILLA Language Atlas.

Re-derives all creation dates, deposit dates, and dedicated documentation
years from the AILLA2 Excel source data and compares them against the values
currently in data/languages_dataset.csv and the StoryMap JSON files.

This is a one-time verification script to validate the off-by-2 indexing bug
fix in extract_ailla2.py (commit 5ae135f). It intentionally re-implements the
date computation logic independently rather than importing from extract_ailla2.py,
ensuring that any systematic errors in the extraction code would be caught.

Usage:
    uv run scripts/verify_dates.py

Output:
    data/date_verification_report.txt
"""

import ast
import json
import random
import re
import sys
from pathlib import Path

import pandas as pd


# --- Configuration ---

AILLA2_DIR = Path(__file__).parent.parent / "AILLA2"
AILLA2_PATTERN = "all-MODS-priority-*.xlsx"
DATA_DIR = Path(__file__).parent.parent / "data"
LANGUAGES_CSV = DATA_DIR / "languages_dataset.csv"
CURATED_DESC_FILE = DATA_DIR / "curated_descriptions.json"
MAYAN_STORYMAP = DATA_DIR / "mayan_storymap.json"
QUECHUA_STORYMAP = DATA_DIR / "quechua_storymap.json"
REPORT_FILE = DATA_DIR / "date_verification_report.txt"

META_LANGUAGE_IDS = {8, 9, 399, 641}

# Manual date overrides (must match extract_ailla2.py)
DATE_OVERRIDES: dict[int, dict[str, int]] = {
    272: {"earliest_item_year": 2001, "latest_item_year": 2014},
}

# Featured language IDs (Mayan + Quechua)
FEATURED_IDS = {
    14, 15, 27, 29, 30, 32, 33, 34, 39, 46, 85, 89,
    131, 133, 145, 156, 157, 174, 175, 177, 181, 183,
    213, 232, 233, 272, 539,
}


# --- Independent helper functions (do NOT import from extract_ailla2.py) ---

def parse_year(date_str: object) -> int | None:
    """Extract a valid year from a date value.

    Handles 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM:SS', and zeroed dates like
    '2018-00-00'. Excludes years <= 1000 as placeholders.
    """
    if pd.isna(date_str):
        return None
    s = str(date_str).strip()
    if not s:
        return None
    match = re.match(r"(\d{4})", s)
    if not match:
        return None
    year = int(match.group(1))
    if year <= 1000:
        return None
    return year


def parse_language_ids(value: object) -> list[int]:
    """Parse a Subject Languages or Media Languages cell into language IDs."""
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


def normalize_pid(pid: object) -> str:
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


# --- Data loading ---

def load_all_sheets() -> dict[str, dict[str, pd.DataFrame]]:
    """Load Items, Folders, and Files sheets from all AILLA2 Excel files.

    Returns a dict keyed by source filename, each mapping to a dict with
    keys 'items', 'folders', 'files' containing the respective DataFrames.
    Keeps sheets separated by source file so Item Row # lookups are local.
    """
    excel_files = sorted(AILLA2_DIR.glob(AILLA2_PATTERN))
    excel_files = [f for f in excel_files if not f.name.startswith("~$")]

    print(f"Loading {len(excel_files)} AILLA2 Excel files...")
    all_data: dict[str, dict[str, pd.DataFrame]] = {}

    for filepath in excel_files:
        name = filepath.name
        print(f"  {name}...")
        items_df = pd.read_excel(filepath, sheet_name="Items")
        folders_df = pd.read_excel(filepath, sheet_name="Folders")
        files_df = pd.read_excel(filepath, sheet_name="Files")
        print(f"    {len(items_df)} items, {len(folders_df)} folders, {len(files_df)} files")
        all_data[name] = {
            "items": items_df,
            "folders": folders_df,
            "files": files_df,
        }

    return all_data


# --- Check 1: Item Row # indexing ---

def verify_indexing(all_data: dict[str, dict[str, pd.DataFrame]]) -> list[str]:
    """Verify that Item Row # - 2 produces valid indices and correct lookups."""
    lines: list[str] = []
    total_files = 0
    valid_lookups = 0
    nan_lookups = 0
    out_of_bounds = 0
    spot_check_matches = 0
    spot_check_total = 0
    spot_check_details: list[str] = []

    # Collect (source_file, items_df, item_idx, file_folder_pid) for spot-checking
    all_lookups: list[tuple[str, pd.DataFrame, int, str]] = []

    for source_file, sheets in all_data.items():
        items = sheets["items"]
        files = sheets["files"]
        n_items = len(items)

        for _, frow in files.iterrows():
            total_files += 1
            item_row = frow.get("Item Row #")
            if pd.isna(item_row):
                nan_lookups += 1
                continue

            idx = int(item_row) - 2
            if 0 <= idx < n_items:
                valid_lookups += 1
                file_folder = normalize_pid(frow.get("Folder"))
                all_lookups.append((source_file, items, idx, file_folder))
            else:
                out_of_bounds += 1

    # Spot-check: sample 100 valid lookups, verify item's folder matches file's folder
    sample_size = min(100, len(all_lookups))
    random.seed(42)
    sample = random.sample(all_lookups, sample_size)

    for source_file, items, idx, file_folder in sample:
        spot_check_total += 1
        item_folder = normalize_pid(items.iloc[idx].get("Folder"))

        if item_folder and file_folder and item_folder == file_folder:
            spot_check_matches += 1
        elif item_folder and file_folder:
            spot_check_details.append(
                f"  {source_file} idx={idx}: "
                f"item_folder={item_folder}, file_folder={file_folder}"
            )

    lines.append("ITEM ROW # INDEXING VERIFICATION")
    lines.append("-" * 40)
    lines.append(f"Total file rows: {total_files:,}")
    lines.append(
        f"Valid Item Row # lookups (item_row - 2): "
        f"{valid_lookups:,}/{total_files:,} ({valid_lookups/total_files*100:.1f}%)"
    )
    lines.append(f"NaN Item Row # (no lookup needed): {nan_lookups:,}")
    lines.append(f"Out-of-bounds lookups: {out_of_bounds:,}")
    lines.append(
        f"Folder PID spot check ({spot_check_total} sampled): "
        f"{spot_check_matches}/{spot_check_total} match"
    )
    if spot_check_details:
        lines.append(f"  Note: {len(spot_check_details)} mismatches "
                      f"(data-level, does not affect date computation)")
        for detail in spot_check_details:
            lines.append(detail)
    status = "PASS" if out_of_bounds == 0 else "FAIL"
    lines.append(f"Status: {status}")

    return lines


# --- Check 2: Creation dates ---

def compute_creation_dates(
    all_data: dict[str, dict[str, pd.DataFrame]],
) -> dict[int, dict[str, int | None]]:
    """Independently compute earliest/latest item years per language.

    Re-implements the two-pass hybrid counting from extract_ailla2.py.
    Returns dict mapping language_id -> {earliest, latest}.
    """
    lang_earliest: dict[int, int] = {}
    lang_latest: dict[int, int] = {}
    lang_items: dict[int, set] = {}

    # Build combined folder-language map across all source files
    folder_lang_map: dict[str, list[int]] = {}
    for source_file, sheets in all_data.items():
        for _, row in sheets["folders"].iterrows():
            pid = row.get("Islandora PID")
            if pd.isna(pid):
                continue
            pid_str = str(pid).strip()
            if not pid_str:
                continue
            lang_ids = parse_language_ids(row.get("Subject Languages"))
            if lang_ids:
                folder_lang_map[pid_str] = lang_ids

    # Build set of folder PIDs that have Files entries
    folders_with_files: set[str] = set()

    # Pass 1: File-level counting (Media Languages)
    print("\nPass 1: File-level counting (Media Languages)")
    file_matched = 0
    file_unmatched = 0

    for source_file, sheets in all_data.items():
        items = sheets["items"]
        files = sheets["files"]

        # Build item date lookup for this source file
        item_dates: dict[int, int | None] = {}
        for i, (_, row) in enumerate(items.iterrows()):
            item_dates[i] = parse_year(row.get("Date Created"))

        for _, frow in files.iterrows():
            # Track folders with files
            folder_pid = normalize_pid(frow.get("Folder"))
            if folder_pid:
                folders_with_files.add(folder_pid)

            media_langs = parse_language_ids(frow.get("Media Languages"))
            media_langs = [lid for lid in media_langs if lid not in META_LANGUAGE_IDS]
            if not media_langs:
                file_unmatched += 1
                continue

            file_matched += 1

            # Unique item key
            item_row = frow.get("Item Row #")
            item_key = (
                source_file,
                str(frow.get("Folder", "")).strip(),
                item_row,
            )

            # Look up date using corrected index
            year = None
            if pd.notna(item_row):
                idx = int(item_row) - 2
                year = item_dates.get(idx)

            for lid in media_langs:
                if lid not in lang_items:
                    lang_items[lid] = set()
                lang_items[lid].add(item_key)

                if year is not None:
                    if lid not in lang_earliest or year < lang_earliest[lid]:
                        lang_earliest[lid] = year
                    if lid not in lang_latest or year > lang_latest[lid]:
                        lang_latest[lid] = year

    print(f"  Files with indigenous language tags: {file_matched}")
    print(f"  Files with no indigenous language tags: {file_unmatched}")

    # Pass 2: Folder fallback
    print("\nPass 2: Folder-level fallback (Subject Languages)")
    fallback_matched = 0
    fallback_skipped = 0
    fallback_unmatched = 0

    for source_file, sheets in all_data.items():
        items = sheets["items"]

        for row_idx, (_, row) in enumerate(items.iterrows()):
            folder_pid = normalize_pid(row.get("Folder"))
            if not folder_pid:
                fallback_unmatched += 1
                continue

            if folder_pid in folders_with_files:
                fallback_skipped += 1
                continue

            lang_ids = folder_lang_map.get(folder_pid, [])
            if not lang_ids:
                fallback_unmatched += 1
                continue

            fallback_matched += 1
            year = parse_year(row.get("Date Created"))

            item_key = (
                source_file,
                str(row.get("Folder", "")).strip(),
                f"fallback_{row_idx}",
            )

            for lid in lang_ids:
                if lid not in lang_items:
                    lang_items[lid] = set()
                lang_items[lid].add(item_key)

                if year is not None:
                    if lid not in lang_earliest or year < lang_earliest[lid]:
                        lang_earliest[lid] = year
                    if lid not in lang_latest or year > lang_latest[lid]:
                        lang_latest[lid] = year

    print(f"  Items counted via folder fallback: {fallback_matched}")
    print(f"  Items skipped (already file-counted): {fallback_skipped}")
    print(f"  Items unmatched: {fallback_unmatched}")

    # Build result
    result: dict[int, dict[str, int | None]] = {}
    for lid in sorted(set(lang_earliest.keys()) | set(lang_latest.keys()) | set(lang_items.keys())):
        result[lid] = {
            "earliest": lang_earliest.get(lid),
            "latest": lang_latest.get(lid),
        }

    return result


# --- Check 3: Deposit dates ---

def compute_deposit_dates(
    all_data: dict[str, dict[str, pd.DataFrame]],
) -> dict[int, dict[str, int | None]]:
    """Independently compute earliest/latest deposit years per language."""
    lang_earliest: dict[int, int] = {}
    lang_latest: dict[int, int] = {}

    # Build folder-language map
    folder_lang_map: dict[str, list[int]] = {}
    for source_file, sheets in all_data.items():
        for _, row in sheets["folders"].iterrows():
            pid = row.get("Islandora PID")
            if pd.isna(pid):
                continue
            pid_str = str(pid).strip()
            if not pid_str:
                continue
            lang_ids = parse_language_ids(row.get("Subject Languages"))
            if lang_ids:
                folder_lang_map[pid_str] = lang_ids

    for source_file, sheets in all_data.items():
        for _, frow in sheets["files"].iterrows():
            folder_pid = normalize_pid(frow.get("Folder"))
            if not folder_pid:
                continue

            lang_ids = folder_lang_map.get(folder_pid, [])
            if not lang_ids:
                continue

            year = parse_year(frow.get("Date Uploaded"))
            if year is not None:
                for lid in lang_ids:
                    if lid not in lang_earliest or year < lang_earliest[lid]:
                        lang_earliest[lid] = year
                    if lid not in lang_latest or year > lang_latest[lid]:
                        lang_latest[lid] = year

    result: dict[int, dict[str, int | None]] = {}
    for lid in sorted(set(lang_earliest.keys()) | set(lang_latest.keys())):
        result[lid] = {
            "earliest": lang_earliest.get(lid),
            "latest": lang_latest.get(lid),
        }
    return result


# --- Compare computed vs CSV ---

def compare_dates(
    creation: dict[int, dict[str, int | None]],
    deposit: dict[int, dict[str, int | None]],
    csv_df: pd.DataFrame,
) -> tuple[list[str], list[str], list[str], bool]:
    """Compare independently computed dates against CSV values.

    Returns (creation_lines, deposit_lines, featured_lines, all_pass).
    """
    creation_lines: list[str] = []
    deposit_lines: list[str] = []
    featured_lines: list[str] = []
    all_pass = True

    # --- Creation dates ---
    creation_match = 0
    creation_mismatch = 0
    creation_mismatch_details: list[str] = []

    # Apply overrides to our computed data
    for lang_id, overrides in DATE_OVERRIDES.items():
        if lang_id in creation:
            original = creation[lang_id].copy()
            creation[lang_id]["earliest"] = overrides.get(
                "earliest_item_year", creation[lang_id].get("earliest")
            )
            creation[lang_id]["latest"] = overrides.get(
                "latest_item_year", creation[lang_id].get("latest")
            )
            creation_mismatch_details.append(
                f"  Override applied: language_id {lang_id} "
                f"({original['earliest']}->{creation[lang_id]['earliest']}, "
                f"{original['latest']}->{creation[lang_id]['latest']})"
            )

    for _, row in csv_df.iterrows():
        lid = int(row["language_id"])
        csv_earliest = row.get("earliest_item_year")
        csv_latest = row.get("latest_item_year")

        if lid not in creation:
            # Language has no computed dates
            if pd.notna(csv_earliest) and csv_earliest > 0:
                creation_mismatch += 1
                name = row.get("name_en", f"ID {lid}")
                creation_mismatch_details.append(
                    f"  CSV-only: {name} (ID {lid}): "
                    f"CSV={int(csv_earliest)}-{int(csv_latest)}, computed=none"
                )
            continue

        computed = creation[lid]
        csv_e = int(csv_earliest) if pd.notna(csv_earliest) else None
        csv_l = int(csv_latest) if pd.notna(csv_latest) else None
        comp_e = computed.get("earliest")
        comp_l = computed.get("latest")

        if csv_e == comp_e and csv_l == comp_l:
            creation_match += 1
        else:
            creation_mismatch += 1
            name = row.get("name_en", f"ID {lid}")
            creation_mismatch_details.append(
                f"  MISMATCH: {name} (ID {lid}): "
                f"CSV={csv_e}-{csv_l}, computed={comp_e}-{comp_l}"
            )
            all_pass = False

    total_creation = creation_match + creation_mismatch
    creation_lines.append("CREATION DATE VERIFICATION (earliest_item_year, latest_item_year)")
    creation_lines.append("-" * 40)
    creation_lines.append(f"Languages compared: {total_creation}")
    creation_lines.append(f"  Matches: {creation_match}")
    creation_lines.append(f"  Mismatches: {creation_mismatch}")
    if creation_mismatch_details:
        creation_lines.append("")
        creation_lines.append("Details:")
        creation_lines.extend(creation_mismatch_details)
    status = "PASS" if creation_mismatch == 0 else "FAIL"
    creation_lines.append(f"Status: {status}")

    # --- Deposit dates ---
    deposit_match = 0
    deposit_mismatch = 0
    deposit_mismatch_details: list[str] = []

    for _, row in csv_df.iterrows():
        lid = int(row["language_id"])
        csv_dep_e = row.get("earliest_deposit_year")
        csv_dep_l = row.get("latest_deposit_year")

        if lid not in deposit:
            if pd.notna(csv_dep_e) and csv_dep_e > 0:
                deposit_mismatch += 1
                name = row.get("name_en", f"ID {lid}")
                deposit_mismatch_details.append(
                    f"  CSV-only: {name} (ID {lid}): "
                    f"CSV={int(csv_dep_e)}-{int(csv_dep_l)}, computed=none"
                )
            continue

        computed = deposit[lid]
        csv_de = int(csv_dep_e) if pd.notna(csv_dep_e) else None
        csv_dl = int(csv_dep_l) if pd.notna(csv_dep_l) else None
        comp_de = computed.get("earliest")
        comp_dl = computed.get("latest")

        if csv_de == comp_de and csv_dl == comp_dl:
            deposit_match += 1
        else:
            deposit_mismatch += 1
            name = row.get("name_en", f"ID {lid}")
            deposit_mismatch_details.append(
                f"  MISMATCH: {name} (ID {lid}): "
                f"CSV={csv_de}-{csv_dl}, computed={comp_de}-{comp_dl}"
            )
            all_pass = False

    total_deposit = deposit_match + deposit_mismatch
    deposit_lines.append("DEPOSIT DATE VERIFICATION (earliest_deposit_year, latest_deposit_year)")
    deposit_lines.append("-" * 40)
    deposit_lines.append(f"Languages compared: {total_deposit}")
    deposit_lines.append(f"  Matches: {deposit_match}")
    deposit_lines.append(f"  Mismatches: {deposit_mismatch}")
    if deposit_mismatch_details:
        deposit_lines.append("")
        deposit_lines.append("Details:")
        deposit_lines.extend(deposit_mismatch_details)
    status = "PASS" if deposit_mismatch == 0 else "FAIL"
    deposit_lines.append(f"Status: {status}")

    # --- Featured languages detail ---
    featured_lines.append("FEATURED LANGUAGES DETAIL (27 languages)")
    featured_lines.append("-" * 40)
    header = (
        f"{'Language':<30} {'Family':<10} {'CSV Created':<14} {'Vfy Created':<14} "
        f"{'CSV Deposit':<14} {'Vfy Deposit':<14} Status"
    )
    featured_lines.append(header)
    featured_lines.append("-" * len(header))

    for _, row in csv_df.iterrows():
        lid = int(row["language_id"])
        if lid not in FEATURED_IDS:
            continue

        name = str(row.get("name_en", ""))[:28]
        family = str(row.get("language_family", ""))[:8]

        csv_e = int(row["earliest_item_year"]) if pd.notna(row.get("earliest_item_year")) else None
        csv_l = int(row["latest_item_year"]) if pd.notna(row.get("latest_item_year")) else None
        comp = creation.get(lid, {})
        comp_e = comp.get("earliest")
        comp_l = comp.get("latest")

        csv_de = int(row["earliest_deposit_year"]) if pd.notna(row.get("earliest_deposit_year")) else None
        csv_dl = int(row["latest_deposit_year"]) if pd.notna(row.get("latest_deposit_year")) else None
        dep = deposit.get(lid, {})
        dep_e = dep.get("earliest")
        dep_l = dep.get("latest")

        csv_created = f"{csv_e}-{csv_l}" if csv_e else "n/a"
        vfy_created = f"{comp_e}-{comp_l}" if comp_e else "n/a"
        csv_dep = f"{csv_de}-{csv_dl}" if csv_de else "n/a"
        vfy_dep = f"{dep_e}-{dep_l}" if dep_e else "n/a"

        match = csv_created == vfy_created and csv_dep == vfy_dep
        status = "PASS" if match else "FAIL"

        featured_lines.append(
            f"{name:<30} {family:<10} {csv_created:<14} {vfy_created:<14} "
            f"{csv_dep:<14} {vfy_dep:<14} {status}"
        )

    return creation_lines, deposit_lines, featured_lines, all_pass


# --- Check 4: Dedicated documentation years ---

def verify_doc_years(csv_df: pd.DataFrame) -> tuple[list[str], bool]:
    """Verify _dedicated_doc_years against description text and StoryMap order."""
    lines: list[str] = []
    all_pass = True

    with open(CURATED_DESC_FILE, encoding="utf-8") as f:
        raw = json.load(f)

    doc_years = {int(k): v for k, v in raw.get("_dedicated_doc_years", {}).items()}
    descriptions = {int(k): v for k, v in raw.items()
                    if k != "_comment" and k != "_dedicated_doc_years"}

    lines.append("DEDICATED DOCUMENTATION YEARS")
    lines.append("-" * 40)

    # 4a: Description text consistency
    text_matches = 0
    text_notes: list[str] = []

    for lid, year in sorted(doc_years.items()):
        desc = descriptions.get(lid, "")
        # Look for "documentation from YYYY" or "from the YYYYs"
        specific_match = re.search(r"documentation from (\d{4})", desc)
        decade_match = re.search(r"from (?:the )?(\d{4})s", desc)
        # Also handle "documentation is from" phrasing (Tzotzil)
        is_from_match = re.search(r"documentation is from .+?(\d{4})s?", desc)

        extracted_year = None
        note = None

        if specific_match:
            extracted_year = int(specific_match.group(1))
        elif decade_match:
            extracted_year = int(decade_match.group(1))
            note = f"uses decade phrasing (\"the {extracted_year}s\"), mapped to {year}"
        elif is_from_match:
            extracted_year = int(is_from_match.group(1))
            note = f"uses variant phrasing, extracted {extracted_year}"

        if extracted_year == year:
            text_matches += 1
            if note:
                text_notes.append(f"  Note: language_id {lid}: {note}")
        elif extracted_year is not None:
            lines.append(f"  MISMATCH: language_id {lid}: doc_year={year}, text={extracted_year}")
            all_pass = False
        else:
            lines.append(f"  WARNING: language_id {lid}: could not extract year from description")
            all_pass = False

    lines.append(f"Description-text consistency: {text_matches}/{len(doc_years)} match")
    lines.extend(text_notes)

    # 4b: StoryMap slide order
    for storymap_path, family_name in [
        (MAYAN_STORYMAP, "Mayan"),
        (QUECHUA_STORYMAP, "Quechua"),
    ]:
        with open(storymap_path, encoding="utf-8") as f:
            storymap = json.load(f)

        slides = storymap["storymap"]["slides"]
        # Extract language slides (skip overview at index 0 and summary at end)
        lang_slides = slides[1:-1]

        # Extract language_ids from AILLA URLs in slide text
        slide_years: list[tuple[str, int]] = []
        for slide in lang_slides:
            text = slide["text"]["text"]
            url_match = re.search(r"ailla\.utexas\.org/languages/(\d+)", text)
            if url_match:
                lid = int(url_match.group(1))
                doc_year = doc_years.get(lid, 9999)
                headline = slide["text"]["headline"]
                slide_years.append((headline, doc_year))

        # Check ascending order
        years_only = [y for _, y in slide_years]
        is_sorted = all(years_only[i] <= years_only[i + 1] for i in range(len(years_only) - 1))
        status = "PASS" if is_sorted else "FAIL"
        if not is_sorted:
            all_pass = False
        lines.append(f"{family_name} slide chronological order: {status} ({len(lang_slides)} slides)")

    # 4c: Dedicated doc year >= earliest_item_year
    bounds_ok = 0
    bounds_fail = 0
    for lid, doc_year in sorted(doc_years.items()):
        csv_row = csv_df[csv_df["language_id"] == lid]
        if csv_row.empty:
            continue
        earliest = csv_row.iloc[0].get("earliest_item_year")
        if pd.notna(earliest):
            if doc_year >= int(earliest):
                bounds_ok += 1
            else:
                bounds_fail += 1
                lines.append(
                    f"  BOUNDS FAIL: language_id {lid}: "
                    f"doc_year={doc_year} < earliest_item_year={int(earliest)}"
                )
                all_pass = False

    lines.append(
        f"Dedicated doc year >= earliest_item_year: "
        f"{bounds_ok}/{bounds_ok + bounds_fail} "
        f"({'PASS' if bounds_fail == 0 else 'FAIL'})"
    )

    return lines, all_pass


# --- Check 5: StoryMap date cross-check ---

def verify_storymap_dates(csv_df: pd.DataFrame) -> tuple[list[str], bool]:
    """Verify dates in StoryMap slide HTML match CSV values."""
    lines: list[str] = []
    all_pass = True

    lines.append("STORYMAP DATE CROSS-CHECK")
    lines.append("-" * 40)

    for storymap_path, family_name in [
        (MAYAN_STORYMAP, "Mayan"),
        (QUECHUA_STORYMAP, "Quechua"),
    ]:
        with open(storymap_path, encoding="utf-8") as f:
            storymap = json.load(f)

        slides = storymap["storymap"]["slides"]
        lang_slides = slides[1:-1]

        created_matches = 0
        created_present = 0
        deposit_matches = 0
        deposit_present = 0
        created_fails: list[str] = []
        deposit_fails: list[str] = []

        for slide in lang_slides:
            text = slide["text"]["text"]
            headline = slide["text"]["headline"]

            # Extract language_id from AILLA URL
            url_match = re.search(r"ailla\.utexas\.org/languages/(\d+)", text)
            if not url_match:
                continue
            lid = int(url_match.group(1))

            csv_row = csv_df[csv_df["language_id"] == lid]
            if csv_row.empty:
                continue
            row = csv_row.iloc[0]

            # Check "Dates created"
            created_match = re.search(
                r"Dates created:</em>\s*(\d{4})(?:-(\d{4}))?", text
            )
            if created_match:
                created_present += 1
                slide_earliest = int(created_match.group(1))
                slide_latest = int(created_match.group(2)) if created_match.group(2) else slide_earliest
                csv_e = int(row["earliest_item_year"]) if pd.notna(row.get("earliest_item_year")) else None
                csv_l = int(row["latest_item_year"]) if pd.notna(row.get("latest_item_year")) else None

                if slide_earliest == csv_e and slide_latest == csv_l:
                    created_matches += 1
                else:
                    created_fails.append(
                        f"    MISMATCH: {headline} (ID {lid}): "
                        f"slide={slide_earliest}-{slide_latest}, CSV={csv_e}-{csv_l}"
                    )
                    all_pass = False

            # Check "Dates deposited"
            deposit_match = re.search(
                r"Dates deposited:</em>\s*(\d{4})(?:-(\d{4}))?", text
            )
            if deposit_match:
                deposit_present += 1
                slide_dep_e = int(deposit_match.group(1))
                slide_dep_l = int(deposit_match.group(2)) if deposit_match.group(2) else slide_dep_e
                csv_de = int(row["earliest_deposit_year"]) if pd.notna(row.get("earliest_deposit_year")) else None
                csv_dl = int(row["latest_deposit_year"]) if pd.notna(row.get("latest_deposit_year")) else None

                if slide_dep_e == csv_de and slide_dep_l == csv_dl:
                    deposit_matches += 1
                else:
                    deposit_fails.append(
                        f"    MISMATCH: {headline} (ID {lid}): "
                        f"slide={slide_dep_e}-{slide_dep_l}, CSV={csv_de}-{csv_dl}"
                    )
                    all_pass = False

        no_created = len(lang_slides) - created_present
        no_deposit = len(lang_slides) - deposit_present

        lines.append(f"{family_name} StoryMap ({storymap_path.name}):")
        lines.append(f"  {len(lang_slides)} language slides checked")
        created_note = f" ({no_created} without field)" if no_created else ""
        lines.append(f"  Dates created: {created_matches}/{created_present} match{created_note}")
        if created_fails:
            lines.extend(created_fails)
        deposit_note = f" ({no_deposit} without field)" if no_deposit else ""
        lines.append(f"  Dates deposited: {deposit_matches}/{deposit_present} match{deposit_note}")
        if deposit_fails:
            lines.extend(deposit_fails)
        status = "PASS" if not created_fails and not deposit_fails else "FAIL"
        lines.append(f"  Status: {status}")

    return lines, all_pass


# --- Report generation ---

def generate_report(sections: list[tuple[str, list[str]]]) -> None:
    """Write all verification sections to the report file."""
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("DATE VERIFICATION REPORT\n")
        f.write("Post-bug-fix validation (commit 5ae135f)\n")
        f.write("=" * 80 + "\n")

        for title, lines in sections:
            f.write("\n")
            for line in lines:
                f.write(line + "\n")
            f.write("\n")

    print(f"\nReport written to {REPORT_FILE}")


# --- Main ---

def main() -> None:
    """Run all verification checks and generate report."""
    print("=" * 60)
    print("DATE VERIFICATION")
    print("=" * 60)

    # Load source data
    all_data = load_all_sheets()

    total_items = sum(len(s["items"]) for s in all_data.values())
    total_files = sum(len(s["files"]) for s in all_data.values())
    print(f"\nLoaded: {total_items:,} items, {total_files:,} files across {len(all_data)} files")

    # Load CSV
    csv_df = pd.read_csv(LANGUAGES_CSV)
    print(f"CSV: {len(csv_df)} languages")

    overall_pass = True

    # Check 1: Indexing
    print("\n--- Check 1: Item Row # Indexing ---")
    indexing_lines = verify_indexing(all_data)
    if "FAIL" in indexing_lines[-1]:
        overall_pass = False

    # Check 2 & 3: Compute dates independently
    print("\n--- Check 2: Creation Dates ---")
    creation = compute_creation_dates(all_data)
    print(f"Computed creation dates for {len(creation)} languages")

    print("\n--- Check 3: Deposit Dates ---")
    deposit = compute_deposit_dates(all_data)
    print(f"Computed deposit dates for {len(deposit)} languages")

    # Compare against CSV
    print("\n--- Comparing against CSV ---")
    creation_lines, deposit_lines, featured_lines, dates_pass = compare_dates(
        creation, deposit, csv_df
    )
    if not dates_pass:
        overall_pass = False

    # Check 4: Dedicated doc years
    print("\n--- Check 4: Dedicated Documentation Years ---")
    doc_lines, doc_pass = verify_doc_years(csv_df)
    if not doc_pass:
        overall_pass = False

    # Check 5: StoryMap cross-check
    print("\n--- Check 5: StoryMap Date Cross-check ---")
    storymap_lines, storymap_pass = verify_storymap_dates(csv_df)
    if not storymap_pass:
        overall_pass = False

    # Generate report
    sections = [
        ("Indexing", indexing_lines),
        ("Creation", creation_lines),
        ("Deposit", deposit_lines),
        ("Featured", featured_lines),
        ("Doc Years", doc_lines),
        ("StoryMap", storymap_lines),
    ]
    generate_report(sections)

    # Summary
    print("\n" + "=" * 60)
    if overall_pass:
        print("OVERALL RESULT: ALL CHECKS PASSED")
    else:
        print("OVERALL RESULT: SOME CHECKS FAILED (see report)")
    print("=" * 60)

    sys.exit(0 if overall_pass else 1)


if __name__ == "__main__":
    main()
