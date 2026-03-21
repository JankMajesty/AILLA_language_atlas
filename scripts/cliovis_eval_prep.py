"""
ClioVis Evaluation Prototype: Data Extraction and StoryMapJS Generation

Extracts a test dataset for the Tukanoan language family from AILLA data
and generates:
  1. A markdown reference sheet for manual ClioVis entry
  2. A StoryMapJS JSON file for side-by-side platform comparison

The Tukanoan family was selected for this evaluation because it has:
  - 19 languages (ideal size for manual ClioVis entry)
  - 4 countries (Colombia, Brazil, Peru, Ecuador)
  - Strong geographic concentration in the Vaupés region with outliers
  - 68% of languages have sociolinguistic descriptions
  - 39 total AILLA collections

Usage:
    uv run scripts/cliovis_eval_prep.py
"""

import json
import re
from pathlib import Path

import pandas as pd


# Approximate geographic coordinates for Tukanoan languages.
# These are based on the known linguistic geography of the Vaupés region
# and published ethnolinguistic sources. Most Eastern Tukanoan languages
# are concentrated along river systems in the Colombia-Brazil border area.
# Coordinates represent the approximate center of each language's
# traditional territory, not precise community locations.
TUKANOAN_COORDINATES = {
    "Arapaso":          {"lat":  0.50, "lon": -69.50, "note": "Vaupés region, Brazil/Colombia border"},
    "Bará":             {"lat":  0.45, "lon": -70.00, "note": "Upper Pira-Paraná, Papurí, and Tiquié rivers"},
    "Barasana-Eduria":  {"lat":  0.40, "lon": -70.50, "note": "Pira-Paraná drainage, Vaupés, Colombia"},
    "Carapana":         {"lat":  0.50, "lon": -70.10, "note": "Vaupés region, Colombia/Brazil border"},
    "Cubeo":            {"lat":  1.05, "lon": -70.50, "note": "Vaupés and Cuduyarí rivers, Colombia"},
    "Desano":           {"lat":  0.30, "lon": -69.80, "note": "Upper Tiquié and Papurí, Brazil/Colombia"},
    "Koreguaje":        {"lat":  1.30, "lon": -75.50, "note": "Orteguaza and Caquetá rivers, Caquetá Dept."},
    "Maijuna":          {"lat": -2.50, "lon": -73.00, "note": "Sucusari and Yanayacu rivers, Loreto, Peru"},
    "Makuna":           {"lat": -0.20, "lon": -70.50, "note": "Lower Apaporis and Comeña rivers, Vaupés"},
    "Piratapuyo":       {"lat":  0.55, "lon": -69.80, "note": "Papurí River, Colombia/Brazil border"},
    "Pisamira":         {"lat":  1.00, "lon": -70.00, "note": "Vaupés River area, Colombia"},
    "Siona":            {"lat":  0.20, "lon": -76.30, "note": "Putumayo River, Colombia/Ecuador border"},
    "Siriano":          {"lat":  0.50, "lon": -70.00, "note": "Vaupés region, Colombia/Brazil"},
    "Tanimuca-Retuarã": {"lat": -0.50, "lon": -70.80, "note": "Apaporis and Mirití-Paraná, Colombia"},
    "Tatuyo":           {"lat":  0.50, "lon": -70.30, "note": "Pira-Paraná drainage, Vaupés Dept."},
    "Tucano":           {"lat":  0.80, "lon": -69.50, "note": "Vaupés and Papurí rivers, Colombia/Brazil"},
    "Tuyuca":           {"lat":  0.30, "lon": -69.50, "note": "Upper Tiquié River, Brazil/Colombia"},
    "Wajiara":          {"lat":  0.50, "lon": -70.20, "note": "Vaupés region (also called Yuruti)"},
    "Wanano":           {"lat":  0.30, "lon": -69.00, "note": "Upper Vaupés River, Brazil"},
}


def load_tukanoan_data() -> pd.DataFrame:
    """Load and filter the languages dataset to the Tukanoan family."""
    languages = pd.read_csv("data/languages_dataset.csv")
    tuk = languages[languages["language_family"] == "Tukanoan"].copy()

    # Add coordinates from the lookup table
    tuk["latitude"] = tuk["name_en"].map(
        lambda x: TUKANOAN_COORDINATES.get(x, {}).get("lat")
    )
    tuk["longitude"] = tuk["name_en"].map(
        lambda x: TUKANOAN_COORDINATES.get(x, {}).get("lon")
    )
    tuk["location_note"] = tuk["name_en"].map(
        lambda x: TUKANOAN_COORDINATES.get(x, {}).get("note", "")
    )

    # Sort by collection count descending for a useful ordering
    tuk = tuk.sort_values("collection_count", ascending=False).reset_index(drop=True)

    return tuk


def clean_description(desc: str) -> str:
    """Strip HTML tags and truncate long descriptions."""
    if not isinstance(desc, str) or desc == "":
        return ""
    # Remove HTML tags (some descriptions contain <br> tags)
    cleaned = re.sub(r"<[^>]+>", " ", desc)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    # Truncate for the reference sheet (full text goes in StoryMapJS)
    if len(cleaned) > 300:
        cleaned = cleaned[:297] + "..."
    return cleaned


def generate_reference_sheet(tuk: pd.DataFrame) -> str:
    """Generate a markdown reference sheet for manual ClioVis data entry."""
    lines = [
        "# Tukanoan Language Family: ClioVis Test Dataset",
        "",
        "**Purpose:** Reference sheet for manually entering data into ClioVis",
        "to evaluate the platform for the AILLA Language Atlas project.",
        "",
        "**Language family:** Tukanoan (Eastern and Western branches)",
        f"**Languages:** {len(tuk)}",
        f"**Countries:** {', '.join(sorted(set(c.strip() for countries in tuk['countries'].dropna() for c in countries.split(';'))))}",
        f"**Total AILLA collections:** {tuk['collection_count'].sum()}",
        "",
        "**Geographic context:** The Tukanoan languages are concentrated in the",
        "Vaupés region along the Colombia-Brazil border, with western outliers",
        "(Koreguaje in Caquetá, Siona on the Putumayo) and a southern outlier",
        "(Maijuna in Peru). This distribution makes them ideal for testing",
        "how well a visualization platform handles clustered vs. dispersed points.",
        "",
        "**Note on coordinates:** The dataset does not include precise coordinates.",
        "The latitude/longitude values below are approximate centers of each",
        "language's traditional territory, based on published ethnolinguistic",
        "sources. They are suitable for visualization prototyping, not for",
        "precise geospatial analysis.",
        "",
        "**Note on temporal data:** The current AILLA dataset does not include",
        "collection deposit dates or recording dates. The `collection_count`",
        "field reflects the current state of AILLA holdings. Temporal",
        "visualization in ClioVis could use collection growth over time if",
        "date metadata becomes available in a future data extraction.",
        "",
        "---",
        "",
        "## Data Table",
        "",
        "| # | Language | Indigenous Name | ISO 639-3 | Countries | Lat | Lon | Collections | AILLA URL |",
        "|---|----------|-----------------|-----------|-----------|-----|-----|-------------|-----------|",
    ]

    for i, (_, row) in enumerate(tuk.iterrows(), 1):
        name = row["name_en"]
        indigenous = row["indigenous_name"] if pd.notna(row["indigenous_name"]) else ""
        iso = row["iso_639_3_code"]
        countries = row["countries"] if pd.notna(row["countries"]) else "unknown"
        lat = f"{row['latitude']:.2f}" if pd.notna(row["latitude"]) else ""
        lon = f"{row['longitude']:.2f}" if pd.notna(row["longitude"]) else ""
        colls = int(row["collection_count"])
        url = row["ailla_language_url"]

        lines.append(
            f"| {i} | {name} | {indigenous} | `{iso}` | {countries} | {lat} | {lon} | {colls} | [{name}]({url}) |"
        )

    lines.extend([
        "",
        "---",
        "",
        "## Location Notes",
        "",
        "Approximate geographic locations for each language, for reference",
        "when placing map pins in ClioVis:",
        "",
    ])

    for _, row in tuk.iterrows():
        note = row["location_note"]
        if note:
            lines.append(f"- **{row['name_en']}:** {note}")

    lines.extend([
        "",
        "---",
        "",
        "## Descriptions (for ClioVis narrative content)",
        "",
        "Sociolinguistic notes from AILLA, where available. Use these as",
        "the body text for each ClioVis entry.",
        "",
    ])

    for _, row in tuk.iterrows():
        desc = clean_description(row.get("description", ""))
        if desc:
            lines.append(f"### {row['name_en']}")
            lines.append(f"")
            lines.append(f"{desc}")
            lines.append(f"")

    lines.extend([
        "---",
        "",
        "## Collection URLs (for ClioVis linking)",
        "",
        "Direct links to AILLA collections for each language. These can be",
        "added as external links in ClioVis entries.",
        "",
    ])

    for _, row in tuk.iterrows():
        urls = row["collection_urls"] if pd.notna(row["collection_urls"]) else ""
        if urls:
            lines.append(f"**{row['name_en']}** ({int(row['collection_count'])} collections):")
            for url in urls.split("; "):
                url = url.strip()
                if url:
                    lines.append(f"  - {url}")
            lines.append("")

    lines.extend([
        "",
        f"*Generated from AILLA data on 2026-02-21 by scripts/cliovis_eval_prep.py*",
    ])

    return "\n".join(lines)


def generate_storymapjs_json(tuk: pd.DataFrame) -> dict:
    """
    Generate a StoryMapJS JSON structure for the Tukanoan language family.

    StoryMapJS format reference: https://storymap.knightlab.com/
    The JSON structure follows the StoryMapJS specification with a title
    slide and one slide per language, each with a map location.
    """
    slides = []

    # Title slide (no location needed)
    title_slide = {
        "type": "overview",
        "text": {
            "headline": "Tukanoan Languages of the Amazon",
            "text": (
                "<p>The Tukanoan language family spans the northwestern Amazon, "
                "with most languages concentrated in the Vaupés region along the "
                "Colombia-Brazil border. This family includes 19 languages "
                "documented in the Archive of the Indigenous Languages of Latin "
                "America (AILLA) at UT Austin.</p>"
                "<p>The Vaupés region is one of the most linguistically diverse "
                "areas in the world, where multilingualism is the norm and "
                "language identity is central to social organization. Many "
                "Tukanoan-speaking communities practice linguistic exogamy, "
                "where spouses must come from a different language group.</p>"
                "<p><em>Data source: "
                '<a href="https://ailla.utexas.org">AILLA</a>, '
                "Lillas Benson Latin American Studies and Collections, "
                "University of Texas at Austin.</em></p>"
            ),
        },
        "media": {
            "url": "",
            "caption": "AILLA Language Atlas: Tukanoan Family Prototype",
        },
    }
    slides.append(title_slide)

    # One slide per language, ordered by collection count (richest first)
    for _, row in tuk.iterrows():
        name = row["name_en"]
        indigenous = row["indigenous_name"] if pd.notna(row["indigenous_name"]) else ""
        iso = row["iso_639_3_code"]
        countries = row["countries"] if pd.notna(row["countries"]) else "unknown"
        lat = row["latitude"]
        lon = row["longitude"]
        colls = int(row["collection_count"])
        ailla_url = row["ailla_language_url"]
        collection_urls = row["collection_urls"] if pd.notna(row["collection_urls"]) else ""

        # Skip languages without coordinates
        if pd.isna(lat) or pd.isna(lon):
            continue

        # Build headline: include indigenous name if available
        if indigenous:
            headline = f"{name} ({indigenous})"
        else:
            headline = name

        # Build description text
        desc_parts = []

        # Country and classification info
        desc_parts.append(
            f"<p><strong>ISO 639-3:</strong> <code>{iso}</code> | "
            f"<strong>Countries:</strong> {countries} | "
            f"<strong>AILLA collections:</strong> {colls}</p>"
        )

        # Sociolinguistic description from AILLA
        raw_desc = row.get("description", "")
        if isinstance(raw_desc, str) and raw_desc.strip():
            # Clean HTML but keep paragraph breaks
            cleaned = re.sub(r"<br\s*/?>", "</p><p>", raw_desc)
            cleaned = re.sub(r"<(?!/?p)[^>]+>", "", cleaned)
            if not cleaned.startswith("<p>"):
                cleaned = f"<p>{cleaned}</p>"
            desc_parts.append(cleaned)

        # Link to AILLA
        desc_parts.append(
            f'<p><a href="{ailla_url}">View on AILLA</a></p>'
        )

        # Collection links
        if collection_urls:
            coll_links = []
            for i, url in enumerate(collection_urls.split("; "), 1):
                url = url.strip()
                if url:
                    coll_links.append(f'<a href="{url}">Collection {i}</a>')
            if coll_links:
                desc_parts.append(f"<p><strong>Collections:</strong> {' | '.join(coll_links)}</p>")

        slide = {
            "text": {
                "headline": headline,
                "text": "\n".join(desc_parts),
            },
            "location": {
                "lat": lat,
                "lon": lon,
                "name": row.get("location_note", ""),
                "zoom": 7,
            },
            "media": {
                "url": "",
                "caption": f"{name} language, Tukanoan family",
            },
        }
        slides.append(slide)

    storymap = {
        "storymap": {
            "language": "en",
            "map_type": "stamen:terrain",
            "map_as_image": False,
            "calculate_zoom": True,
            "slides": slides,
        }
    }

    return storymap


def validate_dataset(tuk: pd.DataFrame) -> list[str]:
    """Run validation checks and return a list of findings."""
    findings = []

    # Check for missing coordinates
    missing_coords = tuk[tuk["latitude"].isna() | tuk["longitude"].isna()]
    if len(missing_coords) > 0:
        findings.append(
            f"WARNING: {len(missing_coords)} languages missing coordinates: "
            f"{', '.join(missing_coords['name_en'].tolist())}"
        )
    else:
        findings.append(f"OK: All {len(tuk)} languages have coordinates")

    # Check for missing country data
    missing_countries = tuk[tuk["countries"].isna()]
    if len(missing_countries) > 0:
        findings.append(
            f"WARNING: {len(missing_countries)} languages missing country data: "
            f"{', '.join(missing_countries['name_en'].tolist())}"
        )
    else:
        findings.append(f"OK: All {len(tuk)} languages have country data")

    # Check collection coverage
    with_collections = tuk[tuk["collection_count"] > 0]
    findings.append(
        f"INFO: {len(with_collections)}/{len(tuk)} languages have AILLA collections "
        f"({len(with_collections)/len(tuk)*100:.0f}%)"
    )

    # Check description coverage
    has_desc = tuk[tuk["description"].apply(
        lambda x: isinstance(x, str) and len(x.strip()) > 0
    )]
    findings.append(
        f"INFO: {len(has_desc)}/{len(tuk)} languages have descriptions "
        f"({len(has_desc)/len(tuk)*100:.0f}%)"
    )

    # Check ISO code uniqueness (Tucano and Pisamira share 'tuo')
    iso_dupes = tuk[tuk.duplicated(subset=["iso_639_3_code"], keep=False)]
    if len(iso_dupes) > 0:
        for code in iso_dupes["iso_639_3_code"].unique():
            names = iso_dupes[iso_dupes["iso_639_3_code"] == code]["name_en"].tolist()
            findings.append(
                f"NOTE: ISO code '{code}' shared by: {', '.join(names)} "
                f"(Pisamira is classified as a Tukano dialect in ISO 639-3)"
            )

    return findings


def main() -> None:
    """Extract Tukanoan test data and generate all evaluation outputs."""
    print("ClioVis Evaluation Prototype: Tukanoan Language Family")
    print("=" * 55)
    print()

    # Load and enrich data
    print("Loading Tukanoan language data...")
    tuk = load_tukanoan_data()
    print(f"  Found {len(tuk)} Tukanoan languages")
    print()

    # Validate
    print("Validation:")
    findings = validate_dataset(tuk)
    for f in findings:
        print(f"  {f}")
    print()

    # Output directory
    output_dir = Path("data/cliovis_eval")
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Generate markdown reference sheet
    print("Generating reference sheet...")
    reference_md = generate_reference_sheet(tuk)
    ref_path = output_dir / "tukanoan_reference_sheet.md"
    ref_path.write_text(reference_md, encoding="utf-8")
    print(f"  Saved: {ref_path}")

    # 2. Generate StoryMapJS JSON
    print("Generating StoryMapJS JSON...")
    storymap_json = generate_storymapjs_json(tuk)
    json_path = output_dir / "tukanoan_storymap.json"
    json_path.write_text(
        json.dumps(storymap_json, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )
    slide_count = len(storymap_json["storymap"]["slides"])
    print(f"  Saved: {json_path} ({slide_count} slides including title)")

    # 3. Export the subset as CSV for easy reference
    print("Exporting subset CSV...")
    export_cols = [
        "name_en", "indigenous_name", "iso_639_3_code", "language_family",
        "countries", "latitude", "longitude", "location_note",
        "collection_count", "ailla_language_url", "collection_urls",
        "description",
    ]
    csv_path = output_dir / "tukanoan_subset.csv"
    tuk[export_cols].to_csv(csv_path, index=False, encoding="utf-8")
    print(f"  Saved: {csv_path}")

    print()
    print("All outputs written to data/cliovis_eval/")
    print()
    print("Next steps:")
    print("  1. Open tukanoan_reference_sheet.md and use it to enter")
    print("     data into ClioVis manually")
    print("  2. Open tukanoan_storymap.json in the StoryMapJS editor")
    print("     (storymap.knightlab.com) for comparison")
    print("  3. Use docs/cliovis_evaluation_rubric.md to score both")
    print("     platforms systematically")


if __name__ == "__main__":
    main()
