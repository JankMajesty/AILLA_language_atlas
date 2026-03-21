# AILLA Language Atlas Project

**Fellowship:** Latin American and Iberian Digital Scholarship (LBDS) Fellowship
**Institution:** Lillas Benson Latin American Studies and Collections, UT Austin
**Project Period:** 2026 Spring Semester
**Focus:** Interactive visualization of indigenous languages of Latin America

---

## Project Overview

This project creates an interactive Language Atlas visualization using AILLA (Archive of the Indigenous Languages of Latin America) metadata. The atlas will showcase the geographic distribution, linguistic diversity, and archival documentation of 639 indigenous languages across Latin America.

**Week 1 Goal:** Extract and structure AILLA metadata into datasets suitable for visualization
**Week 2+ Goals:** Build StoryMapJS interface, curate images, refine presentation

---

## Current Status

### ✅ Week 1 Complete: Data Collection (2026-01-28)

**Accomplished:**
- Extracted metadata for 639 languages from AILLA public API
- Compiled 263 collection records with language relationships
- Documented 63 countries and 54 language families
- Generated structured CSV and JSON datasets
- Created comprehensive documentation

**Datasets Available:**
- `data/languages_dataset.csv` (639 records)
- `data/collections_dataset.csv` (263 records)
- `data/countries_dataset.csv` (63 records)
- `data/language_families_dataset.csv` (54 records)
- `data/ailla_atlas_data.json` (complete structured export)

---

## Repository Structure

```
LBDSfellowship/
├── scripts/
│   └── ailla_scraper.py          # Web scraper for AILLA metadata extraction
├── data/
│   ├── languages_dataset.csv     # Primary language metadata (639 records)
│   ├── collections_dataset.csv   # Collection metadata (263 records)
│   ├── countries_dataset.csv     # Country reference data (63 records)
│   ├── language_families_dataset.csv  # Language family reference (54 records)
│   ├── ailla_atlas_data.json     # Complete structured JSON export
│   ├── ailla_raw_data.json       # Raw API responses (backup)
│   └── extraction_report.txt     # Data quality summary
├── docs/
│   ├── dataset_documentation.md  # Complete data dictionary and usage guide
│   └── methodology.md            # Extraction methodology and technical details
├── README.md                     # This file
└── pyproject.toml                # Python dependencies (managed by uv)
```

---

## Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager

### Installation

```bash
# Clone or navigate to project directory
cd /Users/jankmajesty/Desktop/LBDSfellowship

# Dependencies are already installed in .venv/
# To reinstall if needed:
uv add requests pandas
```

### Running the Scraper

To extract fresh data from AILLA:

```bash
uv run scripts/ailla_scraper.py
```

**Runtime:** ~2 minutes (respectful rate limiting)

**Output:**
- Updates all CSV files in `data/`
- Updates JSON export
- Generates new extraction report

---

## Working with the Data

### Python

```python
import pandas as pd

# Load languages dataset
languages = pd.read_csv('data/languages_dataset.csv')

# Explore the data
print(f"Total languages: {len(languages)}")
print(f"\nTop 5 language families:")
print(languages['language_family'].value_counts().head())

# Filter to a specific country
mexican_langs = languages[languages['countries'].str.contains('Mexico', na=False)]
print(f"\nLanguages in Mexico: {len(mexican_langs)}")

# Find well-documented languages
well_documented = languages[languages['collection_count'] > 10]
print(f"\nLanguages with 10+ collections: {len(well_documented)}")
```

### R

```r
library(tidyverse)

# Load languages dataset
languages <- read_csv('data/languages_dataset.csv')

# Count languages per family
languages %>%
  count(language_family, sort = TRUE) %>%
  head(10)

# Analyze geographic distribution
languages %>%
  separate_rows(countries, sep = "; ") %>%
  count(countries, sort = TRUE)

# Documentation coverage
languages %>%
  summarise(
    total = n(),
    documented = sum(collection_count > 0),
    pct_documented = mean(collection_count > 0) * 100
  )
```

### Command Line

```bash
# Count total languages
wc -l data/languages_dataset.csv

# See column headers
head -1 data/languages_dataset.csv

# Find a specific language
grep -i "nahuatl" data/languages_dataset.csv

# Count languages per country (rough approximation)
cut -d',' -f11 data/languages_dataset.csv | grep -o "Mexico" | wc -l
```

---

## Key Statistics

**As of 2026-01-28:**

### Languages (639 total)

**Data Completeness:**
- ISO 639-3 codes: 100% (639/639)
- Language family classifications: 90.1% (576/639)
- Geographic data: 90.8% (580/639)
- Indigenous names: 38.3% (245/639)
- Collection documentation: 73.4% (469/639)

**Top Language Families:**
1. Otomanguean: 143 languages
2. South American Indigenous Languages: 50 languages
3. Uto-Aztecan: 49 languages
4. Mayan: 33 languages
5. Arawakan: 24 languages

**Top Countries:**
1. Mexico: 261 languages
2. Brazil: 77 languages
3. Colombia: 53 languages
4. Peru: 50 languages
5. United States: 47 languages

### Collections (263 total)

**Documentation Density:**
- Average collections per language: 3.00
- Maximum collections for one language: 23
- Languages with 0 collections: 170 (26.6%)
- Languages with 1+ collections: 469 (73.4%)

---

## Documentation

### For Data Users

**[Dataset Documentation](docs/dataset_documentation.md)** - Complete data dictionary
- Field definitions for all datasets
- Data completeness notes
- Usage examples (Python, R)
- StoryMapJS integration guidance
- Citation information

### For Technical Users

**[Methodology Documentation](docs/methodology.md)** - Technical implementation details
- API endpoints and data extraction process
- Data transformations and cleaning steps
- Validation and quality assurance
- Ethical web scraping practices
- Instructions for updating the dataset

### Summary Report

**`data/extraction_report.txt`** - Automated quality report
- Dataset sizes and record counts
- Data completeness statistics
- Top language families and countries
- Collection documentation coverage

---

## Data Updates

### When to Update

- **Quarterly or biannually:** To capture new AILLA additions
- **Before major presentations:** Ensure current data
- **When AILLA announces significant additions:** New collections or languages

### Update Process

1. Archive current datasets with date:
   ```bash
   mkdir -p data/archive
   cp data/*_dataset.csv data/archive/
   mv data/archive/languages_dataset.csv data/archive/languages_dataset_$(date +%Y-%m-%d).csv
   # Repeat for other files
   ```

2. Run scraper:
   ```bash
   uv run scripts/ailla_scraper.py
   ```

3. Compare results:
   - Check `data/extraction_report.txt` for new counts
   - Spot-check a few languages for accuracy
   - Note any significant changes

4. Update documentation:
   - Revise this README with new statistics
   - Update extraction date in docs/

---

## Next Steps (Week 2+)

### StoryMapJS Development

**Goals:**
- Create interactive map slides for languages or countries
- Add chronological elements (collection dates, documentation periods)
- Curate representative images for visual engagement
- Link to AILLA collections for deeper exploration

**Data Preparation:**
- Geocode countries to latitude/longitude coordinates
- Select representative languages for featured slides
- Extract temporal data from collections
- Prepare multilingual slide content (EN/ES/PT)

**Technical Implementation:**
- StoryMapJS JSON format generation
- Image hosting and optimization
- Multilingual interface design
- Mobile-responsive layout

### Potential Enhancements

**Data Analysis:**
- Language endangerment visualization
- Documentation coverage heatmaps
- Language family network graphs
- Temporal growth of AILLA archive

**Integration:**
- Link to Glottolog for additional linguistic data
- Cross-reference with Ethnologue for speaker populations
- Connect to UNESCO Atlas of World's Languages in Danger
- Embed in AILLA website or Benson digital exhibits

---

## Troubleshooting

### Scraper Issues

**Problem:** `ModuleNotFoundError: No module named 'requests'`
**Solution:** Run `uv add requests pandas` to install dependencies

**Problem:** `JSONDecodeError` during scraping
**Solution:** Check internet connection; AILLA backend may be temporarily unavailable

**Problem:** Different record counts than documented
**Solution:** AILLA may have added/removed languages; update documentation with new counts

### Data Issues

**Problem:** CSV won't open correctly in Excel
**Solution:** Use "Import Data" and specify UTF-8 encoding

**Problem:** Special characters appear garbled
**Solution:** Ensure UTF-8 encoding is used when opening files

**Problem:** Missing data in specific fields
**Solution:** This reflects the AILLA database state; see data completeness notes in documentation

---

## Contact and Support

**AILLA Questions:** ailla@austin.utexas.edu
**Project Questions:** Contact Dr. Susan Smythe Kung (AILLA Director)
**Technical Issues:** Consult methodology documentation or contact LBDS Fellow

---

## Citation

When using this dataset or visualization, please cite:

> AILLA Language Atlas Dataset. (2026). Extracted from the Archive of the Indigenous Languages of Latin America. Lillas Benson Latin American Studies and Collections, University of Texas at Austin. https://ailla.utexas.org

And acknowledge the data source:

> Archive of the Indigenous Languages of Latin America (AILLA). Lillas Benson Latin American Studies and Collections, University of Texas at Austin. https://ailla.utexas.org

---

## License

This dataset is derived from publicly accessible AILLA metadata and is intended for educational, research, and archival purposes. All underlying archival materials remain under the copyright and usage terms specified by AILLA and original depositors.

**Educational Use:** Encouraged for teaching, research, and public outreach
**Commercial Use:** Requires consultation with AILLA
**Respect:** Honor indigenous language communities' cultural protocols

---

## Acknowledgments

**Data Source:** Archive of the Indigenous Languages of Latin America (AILLA)
**Fellowship Support:** Lillas Benson Latin American Studies and Collections
**Director:** Dr. Susan Smythe Kung
**Tools:** Python, uv, pandas, requests, Claude Code CLI

**Special Thanks:**
- AILLA staff for maintaining comprehensive public API
- Language communities whose knowledge is preserved in AILLA
- Linguists, anthropologists, and collectors who contributed materials

---

**Project Status:** Week 1 Complete ✅ | Week 2 In Progress 🚧
**Last Updated:** 2026-01-28
**Next Milestone:** StoryMapJS interface development
