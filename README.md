# AILLA Language Atlas

Interactive StoryMapJS visualizations of indigenous language documentation in the [Archive of the Indigenous Languages of Latin America (AILLA)](https://ailla.utexas.org) at the University of Texas at Austin.

## About

The AILLA Language Atlas maps two of the most extensively documented language families in AILLA's holdings: the **Mayan languages** of Mesoamerica and the **Quechua languages** of the Andes. Each map plots languages geographically and orders them chronologically by the start of dedicated documentation, revealing spatial and temporal patterns in how these languages have been recorded and preserved.

The atlas draws on AILLA's metadata for 641 indigenous languages across 263 archival collections, with curated descriptions for 27 featured languages. All maps are available in English, Spanish, and Portuguese.

## View the Maps

- **Mayan Languages:** [English](https://jankmajesty.github.io/AILLA_language_atlas/preview_mayan.html) | [Espanol](https://jankmajesty.github.io/AILLA_language_atlas/preview_mayan_es.html) | [Portugues](https://jankmajesty.github.io/AILLA_language_atlas/preview_mayan_pt.html)
- **Quechua Languages:** [English](https://jankmajesty.github.io/AILLA_language_atlas/preview_quechua.html) | [Espanol](https://jankmajesty.github.io/AILLA_language_atlas/preview_quechua_es.html) | [Portugues](https://jankmajesty.github.io/AILLA_language_atlas/preview_quechua_pt.html)

### Embedding

The maps can be embedded in any website via iframe:

```html
<iframe src="https://jankmajesty.github.io/AILLA_language_atlas/preview_mayan.html"
        width="100%" height="600" frameborder="0"></iframe>
```

## What the Maps Show

**Mayan map:** 18 featured languages with 100+ items each, plus a summary slide listing 15 additional languages. Covers Guatemala, Mexico, and Belize, with documentation spanning from the 1960s to the present.

**Quechua map:** 9 featured languages with 5+ items each, plus a summary slide listing 5 additional languages. Covers Peru, Ecuador, Colombia, Bolivia, and Argentina, with documentation from 1959 to 2018.

Each language slide includes:
- Speaker population and geographic context
- AILLA holdings summary (item counts, collection highlights)
- Dates of material creation and deposit at AILLA
- Direct link to the language's page on AILLA's website

## Data Pipeline

The atlas is built from AILLA's public API and institutional AILLA2 spreadsheets through a multi-stage pipeline:

```
ailla_scraper.py     API extraction (languages, collections, countries)
       |
extract_ailla2.py    AILLA2 spreadsheet processing (18,548 items, 118,893 files)
       |
analyze_families.py  Language family ranking by documentation depth
       |
geocode.py           Curated coordinates for featured languages
       |
build_storymaps.py   StoryMapJS JSON generation
       |
translate_storymaps.py   Trilingual translation (EN/ES/PT)
```

### Requirements

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager

```bash
uv sync
```

## Citation

> AILLA Language Atlas. (2026). Archive of the Indigenous Languages of Latin America, Lillas Benson Latin American Studies and Collections, University of Texas at Austin. https://ailla.utexas.org

## License

This project visualizes publicly accessible AILLA metadata for educational, research, and archival purposes. All underlying archival materials remain under the copyright and usage terms specified by AILLA and original depositors. Please honor indigenous language communities' cultural protocols when using this data.

## Acknowledgments

**Data source:** Archive of the Indigenous Languages of Latin America (AILLA)

**Fellowship support:** Latin American and Iberian Digital Scholarship (LBDS) Fellowship, Lillas Benson Latin American Studies and Collections, University of Texas at Austin

**Director:** Dr. Susan Smythe Kung
