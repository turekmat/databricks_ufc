## Databricks UFC Pipeline (ESPN → ADLS → Delta)

### Overview

Automated pipeline to ingest UFC data from the unofficial ESPN Core/Scoreboard APIs into Azure Data Lake Storage (ADLS) and Delta tables on Databricks.

- **Ingest**: events, fights, and athletes (profiles)
- **Storage layers**: landing → raw → bronze (Delta) → silver (Delta)
- **Goals**: incremental loads with watermark, backfill, idempotent MERGE-based upserts, data quality in silver

### Key notebooks/scripts

- `01_ingest_espn_fights.py`: Ingest events and fights to bronze (Delta) + raw JSON mirror
- `02_explore_espn_bronze.ipynb`: Quick summaries and validations of bronze
- `02_ingest_espn_athletes.ipynb`: Ingest athlete profiles to bronze (Delta) + raw JSON mirror
- `11_events_clean.ipynb`: Clean `espn_events` → `espn_events_silver`
- `12_fights_clean.ipynb`: Clean `espn_fights` → `espn_fights_silver`
- `13_athletes_clean.ipynb`: Clean `espn_athletes` → `espn_athletes_silver`

### Minimal run order

1. `01_ingest_espn_fights.py`
2. `02_ingest_espn_athletes.ipynb`
3. `11_events_clean.ipynb`, `12_fights_clean.ipynb`, `13_athletes_clean.ipynb`
4. (Optional) `02_explore_espn_bronze.ipynb` to validate bronze

### Storage & catalog

- ADLS containers expected: `landing`, `raw`, `bronze` (and `silver` if desired)
- Unity Catalog: uses `hive_metastore` with configurable DBs (default `ufc_bronze`, `ufc_silver`)
- Access: set account key via Databricks secrets in the bootstrap widgets/cells

This README is intentionally brief; expand with setup steps, widgets, and examples as the project stabilizes.
