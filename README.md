## UFC Data Platform on Databricks (ESPN → ADLS → Delta)

This project is a compact, production‑minded data platform built on Databricks (free tier). It ingests public UFC data from ESPN, cleans it to reliable Delta tables, and materializes analytics that answer real fight questions.

### What I built

- End‑to‑end lakehouse: landing → raw → bronze (Delta) → silver (clean) → gold (marts)
- Incremental, idempotent ingestion with watermark + overlap (late data safe)
- Robustness on free tier: self‑healing silver when bronze Delta logs are corrupted; backfill‑only reset
- Secure access (Databricks Secrets), UC‑friendly databases, parameterized notebooks
- Analytics with clear storytelling (stance/style matchups, team performance, height/reach advantage)

### How it works

- Ingest pulls ESPN Core v2 JSON, saves to landing and mirrors to raw, then MERGEs to bronze (events, fights, athletes).
- Silver standardizes schema, trims/cleans text, deduplicates by natural keys, and prunes sparse columns. If bronze is unreadable, it automatically falls back to RAW to keep the pipeline green.
- Gold rebuilds small, focused Delta marts on each run (overwrite) for deterministic BI.

### Reliability details

- MERGE keys: events `event_id`; fights `event_id+competition_id`; athletes `athlete_id`.
- Watermark table controls incremental window; first run backfills, next runs pick up just changes.
- Task‑to‑task passing: the fights task passes new `athlete_id` values to the next step (fewer API calls, still idempotent).

### Examples of insights

- Which stance beats which across the sport? (stance matchup matrix)
- Do certain “styles” (Striker/Wrestler/MMA/Street) dominate specific matchups?
- Which teams have the highest win rate and how many fights per fighter do they produce?
- How much does height/reach advantage help? (binned win‑rate vs height/reach advantage)
