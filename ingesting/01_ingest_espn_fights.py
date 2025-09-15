# ===== WIDGETS =====
dbutils.widgets.text("storage_account", "storagetmufc")
dbutils.widgets.text("secret_scope", "kv-scope")
dbutils.widgets.text("key_name", "adls-account-key")
dbutils.widgets.dropdown("run_mode", "backfill", ["backfill","incremental"])
dbutils.widgets.text("years_back", "7")
dbutils.widgets.text("overlap_days", "1")
dbutils.widgets.text("max_concurrency", "5")
dbutils.widgets.text("http_timeout_sec", "30")
dbutils.widgets.text("http_retries", "3")
dbutils.widgets.text("db_name", "ufc_bronze")
dbutils.widgets.text("user_agent", "ufc-pipeline/1.0 (+databricks)")
dbutils.widgets.text("estimate_sample_days", "12")
dbutils.widgets.text("estimate_sample_core_events", "20")

storage_account  = dbutils.widgets.get("storage_account")
secret_scope     = dbutils.widgets.get("secret_scope")
key_name         = dbutils.widgets.get("key_name")
run_mode         = dbutils.widgets.get("run_mode")
years_back       = int(dbutils.widgets.get("years_back"))
overlap_days     = int(dbutils.widgets.get("overlap_days"))
MAX_CONC         = int(dbutils.widgets.get("max_concurrency"))
HTTP_TIMEOUT     = int(dbutils.widgets.get("http_timeout_sec"))
HTTP_RETRIES     = int(dbutils.widgets.get("http_retries"))
DB_NAME          = dbutils.widgets.get("db_name")
USER_AGENT       = dbutils.widgets.get("user_agent")
EST_SAMPLE_DAYS  = int(dbutils.widgets.get("estimate_sample_days"))
EST_SAMPLE_CORE  = int(dbutils.widgets.get("estimate_sample_core_events"))

# ===== STORAGE CONFIG (Access Key) =====
account_key = dbutils.secrets.get(secret_scope, key_name)
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", account_key)

def abfss(container, path=""):
    base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    return f"{base}/{path}".rstrip("/")

# ===== PATHS =====
PATH_EVENTS_DELTA = abfss("bronze", "ufc_bronze/espn_events")
PATH_FIGHTS_DELTA = abfss("bronze", "ufc_bronze/espn_fights")
PATH_META_WM      = abfss("meta",   "ufc/pipeline/watermarks/last_event_date")
PATH_META_RUNS    = abfss("meta",   "ufc/pipeline/runs")
PATH_LOGS_RUN     = abfss("logs",   "ufc/pipeline")
PATH_LANDING_ROOT = abfss("landing","espn/ufc/events")
PATH_RAW_ROOT     = abfss("raw",    "espn/ufc/events")

# ===== RUNTIME & BOOTSTRAP (bez vytváření Delta LOCATION složek) =====
from datetime import datetime, timezone, date, timedelta
import json, time, math
from statistics import mean
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
INGESTION_DATE = date.today().isoformat()

def ensure_dir(path):
    dbutils.fs.mkdirs(path)
    try: dbutils.fs.put(f"{path}/.keep","", overwrite=False)
    except: pass

for p in [
    f"{PATH_LANDING_ROOT}/run_id={RUN_ID}",
    f"{PATH_RAW_ROOT}/ingestion_date={INGESTION_DATE}",
    f"{PATH_LOGS_RUN}/run_id={RUN_ID}",
]: ensure_dir(p)

# ===== CLEANUP non-Delta složek pro LOCATION =====
targets = [PATH_META_WM, PATH_META_RUNS, PATH_EVENTS_DELTA, PATH_FIGHTS_DELTA]
def is_delta(path):
    try: return any(f.name.rstrip('/') == "_delta_log" for f in dbutils.fs.ls(path))
    except: return False
for p in targets:
    try:
        if not is_delta(p) and len(dbutils.fs.ls(p))>0:
            dbutils.fs.rm(p, recurse=True)
    except: pass

# ===== ESPN HELPERS =====
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
BASE_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/mma/ufc/scoreboard"
CORE_EVENT_TPL  = "https://sports.core.api.espn.com/v2/sports/mma/leagues/ufc/events/{eventId}"
HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/json"}
CORE_PARAMS = {
    "lang": "en",
    "region": "us",
    "contentorigin": "espn",
    "enable": "competitions,competitors,venues,notes,rankings,statistics",
    "expand": "competitions,competitions.competitors,competitions.competitors.athlete,competitions.competitors.statistics,competitions.venue,competitions.status",
}

def http_get(url, params=None, timeout=HTTP_TIMEOUT, retries=HTTP_RETRIES, backoff=1.5):
    last_exc=None
    for i in range(retries+1):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
            if r.status_code == 200: return r.json()
            if r.status_code in (429,500,502,503,504): raise requests.HTTPError(f"{r.status_code}")
            r.raise_for_status()
        except Exception as e:
            last_exc=e
            if i<retries: time.sleep(backoff**i)
            else: raise last_exc

def date_range_backfill(years):
    end = date.today(); start = end - timedelta(days=int(365.25*years))
    d=start
    while d<=end: 
        yield d; d += timedelta(days=1)

def date_range_incremental(from_date, overlap):
    start = from_date - timedelta(days=overlap)
    end = date.today() + timedelta(days=7)
    d=start
    while d<=end:
        yield d; d += timedelta(days=1)

# ===== WATERMARK (Delta, external LOCATION) =====
spark.sql(f"CREATE DATABASE IF NOT EXISTS hive_metastore.{DB_NAME}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.{DB_NAME}.ufc_last_event_date (
  last_event_date DATE, updated_at TIMESTAMP
) USING DELTA LOCATION '{PATH_META_WM}'
""")

def load_watermark():
    df = spark.table(f"hive_metastore.{DB_NAME}.ufc_last_event_date")
    return None if df.count()==0 else df.select("last_event_date").first()[0]

def save_watermark(dt):
    df = spark.createDataFrame([(dt, datetime.now(timezone.utc))], "last_event_date DATE, updated_at TIMESTAMP")
    df.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema","true").save(PATH_META_WM)

# ===== IO HELPERS =====
def put_json(path, obj): dbutils.fs.put(path, json.dumps(obj, ensure_ascii=False), overwrite=True)

def write_landing(day, scoreboard_json, event_payloads):
    base = f"{PATH_LANDING_ROOT}/run_id={RUN_ID}"
    dstr = day.strftime('%Y%m%d')
    put_json(f"{base}/scoreboard_{dstr}.json", scoreboard_json)
    print(f"scoreboard {dstr} done")
    for eid, ev in event_payloads.items():
        put_json(f"{base}/event_{eid}.json", ev)
        print(f"event {eid} done")

def mirror_to_raw():
    src = f"{PATH_LANDING_ROOT}/run_id={RUN_ID}"
    dst = f"{PATH_RAW_ROOT}/ingestion_date={INGESTION_DATE}"
    try:
        dbutils.fs.cp(src, dst, recurse=True)
    except Exception:
        for f in dbutils.fs.ls(src):
            dbutils.fs.cp(f.path, f"{dst}/{f.name}", recurse=True)

# ===== PARSING =====
def dig(d, path, default=None):
    cur=d
    try:
        for p in path:
            cur = cur[p] if isinstance(p,int) else cur.get(p)
            if cur is None: return default
        return cur
    except: return default

def extract_event_row(ev, ref_get=None):
    node = ev.get("header") if isinstance(ev, dict) and isinstance(ev.get("header"), dict) else ev
    return {
        "event_id": str(dig(node,["id"],"")),
        "event_date": dig(node,["date"],None) or dig(node,["competitions",0,"date"],None),
        "event_name": dig(node,["name"],None) or dig(ev,["header","shortName"],None) or dig(node,["competitions",0,"name"],None) or dig(node,["competitions",0,"notes",0,"headline"],None),
        "venue": dig(node,["venue","fullName"],None) or dig(node,["competitions",0,"venue","fullName"],None),
        "country": dig(node,["venue","address","country"],None) or dig(node,["competitions",0,"venue","address","country"],None),
        "status": dig(node,["status","type","name"],None) or dig(node,["status","name"],None) or dig(node,["competitions",0,"status","type","name"],None) or dig(node,["competitions",0,"status","name"],None),
        "num_fights": len(dig(node,["competitions"],[]) or []),
        "raw_payload": json.dumps(ev, ensure_ascii=False)
    }

def extract_fight_rows(ev, ref_get=None):
    rows=[]
    node = ev.get("header") if isinstance(ev, dict) and isinstance(ev.get("header"), dict) else ev
    event_id=str(dig(node,["id"],"")); event_date=dig(node,["date"],None) or dig(node,["competitions",0,"date"],None)
    comps_raw = dig(node,["competitions"],[]) or []

    def maybe_deref(obj):
        if ref_get and isinstance(obj,dict) and obj.get("$ref") and isinstance(obj.get("$ref"),str):
            try: return ref_get(obj["$ref"]) or {}
            except: return {}
        return obj

    for comp0 in comps_raw:
        comp = maybe_deref(comp0)
        comp_id = str(dig(comp,["id"],""))
        bout    = dig(comp,["name"],None) or dig(comp,["notes",0,"headline"],None)
        wclass  = dig(comp,["type","text"],None)
        if not wclass:
            wnode = maybe_deref(comp.get("weightClass") or comp.get("weight") or {})
            wclass = dig(wnode,["name"],None)
        order   = dig(comp,["matchNumber"],None)
        if order is None: order = dig(comp,["order"],None)
        s_node  = maybe_deref(comp.get("status") or {})
        status  = dig(s_node,["type","name"],None) or dig(s_node,["name"],None)

        comp_competitors = comp.get("competitors")
        if isinstance(comp_competitors, dict) and comp_competitors.get("$ref"):
            cc = maybe_deref(comp_competitors)
            cs = cc.get("items") or cc or []
        else:
            cs = comp_competitors or []

        fa, fb = {}, {}
        if len(cs)>=1:
            c = maybe_deref(cs[0])
            anode = maybe_deref(c.get("athlete") or {})
            stats_node = c.get("statistics")
            if isinstance(stats_node, dict) and stats_node.get("$ref"):
                sn = maybe_deref(stats_node)
                stats_list = sn.get("stats") or sn.get("items") or []
            else:
                stats_list = stats_node or []
            fa={"name": dig(anode,["displayName"],None) or dig(anode,["fullName"],None),
                "win": dig(c,["winner"],None),
                "stats": {str(dig(s,["name"],"") or dig(s,["shortDisplayName"],"")).strip():
                           str(dig(s,["displayValue"],"") or dig(s,["value"],"")).strip()
                           for s in (stats_list or []) if (dig(s,["name"],"") or dig(s,["shortDisplayName"],""))}}
        if len(cs)>=2:
            c = maybe_deref(cs[1])
            anode = maybe_deref(c.get("athlete") or {})
            stats_node = c.get("statistics")
            if isinstance(stats_node, dict) and stats_node.get("$ref"):
                sn = maybe_deref(stats_node)
                stats_list = sn.get("stats") or sn.get("items") or []
            else:
                stats_list = stats_node or []
            fb={"name": dig(anode,["displayName"],None) or dig(anode,["fullName"],None),
                "win": dig(c,["winner"],None),
                "stats": {str(dig(s,["name"],"") or dig(s,["shortDisplayName"],"")).strip():
                           str(dig(s,["displayValue"],"") or dig(s,["value"],"")).strip()
                           for s in (stats_list or []) if (dig(s,["name"],"") or dig(s,["shortDisplayName"],""))}}

        rows.append({
            "competition_id": comp_id, "event_id": event_id, "event_date": event_date,
            "bout_name": bout, "weight_class": wclass,
            "card_order": int(order) if str(order).isdigit() else None, "status": status,
            "fighter_a_name": fa.get("name"), "fighter_a_winner": (bool(fa.get("win")) if fa.get("win") is not None else None),
            "fighter_a_stats": json.dumps(fa.get("stats",{}), ensure_ascii=False),
            "fighter_b_name": fb.get("name"), "fighter_b_winner": (bool(fb.get("win")) if fb.get("win") is not None else None),
            "fighter_b_stats": json.dumps(fb.get("stats",{}), ensure_ascii=False),
            "raw_payload": json.dumps(comp, ensure_ascii=False)
        })
    return rows

# ===== FETCH & PARSE PER DAY (šetří paměť) =====
def fetch_day(day):
    sb = http_get(BASE_SCOREBOARD, params={"dates": day.strftime("%Y%m%d")})
    events = sb.get("events",[]) or []
    ids = [str(e.get("id")) for e in events if e.get("id") is not None]
    payloads={}
    if MAX_CONC<=1 or len(ids)<=1:
        for eid in ids: payloads[eid]=http_get(CORE_EVENT_TPL.format(eventId=eid), params=CORE_PARAMS)
    else:
        with ThreadPoolExecutor(max_workers=min(MAX_CONC,len(ids))) as ex:
            futs={ex.submit(http_get, CORE_EVENT_TPL.format(eventId=eid), CORE_PARAMS): eid for eid in ids}
            for f in as_completed(futs):
                eid=futs[f]
                try: payloads[eid]=f.result()
                except Exception as e: put_json(f"{PATH_LOGS_RUN}/run_id={RUN_ID}/error_event_{eid}.json", {"error":str(e)})
    write_landing(day, sb, payloads)
    erows, frows = [], []
    # simple per-day ref cache to minimize repeated HTTP calls
    ref_cache={}
    def ref_get(url):
        if not isinstance(url,str): return {}
        if url in ref_cache: return ref_cache[url]
        try:
            data = http_get(url)
            ref_cache[url]=data; return data
        except Exception:
            ref_cache[url]={}; return {}
    for ev in payloads.values():
        erows.append(extract_event_row(ev, ref_get))
        frows.extend(extract_fight_rows(ev, ref_get))
    return erows, frows

# ===== ESTIMATION =====
def _percentile(values, p):
    vs = sorted(values)
    if not vs: return None
    k = (len(vs)-1) * p
    f = math.floor(k); c = math.ceil(k)
    if f == c: return vs[int(k)]
    return vs[f] + (vs[c]-vs[f]) * (k - f)

def _pick_sample_days(days_list, k):
    n = len(days_list)
    if n <= k: return list(days_list)
    if k <= 1: return [days_list[0]]
    idxs = {round(i * (n-1) / (k-1)) for i in range(k)}
    return [days_list[i] for i in sorted(idxs)]

def estimate_workload(days_list, sample_days=12, sample_core=20):
    sample_days = max(1, min(sample_days, len(days_list)))
    sampled_days = _pick_sample_days(days_list, sample_days)

    sb_times, sb_sizes, events_per_day = [], [], []
    sampled_event_ids = []

    for d in sampled_days:
        t0 = time.perf_counter()
        sb = http_get(BASE_SCOREBOARD, params={"dates": d.strftime("%Y%m%d")})
        sb_times.append(max(0.0, time.perf_counter() - t0))
        try:
            sb_sizes.append(len(json.dumps(sb, ensure_ascii=False)))
        except Exception:
            sb_sizes.append(0)
        evs = sb.get("events", []) or []
        events_per_day.append(len(evs))
        for e in evs:
            if len(sampled_event_ids) < sample_core and e.get("id") is not None:
                sampled_event_ids.append(str(e.get("id")))
        if len(sampled_event_ids) >= sample_core:
            break

    core_times, core_sizes, fights_per_event = [], [], []
    for eid in sampled_event_ids:
        t0 = time.perf_counter()
        ev = http_get(CORE_EVENT_TPL.format(eventId=eid))
        core_times.append(max(0.0, time.perf_counter() - t0))
        try:
            core_sizes.append(len(json.dumps(ev, ensure_ascii=False)))
        except Exception:
            core_sizes.append(0)
        comps = (ev.get("competitions") or []) if isinstance(ev, dict) else []
        fights_per_event.append(len(comps))

    days_total = len(days_list)
    events_per_day_avg = float(mean(events_per_day)) if events_per_day else 0.0
    fights_per_event_avg = float(mean(fights_per_event)) if fights_per_event else 0.0
    est_events_total = int(round(events_per_day_avg * days_total))
    est_requests_total = int(days_total + est_events_total)

    sb_avg_s = float(mean(sb_times)) if sb_times else 0.3
    core_avg_s = float(mean(core_times)) if core_times else 0.4
    sb_p90_s = float(_percentile(sb_times, 0.9) or sb_avg_s)
    core_p90_s = float(_percentile(core_times, 0.9) or core_avg_s)

    conc = max(1, int(MAX_CONC))
    batches_per_day_avg = math.ceil((events_per_day_avg or 0.0) / conc) if events_per_day_avg else 0

    eta_s_avg = days_total * (sb_avg_s + batches_per_day_avg * core_avg_s)
    eta_s_p90 = days_total * (sb_p90_s + batches_per_day_avg * core_p90_s)

    sb_avg_bytes = int(mean(sb_sizes)) if sb_sizes else 0
    core_avg_bytes = int(mean(core_sizes)) if core_sizes else 0
    est_total_bytes = days_total * sb_avg_bytes + est_events_total * core_avg_bytes

    return {
        "days_total": days_total,
        "sample_days": len(sampled_days),
        "sampled_core_events": len(sampled_event_ids),
        "avg_events_per_day": round(events_per_day_avg, 2),
        "avg_fights_per_event": round(fights_per_event_avg, 2) if fights_per_event else None,
        "estimated_events_total": est_events_total,
        "estimated_requests_total": est_requests_total,
        "max_concurrency": conc,
        "avg_latency_s": {"scoreboard": round(sb_avg_s, 3), "core_event": round(core_avg_s, 3)},
        "p90_latency_s": {"scoreboard": round(sb_p90_s, 3), "core_event": round(core_p90_s, 3)},
        "eta_seconds_avg": int(eta_s_avg),
        "eta_minutes_avg": round(eta_s_avg/60.0, 1),
        "eta_minutes_p90": round(eta_s_p90/60.0, 1),
        "avg_payload_bytes": {"scoreboard": sb_avg_bytes, "core_event": core_avg_bytes},
        "estimated_total_bytes": est_total_bytes,
        "estimated_total_megabytes": round(est_total_bytes/1_000_000.0, 2)
    }

# ===== BRONZE TABLES (Delta, external LOCATION) =====
spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.{DB_NAME}.espn_events (
  event_id STRING, event_date TIMESTAMP, event_year INT, event_name STRING,
  venue STRING, country STRING, status STRING, num_fights INT,
  ingestion_date DATE, run_id STRING, raw_payload STRING
) USING DELTA PARTITIONED BY (event_year) LOCATION '{PATH_EVENTS_DELTA}'
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.{DB_NAME}.espn_fights (
  competition_id STRING, event_id STRING, event_date TIMESTAMP, event_year INT,
  bout_name STRING, weight_class STRING, card_order INT, status STRING,
  fighter_a_name STRING, fighter_a_winner BOOLEAN, fighter_a_stats STRING,
  fighter_b_name STRING, fighter_b_winner BOOLEAN, fighter_b_stats STRING,
  ingestion_date DATE, run_id STRING, raw_payload STRING
) USING DELTA PARTITIONED BY (event_year) LOCATION '{PATH_FIGHTS_DELTA}'
""")

from pyspark.sql import functions as F

def rows_to_df(event_rows, fight_rows):
    event_schema_str = "event_id STRING, event_date STRING, event_name STRING, venue STRING, country STRING, status STRING, num_fights INT, raw_payload STRING"
    fight_schema_str = "competition_id STRING, event_id STRING, event_date STRING, bout_name STRING, weight_class STRING, card_order INT, status STRING, fighter_a_name STRING, fighter_a_winner BOOLEAN, fighter_a_stats STRING, fighter_b_name STRING, fighter_b_winner BOOLEAN, fighter_b_stats STRING, raw_payload STRING"
    df_e = spark.createDataFrame(event_rows, schema=event_schema_str) if event_rows else spark.createDataFrame([], event_schema_str)
    df_f = spark.createDataFrame(fight_rows,  schema=fight_schema_str)  if fight_rows  else spark.createDataFrame([], fight_schema_str)
    # Robust ISO8601 parsing for timestamps like 2025-02-15T21:00Z or with seconds
    def parse_iso_ts(col):
        return F.coalesce(
            F.to_timestamp(F.col(col), "yyyy-MM-dd'T'HH:mm:ssX"),
            F.to_timestamp(F.col(col), "yyyy-MM-dd'T'HH:mmX"),
            F.to_timestamp(F.regexp_replace(F.col(col), "Z$", "+00:00"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.regexp_replace(F.col(col), "Z$", "+00:00"), "yyyy-MM-dd'T'HH:mmXXX"),
            F.to_timestamp(F.regexp_replace(F.col(col), "T", " "), "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(F.regexp_replace(F.col(col), "T", " "), "yyyy-MM-dd HH:mm")
        )
    df_e = (df_e.withColumn("event_ts", parse_iso_ts("event_date"))
                 .withColumn("event_year",F.year("event_ts").cast("int"))
                 .withColumn("ingestion_date",F.to_date(F.lit(INGESTION_DATE)))
                 .withColumn("run_id",F.lit(RUN_ID))
                 .drop("event_date").withColumnRenamed("event_ts","event_date"))
    df_f = (df_f.withColumn("event_ts", parse_iso_ts("event_date"))
                 .withColumn("event_year",F.year("event_ts").cast("int"))
                 .withColumn("ingestion_date",F.to_date(F.lit(INGESTION_DATE)))
                 .withColumn("run_id",F.lit(RUN_ID))
                 .drop("event_date").withColumnRenamed("event_ts","event_date"))
    return df_e, df_f

def upsert_bronze(df_e, df_f):
    view_e = f"_s_events_{RUN_ID}"
    view_f = f"_s_fights_{RUN_ID}"
    df_e.createOrReplaceTempView(view_e)
    df_f.createOrReplaceTempView(view_f)
    spark.sql(f"""
        MERGE INTO hive_metastore.{DB_NAME}.espn_events t
        USING (SELECT * FROM {view_e}) s
        ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    spark.sql(f"""
        MERGE INTO hive_metastore.{DB_NAME}.espn_fights t
        USING (SELECT * FROM {view_f}) s
        ON t.competition_id = s.competition_id AND t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    spark.catalog.dropTempView(view_e)
    spark.catalog.dropTempView(view_f)

# ===== RUN =====
run_stats={"days":0,"events":0,"fights":0,"errors":0}
event_rows_all=[]; fight_rows_all=[]

try:
    # Build concrete list of days for this run
    if run_mode=="backfill":
        days_list = list(date_range_backfill(years_back))
    else:
        _wm = load_watermark()
        days_list = list(date_range_backfill(years_back)) if _wm is None else list(date_range_incremental(_wm, overlap_days))

    # Pre-run estimation (lightweight sampling)
    try:
        estimation = estimate_workload(days_list, EST_SAMPLE_DAYS, EST_SAMPLE_CORE)
        put_json(f"{PATH_LOGS_RUN}/run_id={RUN_ID}/estimation.json", estimation)
        display({"run_id": RUN_ID, "estimation": estimation})
    except Exception as _est_e:
        put_json(f"{PATH_LOGS_RUN}/run_id={RUN_ID}/estimation_error.json", {"error": str(_est_e)})

    printed_sample=False
    for day in days_list:
        try:
            erows, frows = fetch_day(day)
            event_rows_all.extend(erows); fight_rows_all.extend(frows)
            run_stats["days"] += 1
            if (not printed_sample) and erows:
                try:
                    first_event = erows[0]
                    feid = first_event.get("event_id")
                    fights_for_event = [r for r in frows if r.get("event_id") == feid]
                    sample_out = {
                        "sample_event": first_event,
                        "sample_fights": fights_for_event[:5],
                        "sample_fights_total": len(fights_for_event)
                    }
                    print(json.dumps(sample_out, ensure_ascii=False, indent=2))
                except Exception:
                    pass
                printed_sample=True
        except Exception as e:
            run_stats["errors"] += 1
            put_json(f"{PATH_LOGS_RUN}/run_id={RUN_ID}/error_day_{day.isoformat()}.json", {"error":str(e)})

    mirror_to_raw()
    df_e, df_f = rows_to_df(event_rows_all, fight_rows_all)
    run_stats["events"] = df_e.count(); run_stats["fights"] = df_f.count()
    upsert_bronze(df_e, df_f)

    if run_stats["events"]>0:
        max_dt = df_e.agg(F.max("event_date")).first()[0]
        if max_dt: save_watermark(max_dt.date())

    status="SUCCESS"

except Exception as e:
    status="FAILED"
    put_json(f"{PATH_LOGS_RUN}/run_id={RUN_ID}/fatal_error.json", {"error":str(e)})
    raise

finally:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS hive_metastore.{DB_NAME}.pipeline_runs (
      run_id STRING, started_at TIMESTAMP, finished_at TIMESTAMP, run_mode STRING,
      years_back INT, overlap_days INT, events_count BIGINT, fights_count BIGINT, errors BIGINT, status STRING
    ) USING DELTA LOCATION '{PATH_META_RUNS}'
    """)
    started = datetime.strptime(RUN_ID,"%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc); finished = datetime.now(timezone.utc)
    log_df = spark.createDataFrame([(
        RUN_ID, started, finished, run_mode, years_back, overlap_days,
        int(run_stats["events"]), int(run_stats["fights"]), int(run_stats["errors"]), status
    )], schema="run_id STRING, started_at TIMESTAMP, finished_at TIMESTAMP, run_mode STRING, years_back INT, overlap_days INT, events_count BIGINT, fights_count BIGINT, errors BIGINT, status STRING")
    log_df.coalesce(1).write.format("delta").mode("append").save(PATH_META_RUNS)

display({"run_id": RUN_ID, "status": status, **run_stats})
