import yaml
from datetime import datetime, timezone, timedelta
from extract.navusoft_client import NavusoftClient
from control.snowflake_connection import SnowflakeConnection
from control.snowflake_watermarks import SnowflakeWatermarks
import snowflake.connector
import os
import json

# -----------------------------
# Force full reload flag
# -----------------------------
force_full_reload = True   # Full backfill toggle
watermark_overlap = timedelta(days=2)  # SOFT overlap window

# -----------------------------
# Load configuration
# -----------------------------
with open("config/navusoft.yml", "r") as f:
    cfg = yaml.safe_load(f)

# -----------------------------
# Initialize Snowflake connection & watermarks
# -----------------------------
sf_conn = SnowflakeConnection(cfg["snowflake"])
wm = SnowflakeWatermarks(sf_conn)
sf_cur = sf_conn.cursor()

# -----------------------------
# Initialize Navusoft client
# -----------------------------
client = NavusoftClient(
    base_url=cfg["api"]["base_url"],
    runquery_endpoint=cfg["api"]["runquery_endpoint"],
    sessionId=cfg["api"]["sessionId"]
)

# -----------------------------
# Prepare local folder for JSON
# -----------------------------
os.makedirs("output", exist_ok=True)
run_started_at = datetime.now(timezone.utc).isoformat()

# -----------------------------
# Snowflake stage & table
# -----------------------------
stage_name = "NAVUSOFT_STAGE"
sf_schema = "NAVUSOFT"
raw_table = "NAVUSOFT_RAW_ENTITIES"

# Ensure stage exists
sf_cur.execute(f"CREATE STAGE IF NOT EXISTS {sf_schema}.{stage_name}")

# -----------------------------
# Fetch entities and dump JSON locally
# -----------------------------
staged_files = []  # keep track of files to upload
total_rows = 0

for entity_cfg in cfg["entities"]:
    entity = entity_cfg["name"]
    columns = entity_cfg.get("columns", ["id"])
    incr_field = entity_cfg.get("incremental_field")

    # -----------------------------
    # Soft watermark lookup
    # -----------------------------
    soft_watermark = None
    if incr_field and not force_full_reload:
        soft_watermark = wm.get_soft(
            source_system="NAVUSOFT",
            entity_name=entity,
            overlap=watermark_overlap
        )

    # -----------------------------
    # Determine load mode (INFORMATIONAL ONLY)
    # -----------------------------
    if incr_field and soft_watermark and not force_full_reload:
        load_mode = "INCREMENTAL_SOFT"
        filter_expr = f"{incr_field} >= '{soft_watermark}'"
    else:
        load_mode = "FULL"
        filter_expr = None

    print(f"\n▶ Fetching {entity}")
    print(f"  Mode: {load_mode}")
    print(f"  Soft watermark: {soft_watermark}")

    # -----------------------------
    # Fetch data (NEVER hard limited)
    # -----------------------------
    rows = client.fetch(
        entity=entity,
        entity_columns=columns,
        filter_expr=filter_expr
    )

    if not rows:
        print(f"  No rows returned for {entity}")
        continue

    total_rows += len(rows)

    # -----------------------------
    # Optional truncate on FULL reload
    # -----------------------------
    if force_full_reload:
        print(f"  ⚠ Full reload: clearing existing raw rows for {entity}")
        sf_cur.execute(f"""
            DELETE FROM {sf_schema}.{raw_table}
            WHERE ENTITY_NAME = '{entity}'
        """)

    # -----------------------------
    # Write JSON Lines
    # -----------------------------
    output_path = f"output/{entity}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        for row in rows:
            record = {
                "_source": "navusoft",
                "_entity": entity,
                "_load_mode": load_mode,
                "_ingested_at": run_started_at,
                "payload": row
            }
            f.write(json.dumps(record) + "\n")

    print(f"  Wrote {len(rows)} rows to {output_path}")
    staged_files.append((entity, output_path))

    # -----------------------------
    # Record observed max watermark (SAFE)
    # -----------------------------
    if incr_field:
        incr_values = [
            r.get(incr_field)
            for r in rows
            if r.get(incr_field) is not None
        ]

        if incr_values:
            observed_max = max(incr_values)
            wm.record_observed_max(
                source_system="NAVUSOFT",
                entity_name=entity,
                incremental_field=incr_field,
                observed_max_value=observed_max
            )
            print(f"  Recorded observed max watermark → {observed_max}")
        else:
            print("  ⚠ No incremental values found — watermark not updated")

# -----------------------------
# PUT + COPY INTO Snowflake raw table
# -----------------------------
for entity, filepath in staged_files:
    # PUT file into stage
    sf_cur.execute(
        f"PUT file://{os.path.abspath(filepath)} @{sf_schema}.{stage_name}/{entity} OVERWRITE = TRUE"
    )

    # COPY INTO raw table
    sf_cur.execute(f"""
        COPY INTO {sf_schema}.{raw_table} (SOURCE_SYSTEM, ENTITY_NAME, LOAD_MODE, INGESTED_AT, PAYLOAD)
        FROM (
            SELECT
                $1:_source,
                $1:_entity,
                $1:_load_mode,
                $1:_ingested_at,
                $1:payload
            FROM @{sf_schema}.{stage_name}/{entity}
        )
        FILE_FORMAT = (TYPE = 'JSON')
        ON_ERROR = 'CONTINUE'
    """)
    print(f"✅ {entity} loaded into {sf_schema}.{raw_table} from stage")

# -----------------------------
# Close Snowflake connection
# -----------------------------
sf_cur.close()
sf_conn.close()

# -----------------------------
# Done
# -----------------------------
print(f"\n✅ Pipeline completed successfully.")
print(f"   Total rows ingested: {total_rows}")
