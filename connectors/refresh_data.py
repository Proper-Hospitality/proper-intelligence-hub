"""
Proper Intelligence Hub — Snowflake Data Refresh Script

Connects to Snowflake, runs all dashboard queries for each property,
and exports the results to data/<property>.json files.

Usage:
    python refresh_data.py                    # Refresh all properties
    python refresh_data.py --property smp     # Refresh one property
    python refresh_data.py --dry-run          # Print queries without executing

Requirements:
    pip install snowflake-connector-python cryptography

Environment variables:
    SNOWFLAKE_ACCOUNT       — Snowflake account identifier
    SNOWFLAKE_USER          — Username
    SNOWFLAKE_PRIVATE_KEY   — Private key contents (PEM format)
    SNOWFLAKE_WAREHOUSE     — Warehouse name
    SNOWFLAKE_DATABASE      — Database name (default: PROPER_DW)
    SNOWFLAKE_SCHEMA        — Schema name (default: DASHBOARD)
"""

import json
import os
import sys
import argparse
from pathlib import Path
from datetime import datetime

try:
    import snowflake.connector
except ImportError:
    print("Install snowflake-connector-python: pip install snowflake-connector-python")
    sys.exit(1)

# Property configurations — must match config.js
PROPERTIES = {
    "smp": {
        "code": "SMP",
        "name": "Santa Monica Proper Hotel",
        "outlets": ["Calabra", "Palma", "In-Room Dining", "Pool F&B"],
        "survey_outlets": {
            "survey_calabra": "Calabra",
            "survey_surya": "Surya Spa",
            "survey_palma": "Palma",
            "survey_pool": "Rooftop Pool",
        },
        "comment_keys": {
            "calabra_comments": "Calabra",
            "surya_comments": "Surya Spa",
            "palma_comments": "Palma",
            "pool_comments": "Rooftop Pool",
            "ird_comments": "In-Room Dining",
        },
    },
    "austin": {
        "code": "AUS",
        "name": "Austin Proper Hotel",
        "outlets": ["Peacock", "La Piscina", "Goldies", "In-Room Dining"],
        "survey_outlets": {
            "survey_surya": "Spa",
        },
        "comment_keys": {
            "surya_comments": "Spa",
        },
    },
}

# Query definitions: key -> (view_name, extra_params)
# Views are defined in snowflake_queries.sql
QUERIES = {
    # Revenue
    "revenue_monthly": "V_REVENUE_MONTHLY",
    "revenue_by_segment": "V_REVENUE_BY_SEGMENT",
    "portfolio_comparison": "V_PORTFOLIO_COMPARISON",
    # Reviews
    "reviews_by_source": "V_REVIEWS_BY_SOURCE",
    "negative_reviews": "V_NEGATIVE_REVIEWS",
    "property_reviews_aggregate": "V_PROPERTY_REVIEWS_AGGREGATE",
    # Surveys
    "survey_overall": "V_SURVEY_OVERALL",
    "survey_issues": "V_SURVEY_ISSUES",
    "survey_by_channel": "V_SURVEY_BY_CHANNEL",
    "fb_outlet_ratings": "V_FB_OUTLET_RATINGS",
    # Group Sales
    "bookings_by_status": "V_BOOKINGS_BY_STATUS",
    "bookings_by_owner": "V_BOOKINGS_BY_OWNER",
    "bookings_monthly": "V_BOOKINGS_MONTHLY",
    "tripleseat_segments": "V_TRIPLESEAT_SEGMENTS",
    "sales_activities": "V_SALES_ACTIVITIES",
    # Comp Set
    "comp_set": "V_COMP_SET",
    # Glitches
    "glitch_reports": "V_GLITCH_REPORTS",
    "glitch_by_issue": "V_GLITCH_BY_ISSUE",
    "glitch_by_type": "V_GLITCH_BY_TYPE",
    "glitch_by_status": "V_GLITCH_BY_STATUS",
    "glitch_by_room": "V_GLITCH_BY_ROOM",
    "glitch_monthly": "V_GLITCH_MONTHLY",
    "glitch_compensation": "V_GLITCH_COMPENSATION",
    # F&B
    "fb_glitches": "V_FB_GLITCHES",
    "fb_reviews": "V_FB_REVIEWS",
    "fb_event_revenue": "V_FB_EVENT_REVENUE",
    # Channels
    "channel_performance": "V_CHANNEL_PERFORMANCE",
    "channel_monthly": "V_CHANNEL_MONTHLY",
    "lra_monthly": "V_LRA_MONTHLY",
    # Pace
    "pace_monthly": "V_PACE_MONTHLY",
    "pickup_by_segment": "V_PICKUP_BY_SEGMENT",
}


def get_private_key():
    """Load private key from environment variable (PEM format)."""
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    private_key_pem = os.environ["SNOWFLAKE_PRIVATE_KEY"]
    # Handle escaped newlines from GitHub secrets
    private_key_pem = private_key_pem.replace("\\n", "\n")

    private_key = serialization.load_pem_private_key(
        private_key_pem.encode("utf-8"),
        password=None,
        backend=default_backend(),
    )
    return private_key


def get_connection():
    """Create Snowflake connection using key-pair authentication."""
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=get_private_key(),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "PROPER_DW"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "DASHBOARD"),
    )


def run_query(cursor, view_name, property_code):
    """Run a query against a dashboard view and return {cols, rows}."""
    sql = f"SELECT * FROM {view_name} WHERE PROPERTY_CODE = %s"

    # Portfolio comparison doesn't filter by property
    if view_name == "V_PORTFOLIO_COMPARISON":
        sql = f"SELECT * FROM {view_name}"
        cursor.execute(sql)
    else:
        cursor.execute(sql, (property_code,))

    cols = [desc[0] for desc in cursor.description]
    rows = []
    for row in cursor.fetchall():
        rows.append([str(v) if v is not None else None for v in row])

    return {"cols": cols, "rows": rows}


def run_outlet_query(cursor, property_code, outlet_name):
    """Run outlet-specific survey query."""
    sql = """
        SELECT COMMENT_TEXT FROM V_OUTLET_COMMENTS
        WHERE PROPERTY_CODE = %s AND OUTLET_NAME = %s
    """
    cursor.execute(sql, (property_code, outlet_name))
    return {"cols": [], "rows": [[str(r[0])] for r in cursor.fetchall() if r[0]]}


def run_survey_outlet_query(cursor, property_code, outlet_name):
    """Run per-outlet survey rating query."""
    sql = """
        SELECT VISITS, FOOD, STAFF, SERVICE, MENU FROM V_SURVEY_OUTLET
        WHERE PROPERTY_CODE = %s AND OUTLET_NAME = %s
    """
    cursor.execute(sql, (property_code, outlet_name))
    rows = cursor.fetchall()
    if rows:
        return {"cols": [], "rows": [[str(v) if v is not None else None for v in rows[0]]]}
    return {"cols": [], "rows": []}


def refresh_property(conn, property_id, dry_run=False):
    """Refresh all data for a single property."""
    prop = PROPERTIES[property_id]
    property_code = prop["code"]
    data = {}

    cursor = conn.cursor()
    try:
        # Run standard queries
        for key, view_name in QUERIES.items():
            if dry_run:
                print(f"  [DRY RUN] {key} -> SELECT * FROM {view_name}")
                data[key] = {"cols": [], "rows": []}
                continue

            try:
                data[key] = run_query(cursor, view_name, property_code)
                print(f"  {key}: {len(data[key]['rows'])} rows")
            except Exception as e:
                print(f"  WARNING: {key} failed: {e}")
                data[key] = {"cols": [], "rows": []}

        # Run outlet comment queries
        for key, outlet_name in prop.get("comment_keys", {}).items():
            if dry_run:
                data[key] = {"cols": [], "rows": []}
                continue
            try:
                data[key] = run_outlet_query(cursor, property_code, outlet_name)
                print(f"  {key}: {len(data[key]['rows'])} rows")
            except Exception as e:
                print(f"  WARNING: {key} failed: {e}")
                data[key] = {"cols": [], "rows": []}

        # Run per-outlet survey queries
        for key, outlet_name in prop.get("survey_outlets", {}).items():
            if dry_run:
                data[key] = {"cols": [], "rows": []}
                continue
            try:
                data[key] = run_survey_outlet_query(cursor, property_code, outlet_name)
                print(f"  {key}: {len(data[key]['rows'])} rows")
            except Exception as e:
                print(f"  WARNING: {key} failed: {e}")
                data[key] = {"cols": [], "rows": []}

    finally:
        cursor.close()

    # Write output
    output_path = Path(__file__).parent.parent / "data" / f"{property_id}.json"
    with open(output_path, "w") as f:
        json.dump(data, f)
    size_kb = output_path.stat().st_size / 1024
    print(f"  -> Wrote {output_path} ({size_kb:.0f} KB)")


def main():
    parser = argparse.ArgumentParser(description="Refresh dashboard data from Snowflake")
    parser.add_argument("--property", choices=list(PROPERTIES.keys()), help="Refresh a single property")
    parser.add_argument("--dry-run", action="store_true", help="Print queries without executing")
    args = parser.parse_args()

    properties = [args.property] if args.property else list(PROPERTIES.keys())

    if args.dry_run:
        print("=== DRY RUN MODE ===\n")
        conn = None
    else:
        print(f"Connecting to Snowflake ({os.environ.get('SNOWFLAKE_ACCOUNT', 'NOT SET')})...")
        conn = get_connection()

    try:
        for pid in properties:
            print(f"\n{'='*60}")
            print(f"Refreshing {PROPERTIES[pid]['name']} ({PROPERTIES[pid]['code']})")
            print(f"{'='*60}")
            refresh_property(conn, pid, dry_run=args.dry_run)
    finally:
        if conn:
            conn.close()

    print(f"\nDone. Refreshed {len(properties)} properties at {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
