"""
Export dashboard data for ALL Proper Hospitality properties from Snowflake.
Writes one JSON file per property to data/<property_id>.json.
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

PRIVATE_KEY_PATH = "/Users/mike.thomas/Library/Containers/com.microsoft.Outlook/Data/tmp/Outlook Temp/perplexity_computer_use.p8"

# Property mapping: id -> (duetto_code, revinate_id, tripleseat_name, glitch_name, display_name)
PROPERTIES = {
    "smp": {
        "duetto_code": "SM_PROPER",
        "revinate_id": "sm_proper",
        "tripleseat": "Santa Monica Proper Hotel",
        "glitch": "Santa Monica Proper Hotel",
        "name": "Santa Monica Proper Hotel",
        "survey_table": "FACT_SURVEY_SM_PROPER",
    },
    "austin": {
        "duetto_code": "AUSTIN_PROPER",
        "revinate_id": "austin_proper",
        "tripleseat": "Austin Proper Hotel and Residences",
        "glitch": "Austin Proper",
        "name": "Austin Proper Hotel",
        "survey_table": "FACT_SURVEY_AUSTIN_PROPER",
    },
    "shelborne": {
        "duetto_code": "SHELBORNE",
        "revinate_id": "shelborne",
        "tripleseat": "The Shelborne by Proper",
        "glitch": "Shelborne South Beach",
        "name": "The Shelborne By Proper",
        "survey_table": "FACT_SURVEY_SHELBORNE",
    },
    "sf_proper": {
        "duetto_code": "SF_PROPER",
        "revinate_id": "sf_proper",
        "tripleseat": "San Francisco Proper Hotel",
        "glitch": "San Francisco Proper Hotel",
        "name": "San Francisco Proper Hotel",
        "survey_table": "FACT_SURVEY_SF_PROPER",
    },
    "dtla": {
        "duetto_code": "PROPER_DTLA",
        "revinate_id": "dtla_proper",
        "tripleseat": "Downtown LA Proper Hotel",
        "glitch": "Proper DTLA",
        "name": "Proper Downtown Los Angeles",
        "survey_table": "FACT_SURVEY_DTLA_PROPER",
    },
    "hotel_june": {
        "duetto_code": "JUNE_WEST_LA",
        "revinate_id": "hotel_june",
        "tripleseat": "Hotel June West LA",
        "glitch": "Hotel June LA",
        "name": "Hotel June West LA",
        "survey_table": "FACT_SURVEY_HOTEL_JUNE",
    },
    "june_malibu": {
        "duetto_code": "JUNE_MALIBU",
        "revinate_id": "hotel_june_malibu",
        "tripleseat": None,
        "glitch": None,
        "name": "Hotel June Malibu",
        "survey_table": None,
    },
    "avalon_ps": {
        "duetto_code": "AVALON_PS",
        "revinate_id": "avalon_ps",
        "tripleseat": "Avalon Hotel & Bungalows Palm Springs",
        "glitch": "Avalon Hotel and Bungalows - Palm Springs",
        "name": "Avalon Hotel and Bungalows Palm Springs",
        "survey_table": "FACT_SURVEY_AVALON_PS",
    },
    "avalon_bh": {
        "duetto_code": "AVALON_BH",
        "revinate_id": "avalon_bh",
        "tripleseat": "Avalon Hotel Beverly Hills",
        "glitch": "Avalon Beverly Hills",
        "name": "Avalon Hotel Beverly Hills",
        "survey_table": "FACT_SURVEY_AVALON_BH",
    },
    "culver": {
        "duetto_code": "CULVER",
        "revinate_id": "culver_hotel",
        "tripleseat": "The Culver Hotel",
        "glitch": "The Culver Hotel",
        "name": "The Culver Hotel",
        "survey_table": "FACT_SURVEY_CULVER_HOTEL",
    },
    "montauk": {
        "duetto_code": "MONTAUK_YC",
        "revinate_id": "myc",
        "tripleseat": "Montauk Yacht Club",
        "glitch": None,
        "name": "Montauk Yacht Club",
        "survey_table": "FACT_SURVEY_MYC",
    },
    "ingleside": {
        "duetto_code": "INGLESIDE",
        "revinate_id": "ingleside_inn",
        "tripleseat": "Ingleside Estate",
        "glitch": "Ingleside Estate",
        "name": "Ingleside Estate",
        "survey_table": "FACT_SURVEY_INGLESIDE_INN",
    },
}


def connect():
    with open(PRIVATE_KEY_PATH, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )
    return snowflake.connector.connect(
        account="EEDSXGV-WYB75642",
        user="PERPLEXITY_SERVICE_KEY",
        private_key=private_key,
        role="PERPLEXITY_COMPUTER",
        warehouse="COMPUTE_WH",
    )


def query(cur, sql, params=None):
    """Run query and return {cols, rows} with all values as strings."""
    cur.execute(sql, params or [])
    cols = [d[0] for d in cur.description]
    rows = []
    for row in cur.fetchall():
        rows.append([str(v) if v is not None else None for v in row])
    return {"cols": cols, "rows": rows}


def safe_query(cur, sql, params=None, label=""):
    """Run query, return empty result on error."""
    try:
        return query(cur, sql, params)
    except Exception as e:
        print(f"    WARNING [{label}]: {e}")
        return {"cols": [], "rows": []}


def export_property(cur, pid, prop):
    """Export all data for one property."""
    dc = prop["duetto_code"]
    rid = prop["revinate_id"]
    ts = prop["tripleseat"]
    gl = prop["glitch"]
    data = {}

    # === REVENUE (Duetto) ===
    print(f"  Revenue...")
    data["revenue_monthly"] = safe_query(cur, """
        SELECT STAY_DATE, TODAY_ROOMS, TODAY_ADR, TODAY_ROOM_REVENUE,
               BUDGET_ROOMS, BUDGET_ADR, BUDGET_ROOM_REVENUE,
               STLY_ROOMS, STLY_ADR, STLY_ROOM_REVENUE,
               FORECAST_ROOMS, FORECAST_ADR, FORECAST_ROOM_REVENUE
        FROM DUETTO_UPLOAD.RAW.DUETTO_BUDGET_VS_PY
        WHERE HOTEL_CODE = %s
          AND MARKET_SEGMENT = 'Total'
          AND TRY_TO_DATE(STAY_DATE, 'YYYYMMDD') >= DATEADD(YEAR, -1, CURRENT_DATE())
          AND TRY_TO_DATE(STAY_DATE, 'YYYYMMDD') <= CURRENT_DATE()
        ORDER BY STAY_DATE
    """, [dc], "revenue_monthly")

    data["revenue_by_segment"] = safe_query(cur, """
        SELECT MARKET_SEGMENT,
               SUM(TODAY_ROOMS) AS ROOMS,
               CASE WHEN SUM(TODAY_ROOMS) > 0 THEN SUM(TODAY_ROOM_REVENUE)/SUM(TODAY_ROOMS) ELSE 0 END AS ADR,
               SUM(TODAY_ROOM_REVENUE) AS REVENUE,
               SUM(STLY_ROOMS) AS STLY_ROOMS,
               SUM(STLY_ROOM_REVENUE) AS STLY_REVENUE
        FROM DUETTO_UPLOAD.RAW.DUETTO_BUDGET_VS_PY
        WHERE HOTEL_CODE = %s
          AND MARKET_SEGMENT != 'Total'
          AND TRY_TO_DATE(STAY_DATE, 'YYYYMMDD') >= DATE_TRUNC('YEAR', CURRENT_DATE())
          AND TRY_TO_DATE(STAY_DATE, 'YYYYMMDD') <= CURRENT_DATE()
        GROUP BY MARKET_SEGMENT
        ORDER BY REVENUE DESC
    """, [dc], "revenue_by_segment")

    # Portfolio comparison (all properties)
    data["portfolio_comparison"] = safe_query(cur, """
        SELECT HOTEL_NAME,
               SUM(TODAY_ROOMS) AS ROOMS,
               CASE WHEN SUM(TODAY_ROOMS) > 0 THEN SUM(TODAY_ROOM_REVENUE)/SUM(TODAY_ROOMS) ELSE 0 END AS ADR,
               SUM(TODAY_ROOM_REVENUE) AS REVENUE,
               SUM(BUDGET_ROOM_REVENUE) AS BUDGET_REV,
               CASE WHEN SUM(BUDGET_ROOM_REVENUE) > 0
                    THEN (SUM(TODAY_ROOM_REVENUE)-SUM(BUDGET_ROOM_REVENUE))/SUM(BUDGET_ROOM_REVENUE)*100
                    ELSE 0 END AS VS_BUD,
               SUM(STLY_ROOM_REVENUE) AS STLY_REV,
               CASE WHEN SUM(STLY_ROOM_REVENUE) > 0
                    THEN (SUM(TODAY_ROOM_REVENUE)-SUM(STLY_ROOM_REVENUE))/SUM(STLY_ROOM_REVENUE)*100
                    ELSE 0 END AS VS_STLY
        FROM DUETTO_UPLOAD.RAW.DUETTO_BUDGET_VS_PY
        WHERE MARKET_SEGMENT = 'Total'
          AND HOTEL_CODE != 'UNKNOWN'
          AND TRY_TO_DATE(STAY_DATE, 'YYYYMMDD') >= DATE_TRUNC('YEAR', CURRENT_DATE())
          AND TRY_TO_DATE(STAY_DATE, 'YYYYMMDD') <= CURRENT_DATE()
        GROUP BY HOTEL_NAME
        ORDER BY REVENUE DESC
    """, label="portfolio_comparison")

    # === REVIEWS (Revinate) ===
    print(f"  Reviews...")
    data["reviews_by_source"] = safe_query(cur, """
        SELECT SOURCE,
               COUNT(*) AS CNT,
               AVG(TRY_CAST(OVERALL_RATING AS FLOAT)) AS AVG_RATING,
               AVG(TRY_CAST(SUB_RATING_CLEANLINESS AS FLOAT)) AS CLEAN,
               AVG(TRY_CAST(SUB_RATING_SERVICE AS FLOAT)) AS SERVICE,
               AVG(TRY_CAST(SUB_RATING_ROOMS AS FLOAT)) AS ROOMS,
               AVG(TRY_CAST(SUB_RATING_VALUE AS FLOAT)) AS VALUE,
               AVG(TRY_CAST(SUB_RATING_LOCATION AS FLOAT)) AS LOCATION,
               SUM(CASE WHEN HAS_RESPONSE = 'true' THEN 1 ELSE 0 END) AS RESPONDED,
               MIN(TRY_CAST(OVERALL_RATING AS FLOAT)) AS MIN_R,
               MAX(TRY_CAST(OVERALL_RATING AS FLOAT)) AS MAX_R
        FROM CORE_REVINATE.PROD.FACT_REVIEWS
        WHERE PROPERTY_ID = %s
          AND TRY_TO_DATE(REVIEW_DATE) >= DATEADD(MONTH, -12, CURRENT_DATE())
        GROUP BY SOURCE
        ORDER BY CNT DESC
    """, [rid], "reviews_by_source")

    data["negative_reviews"] = safe_query(cur, """
        SELECT SOURCE, REVIEW_DATE, OVERALL_RATING, REVIEW_TITLE, REVIEW_TEXT,
               HAS_RESPONSE, RESPONSE_TEXT
        FROM CORE_REVINATE.PROD.FACT_REVIEWS
        WHERE PROPERTY_ID = %s
          AND TRY_CAST(OVERALL_RATING AS FLOAT) <= 3
          AND TRY_TO_DATE(REVIEW_DATE) >= DATEADD(MONTH, -6, CURRENT_DATE())
        ORDER BY TRY_TO_DATE(REVIEW_DATE) DESC
        LIMIT 50
    """, [rid], "negative_reviews")

    data["property_reviews_aggregate"] = safe_query(cur, """
        SELECT HOTEL_NAME,
               COUNT(*) AS REVIEW_COUNT,
               AVG(TRY_CAST(OVERALL_RATING AS FLOAT)) AS OVERALL,
               AVG(TRY_CAST(SUB_RATING_CLEANLINESS AS FLOAT)) AS CLEANLINESS,
               AVG(TRY_CAST(SUB_RATING_SERVICE AS FLOAT)) AS SERVICE,
               AVG(TRY_CAST(SUB_RATING_ROOMS AS FLOAT)) AS ROOMS,
               AVG(TRY_CAST(SUB_RATING_VALUE AS FLOAT)) AS VALUE
        FROM CORE_REVINATE.PROD.FACT_REVIEWS
        WHERE PROPERTY_ID = %s
          AND TRY_TO_DATE(REVIEW_DATE) >= DATEADD(MONTH, -12, CURRENT_DATE())
        GROUP BY HOTEL_NAME
    """, [rid], "property_reviews_aggregate")

    # Comp set
    data["comp_set"] = safe_query(cur, """
        SELECT COMPETITOR_NAME,
               COUNT(*) AS REVIEWS,
               AVG(TRY_CAST(OVERALL_RATING AS FLOAT)) AS OVERALL,
               AVG(TRY_CAST(SUB_RATING_CLEANLINESS AS FLOAT)) AS CLEANLINESS,
               AVG(TRY_CAST(SUB_RATING_SERVICE AS FLOAT)) AS SERVICE,
               AVG(TRY_CAST(SUB_RATING_ROOMS AS FLOAT)) AS ROOMS,
               AVG(TRY_CAST(SUB_RATING_VALUE AS FLOAT)) AS VALUE
        FROM CORE_REVINATE.PROD.FACT_COMPETITOR_REVIEWS
        WHERE PROPERTY_ID = %s
          AND TRY_TO_DATE(REVIEW_DATE) >= DATEADD(MONTH, -12, CURRENT_DATE())
        GROUP BY COMPETITOR_NAME
        ORDER BY OVERALL DESC
    """, [rid], "comp_set")

    # F&B reviews
    data["fb_reviews"] = safe_query(cur, """
        SELECT SOURCE, REVIEW_DATE, OVERALL_RATING, REVIEW_TEXT
        FROM CORE_REVINATE.PROD.FACT_REVIEWS
        WHERE PROPERTY_ID = %s
          AND (LOWER(REVIEW_TEXT) LIKE '%%restaurant%%'
            OR LOWER(REVIEW_TEXT) LIKE '%%food%%'
            OR LOWER(REVIEW_TEXT) LIKE '%%breakfast%%'
            OR LOWER(REVIEW_TEXT) LIKE '%%dinner%%'
            OR LOWER(REVIEW_TEXT) LIKE '%%bar%%'
            OR LOWER(REVIEW_TEXT) LIKE '%%cocktail%%'
            OR LOWER(REVIEW_TEXT) LIKE '%%room service%%')
          AND TRY_TO_DATE(REVIEW_DATE) >= DATEADD(MONTH, -6, CURRENT_DATE())
        ORDER BY TRY_TO_DATE(REVIEW_DATE) DESC
        LIMIT 30
    """, [rid], "fb_reviews")

    # === SURVEY OUTLET RATINGS ===
    print(f"  Survey outlets...")
    data["fb_outlet_ratings"] = safe_query(cur, """
        SELECT o.OUTLET_NAME AS OUTLET,
               SUM(CASE WHEN r.VISITED = 'Yes' THEN 1 ELSE 0 END) AS VISITS,
               AVG(TRY_CAST(r.FANDB_QUALITY AS FLOAT)) AS FOOD,
               AVG(TRY_CAST(r.STAFF AS FLOAT)) AS STAFF,
               AVG(TRY_CAST(r.SERVICE AS FLOAT)) AS SERVICE,
               AVG(TRY_CAST(r.MENU AS FLOAT)) AS MENU
        FROM CORE_REVINATE.PROD.FACT_SURVEY_OUTLET_RATINGS r
        JOIN CORE_REVINATE.PROD.DIM_SURVEY_OUTLETS o ON r.OUTLET_ID = o.OUTLET_ID
        WHERE o.PROPERTY_ID = %s
        GROUP BY o.OUTLET_NAME
    """, [rid], "fb_outlet_ratings")

    # Outlet comments
    for outlet_key in ["calabra_comments", "palma_comments", "ird_comments", "pool_comments", "surya_comments"]:
        data[outlet_key] = {"cols": [], "rows": []}

    outlet_comment_sql = """
        SELECT r.COMMENT
        FROM CORE_REVINATE.PROD.FACT_SURVEY_OUTLET_RATINGS r
        JOIN CORE_REVINATE.PROD.DIM_SURVEY_OUTLETS o ON r.OUTLET_ID = o.OUTLET_ID
        WHERE o.PROPERTY_ID = %s AND o.OUTLET_NAME = %s
          AND r.COMMENT IS NOT NULL AND r.COMMENT != ''
        LIMIT 20
    """

    # Get actual outlet names for this property
    outlet_names = safe_query(cur, """
        SELECT OUTLET_NAME FROM CORE_REVINATE.PROD.DIM_SURVEY_OUTLETS
        WHERE PROPERTY_ID = %s
    """, [rid], "outlet_names")

    # Map outlet names to comment keys dynamically
    for row in outlet_names.get("rows", []):
        oname = row[0]
        lower = oname.lower()
        key = None
        if "calabra" in lower: key = "calabra_comments"
        elif "palma" in lower: key = "palma_comments"
        elif "in-room" in lower or "in room" in lower: key = "ird_comments"
        elif "pool" in lower or "piscina" in lower or "swim" in lower: key = "pool_comments"
        elif "spa" in lower or "surya" in lower: key = "surya_comments"
        elif "peacock" in lower: key = "calabra_comments"  # map primary restaurant
        elif "goldie" in lower: key = "palma_comments"
        elif "villon" in lower or "viviane" in lower or "chi chi" in lower: key = "calabra_comments"
        elif "charmaine" in lower or "velvet" in lower: key = "palma_comments"
        elif "scenic" in lower or "caravan" in lower: key = "calabra_comments"
        elif "melvyn" in lower: key = "calabra_comments"
        elif "ocean" in lower: key = "calabra_comments"
        elif "café" in lower or "cafe" in lower: key = "calabra_comments"
        elif "pauline" in lower or "little torch" in lower: key = "palma_comments"
        elif "terrace" in lower: key = "pool_comments"
        elif "culver" in lower: key = "calabra_comments"
        elif "kappo" in lower: key = "palma_comments"
        elif "quill" in lower: key = "pool_comments"

        if key:
            result = safe_query(cur, outlet_comment_sql, [rid, oname], f"comments_{oname}")
            if result["rows"]:
                data[key] = result

    # === SURVEYS ===
    print(f"  Surveys...")
    survey_table = prop.get("survey_table")
    if survey_table:
        data["survey_overall"] = safe_query(cur, f"""
            SELECT COUNT(*) AS TOTAL,
                   AVG(TRY_CAST(OVERALL_EXPERIENCE AS FLOAT)) AS AVG_RATING,
                   AVG(TRY_CAST(NPS AS FLOAT)) AS AVG_NPS,
                   AVG(TRY_CAST(GUESTROOM_CLEANLINESS AS FLOAT)) AS ROOM_CLEAN,
                   AVG(TRY_CAST(GUESTROOM_COMFORT AS FLOAT)) AS ROOM_COMFORT,
                   AVG(TRY_CAST(ARRIVAL_EXPERIENCE AS FLOAT)) AS ARRIVAL,
                   AVG(TRY_CAST(DEPARTURE_EXPERIENCE AS FLOAT)) AS DEPARTURE,
                   AVG(TRY_CAST(SERVICE_RATING AS FLOAT)) AS SERVICE
            FROM CORE_REVINATE.SURVEYS.{survey_table}
        """, label="survey_overall")

        data["survey_issues"] = safe_query(cur, f"""
            SELECT ANYTHING_BETTER_CATEGORY AS ISSUE, COUNT(*) AS CNT
            FROM CORE_REVINATE.SURVEYS.{survey_table}
            WHERE ANYTHING_BETTER_CATEGORY IS NOT NULL AND ANYTHING_BETTER_CATEGORY != ''
            GROUP BY ANYTHING_BETTER_CATEGORY
            ORDER BY CNT DESC
            LIMIT 20
        """, label="survey_issues")

        data["survey_by_channel"] = safe_query(cur, f"""
            SELECT CHANNEL,
                   COUNT(*) AS SURVEYS,
                   AVG(TRY_CAST(OVERALL_EXPERIENCE AS FLOAT)) AS AVG_RATING,
                   AVG(TRY_CAST(NPS AS FLOAT)) AS AVG_NPS
            FROM CORE_REVINATE.SURVEYS.{survey_table}
            WHERE CHANNEL IS NOT NULL AND CHANNEL != ''
            GROUP BY CHANNEL
            ORDER BY SURVEYS DESC
        """, label="survey_by_channel")

        # Survey outlet-specific data (varies by property)
        for outlet_prefix in ["survey_calabra", "survey_surya", "survey_palma", "survey_pool"]:
            data[outlet_prefix] = {"cols": [], "rows": []}
        # Try to pull property-specific outlet survey data
        _extract_survey_outlets(cur, survey_table, data)
    else:
        for k in ["survey_overall", "survey_issues", "survey_by_channel",
                   "survey_calabra", "survey_surya", "survey_palma", "survey_pool"]:
            data[k] = {"cols": [], "rows": []}

    # === GROUP SALES (Tripleseat) ===
    print(f"  Group Sales...")
    if ts:
        data["bookings_by_status"] = safe_query(cur, """
            SELECT STATUS,
                   COUNT(*) AS BOOKINGS,
                   SUM(TOTAL_GRAND_TOTAL) AS TOTAL_REVENUE,
                   AVG(TOTAL_GRAND_TOTAL) AS AVG_REVENUE,
                   SUM(CASE WHEN STATUS = 'Definite' THEN 1 ELSE 0 END) AS DEFINITE
            FROM ROSEDALE_DATABASE.TRIPLESEAT.TRIPLESEAT_BOOKINGS
            WHERE LOCATION_NAME = %s AND DELETED_AT IS NULL
              AND START_DATE >= DATE_TRUNC('YEAR', CURRENT_DATE())
            GROUP BY STATUS
            ORDER BY TOTAL_REVENUE DESC
        """, [ts], "bookings_by_status")

        data["bookings_by_owner"] = safe_query(cur, """
            SELECT OWNED_BY_NAME AS OWNER,
                   COUNT(*) AS BOOKINGS,
                   SUM(TOTAL_GRAND_TOTAL) AS REVENUE,
                   SUM(CASE WHEN STATUS = 'Definite' THEN 1 ELSE 0 END) AS DEFINITE,
                   SUM(CASE WHEN STATUS = 'Tentative' THEN 1 ELSE 0 END) AS TENTATIVE,
                   AVG(TOTAL_GRAND_TOTAL) AS AVG_DEAL
            FROM ROSEDALE_DATABASE.TRIPLESEAT.TRIPLESEAT_BOOKINGS
            WHERE LOCATION_NAME = %s AND DELETED_AT IS NULL
              AND START_DATE >= DATE_TRUNC('YEAR', CURRENT_DATE())
            GROUP BY OWNED_BY_NAME
            ORDER BY REVENUE DESC
        """, [ts], "bookings_by_owner")

        data["bookings_monthly"] = safe_query(cur, """
            SELECT TO_CHAR(START_DATE, 'YYYY-MM') AS MONTH,
                   COUNT(*) AS BOOKINGS,
                   SUM(TOTAL_GRAND_TOTAL) AS TOTAL_REVENUE,
                   SUM(CASE WHEN STATUS = 'Definite' THEN 1 ELSE 0 END) AS DEFINITE_CNT,
                   SUM(CASE WHEN STATUS = 'Definite' THEN TOTAL_GRAND_TOTAL ELSE 0 END) AS DEFINITE_REV
            FROM ROSEDALE_DATABASE.TRIPLESEAT.TRIPLESEAT_BOOKINGS
            WHERE LOCATION_NAME = %s AND DELETED_AT IS NULL
              AND START_DATE >= DATEADD(MONTH, -14, CURRENT_DATE())
            GROUP BY TO_CHAR(START_DATE, 'YYYY-MM')
            ORDER BY MONTH
        """, [ts], "bookings_monthly")

        data["tripleseat_segments"] = safe_query(cur, """
            SELECT COALESCE(MARKET_SEGMENT, 'Uncategorized') AS SEGMENT,
                   COUNT(*) AS BOOKINGS,
                   SUM(TOTAL_GRAND_TOTAL) AS REVENUE,
                   AVG(TOTAL_GRAND_TOTAL) AS AVG_REVENUE,
                   SUM(CASE WHEN STATUS = 'Definite' THEN 1 ELSE 0 END) AS DEFINITE
            FROM ROSEDALE_DATABASE.TRIPLESEAT.TRIPLESEAT_BOOKINGS
            WHERE LOCATION_NAME = %s AND DELETED_AT IS NULL
              AND START_DATE >= DATE_TRUNC('YEAR', CURRENT_DATE())
            GROUP BY MARKET_SEGMENT
            ORDER BY REVENUE DESC
        """, [ts], "tripleseat_segments")

        data["sales_activities"] = safe_query(cur, """
            SELECT 'Total Activities' AS LABEL, COUNT(*) AS CNT
            FROM ROSEDALE_DATABASE.TRIPLESEAT.TRIPLESEAT_ACTIVITY
            WHERE LOCATION_NAME = %s
        """, [ts], "sales_activities")

        data["fb_event_revenue"] = safe_query(cur, """
            SELECT TO_CHAR(START_DATE, 'YYYY-MM') AS MONTH,
                   COUNT(*) AS EVENTS,
                   SUM(TOTAL_EVENT_GRAND_TOTAL) AS FB_REVENUE
            FROM ROSEDALE_DATABASE.TRIPLESEAT.TRIPLESEAT_BOOKINGS
            WHERE LOCATION_NAME = %s AND DELETED_AT IS NULL
              AND TOTAL_EVENT_GRAND_TOTAL > 0
              AND START_DATE >= DATEADD(MONTH, -14, CURRENT_DATE())
            GROUP BY TO_CHAR(START_DATE, 'YYYY-MM')
            ORDER BY MONTH
        """, [ts], "fb_event_revenue")
    else:
        for k in ["bookings_by_status", "bookings_by_owner", "bookings_monthly",
                   "tripleseat_segments", "sales_activities", "fb_event_revenue"]:
            data[k] = {"cols": [], "rows": []}

    # === GLITCH REPORTS ===
    print(f"  Glitches...")
    if gl:
        data["glitch_reports"] = safe_query(cur, """
            SELECT REFERENCE_ID, DATE, ROOM_NUMBER, GUEST_NAME, TYPE,
                   GLITCH_ISSUE, GLITCH_DESCRIPTION, COMPENSATION,
                   RESOLUTION_SATISFACTION, STATUS
            FROM DUETTO_UPLOAD.RAW.GLITCH_REPORTS_RAW
            WHERE PROPERTY = %s
            ORDER BY TRY_TO_DATE(DATE) DESC
        """, [gl], "glitch_reports")

        data["glitch_by_issue"] = safe_query(cur, """
            SELECT GLITCH_ISSUE, COUNT(*) AS CNT
            FROM DUETTO_UPLOAD.RAW.GLITCH_REPORTS_RAW
            WHERE PROPERTY = %s AND GLITCH_ISSUE IS NOT NULL
            GROUP BY GLITCH_ISSUE ORDER BY CNT DESC LIMIT 20
        """, [gl], "glitch_by_issue")

        data["glitch_by_type"] = safe_query(cur, """
            SELECT TYPE, COUNT(*) AS CNT
            FROM DUETTO_UPLOAD.RAW.GLITCH_REPORTS_RAW
            WHERE PROPERTY = %s AND TYPE IS NOT NULL
            GROUP BY TYPE ORDER BY CNT DESC
        """, [gl], "glitch_by_type")

        data["glitch_by_status"] = safe_query(cur, """
            SELECT STATUS, COUNT(*) AS CNT
            FROM DUETTO_UPLOAD.RAW.GLITCH_REPORTS_RAW
            WHERE PROPERTY = %s
            GROUP BY STATUS
        """, [gl], "glitch_by_status")

        data["glitch_by_room"] = safe_query(cur, """
            SELECT ROOM_NUMBER, COUNT(*) AS INCIDENTS, COUNT(DISTINCT GLITCH_ISSUE) AS UNIQUE_ISSUES
            FROM DUETTO_UPLOAD.RAW.GLITCH_REPORTS_RAW
            WHERE PROPERTY = %s AND ROOM_NUMBER IS NOT NULL
            GROUP BY ROOM_NUMBER HAVING COUNT(*) >= 2
            ORDER BY INCIDENTS DESC LIMIT 20
        """, [gl], "glitch_by_room")

        data["glitch_monthly"] = safe_query(cur, """
            SELECT LEFT(DATE, 7) AS MONTH, COUNT(*) AS CNT
            FROM DUETTO_UPLOAD.RAW.GLITCH_REPORTS_RAW
            WHERE PROPERTY = %s
            GROUP BY LEFT(DATE, 7) ORDER BY MONTH
        """, [gl], "glitch_monthly")

        data["glitch_compensation"] = safe_query(cur, """
            SELECT COMPENSATION, COUNT(*) AS CNT
            FROM DUETTO_UPLOAD.RAW.GLITCH_REPORTS_RAW
            WHERE PROPERTY = %s AND COMPENSATION IS NOT NULL AND COMPENSATION != ''
            GROUP BY COMPENSATION ORDER BY CNT DESC LIMIT 15
        """, [gl], "glitch_compensation")

        data["fb_glitches"] = safe_query(cur, """
            SELECT DATE, ROOM_NUMBER, GLITCH_ISSUE, GLITCH_DESCRIPTION, COMPENSATION, STATUS
            FROM DUETTO_UPLOAD.RAW.GLITCH_REPORTS_RAW
            WHERE PROPERTY = %s AND LOWER(TYPE) LIKE '%%f&b%%'
            ORDER BY TRY_TO_DATE(DATE) DESC
        """, [gl], "fb_glitches")
    else:
        for k in ["glitch_reports", "glitch_by_issue", "glitch_by_type", "glitch_by_status",
                   "glitch_by_room", "glitch_monthly", "glitch_compensation", "fb_glitches"]:
            data[k] = {"cols": [], "rows": []}

    # === CHANNELS (Duetto segment-level) ===
    print(f"  Channels...")
    data["channel_performance"] = safe_query(cur, """
        SELECT MARKET_SEGMENT AS SEGMENT,
               SUM(TODAY_ROOMS) AS ROOMS,
               CASE WHEN SUM(TODAY_ROOMS) > 0 THEN SUM(TODAY_ROOM_REVENUE)/SUM(TODAY_ROOMS) ELSE 0 END AS ADR,
               SUM(TODAY_ROOM_REVENUE) AS REVENUE,
               SUM(BUDGET_ROOMS) AS BUD_ROOMS,
               SUM(BUDGET_ROOM_REVENUE) AS BUD_REVENUE,
               SUM(STLY_ROOMS) AS STLY_ROOMS,
               SUM(STLY_ROOM_REVENUE) AS STLY_REVENUE,
               CASE WHEN SUM(STLY_ROOM_REVENUE) > 0
                    THEN (SUM(TODAY_ROOM_REVENUE)-SUM(STLY_ROOM_REVENUE))/SUM(STLY_ROOM_REVENUE)*100
                    ELSE 0 END AS VS_STLY_PCT
        FROM DUETTO_UPLOAD.RAW.DUETTO_BUDGET_VS_PY
        WHERE HOTEL_CODE = %s
          AND MARKET_SEGMENT != 'Total'
          AND TRY_TO_DATE(STAY_DATE, 'YYYYMMDD') >= DATE_TRUNC('YEAR', CURRENT_DATE())
          AND TRY_TO_DATE(STAY_DATE, 'YYYYMMDD') <= CURRENT_DATE()
        GROUP BY MARKET_SEGMENT
        ORDER BY REVENUE DESC
    """, [dc], "channel_performance")

    data["channel_monthly"] = safe_query(cur, """
        SELECT LEFT(STAY_DATE, 6) AS MONTH_RAW,
               MARKET_SEGMENT AS CHANNEL,
               SUM(TODAY_ROOM_REVENUE) AS REVENUE
        FROM DUETTO_UPLOAD.RAW.DUETTO_BUDGET_VS_PY
        WHERE HOTEL_CODE = %s
          AND MARKET_SEGMENT != 'Total'
          AND TRY_TO_DATE(STAY_DATE, 'YYYYMMDD') >= DATEADD(MONTH, -12, CURRENT_DATE())
        GROUP BY LEFT(STAY_DATE, 6), MARKET_SEGMENT
        ORDER BY MONTH_RAW, CHANNEL
    """, [dc], "channel_monthly")

    data["lra_monthly"] = safe_query(cur, """
        SELECT LEFT(STAY_DATE, 6) AS MONTH_RAW,
               MARKET_SEGMENT AS SEGMENT,
               SUM(TODAY_ROOMS) AS ROOMS,
               CASE WHEN SUM(TODAY_ROOMS) > 0 THEN SUM(TODAY_ROOM_REVENUE)/SUM(TODAY_ROOMS) ELSE 0 END AS ADR,
               SUM(TODAY_ROOM_REVENUE) AS REVENUE,
               SUM(STLY_ROOMS) AS STLY_ROOMS,
               SUM(STLY_ROOM_REVENUE) AS STLY_REVENUE
        FROM DUETTO_UPLOAD.RAW.DUETTO_BUDGET_VS_PY
        WHERE HOTEL_CODE = %s
          AND MARKET_SEGMENT IN ('LRA', 'NLRA')
          AND TRY_TO_DATE(STAY_DATE, 'YYYYMMDD') >= DATEADD(MONTH, -12, CURRENT_DATE())
        GROUP BY LEFT(STAY_DATE, 6), MARKET_SEGMENT
        ORDER BY MONTH_RAW, SEGMENT
    """, [dc], "lra_monthly")

    # === PACE & PICKUP ===
    print(f"  Pace & Pickup...")
    data["pace_monthly"] = safe_query(cur, """
        SELECT LEFT(STAY_DATE, 6) AS MONTH_RAW,
               SUM(TODAY_ROOMS) AS ROOMS,
               SUM(TODAY_ROOM_REVENUE) AS REVENUE,
               SUM(TODAY_ROOMS_PICKUP_7D) AS PICKUP_ROOMS_7D,
               SUM(TODAY_ROOM_REVENUE_PICKUP_7D) AS PICKUP_REV_7D,
               SUM(STLY_ROOMS) AS STLY_ROOMS,
               SUM(STLY_ROOM_REVENUE) AS STLY_REVENUE,
               SUM(STLY_ROOMS_PICKUP_7D) AS STLY_PICKUP_ROOMS_7D,
               SUM(STLY_ROOM_REVENUE_PICKUP_7D) AS STLY_PICKUP_REV_7D,
               SUM(FORECAST_ROOMS) AS FCST_ROOMS,
               SUM(FORECAST_ROOM_REVENUE) AS FCST_REVENUE,
               SUM(BUDGET_ROOMS) AS BUD_ROOMS,
               SUM(BUDGET_ROOM_REVENUE) AS BUD_REVENUE
        FROM DUETTO_UPLOAD.RAW.DUETTO_BUDGET_VS_PY
        WHERE HOTEL_CODE = %s
          AND MARKET_SEGMENT = 'Total'
          AND TRY_TO_DATE(STAY_DATE, 'YYYYMMDD') >= CURRENT_DATE()
          AND TRY_TO_DATE(STAY_DATE, 'YYYYMMDD') <= DATEADD(MONTH, 12, CURRENT_DATE())
        GROUP BY LEFT(STAY_DATE, 6)
        ORDER BY MONTH_RAW
    """, [dc], "pace_monthly")

    data["pickup_by_segment"] = safe_query(cur, """
        SELECT MARKET_SEGMENT AS SEGMENT,
               SUM(TODAY_ROOMS_PICKUP_7D) AS PICKUP_ROOMS,
               SUM(TODAY_ROOM_REVENUE_PICKUP_7D) AS PICKUP_REV,
               SUM(STLY_ROOMS_PICKUP_7D) AS STLY_PICKUP_ROOMS,
               SUM(STLY_ROOM_REVENUE_PICKUP_7D) AS STLY_PICKUP_REV
        FROM DUETTO_UPLOAD.RAW.DUETTO_BUDGET_VS_PY
        WHERE HOTEL_CODE = %s
          AND MARKET_SEGMENT != 'Total'
          AND TRY_TO_DATE(STAY_DATE, 'YYYYMMDD') >= CURRENT_DATE()
          AND TRY_TO_DATE(STAY_DATE, 'YYYYMMDD') <= DATEADD(MONTH, 12, CURRENT_DATE())
        GROUP BY MARKET_SEGMENT
        ORDER BY PICKUP_REV DESC
    """, [dc], "pickup_by_segment")

    return data


def _extract_survey_outlets(cur, survey_table, data):
    """Try to extract outlet-specific survey data from property survey table."""
    # Get column names to determine what outlets exist
    try:
        cur.execute(f"DESCRIBE TABLE CORE_REVINATE.SURVEYS.{survey_table}")
        cols = [r[0].upper() for r in cur.fetchall()]
    except Exception:
        return

    # Calabra / primary restaurant
    if "CALABRA_FANDB_QUALITY" in cols:
        data["survey_calabra"] = safe_query(cur, f"""
            SELECT COUNT(CASE WHEN CALABRA = 'Yes' THEN 1 END) AS VISITS,
                   AVG(TRY_CAST(CALABRA_FANDB_QUALITY AS FLOAT)) AS FOOD,
                   AVG(TRY_CAST(CALABRA_STAFF AS FLOAT)) AS STAFF,
                   AVG(TRY_CAST(CALABRA_SERVICE AS FLOAT)) AS SERVICE,
                   AVG(TRY_CAST(CALABRA_MENU AS FLOAT)) AS MENU
            FROM CORE_REVINATE.SURVEYS.{survey_table}
        """, label="survey_calabra")

    # Surya Spa
    if "SURYA_SPA_RESERVATIONS" in cols:
        data["survey_surya"] = safe_query(cur, f"""
            SELECT COUNT(CASE WHEN SURYA_SPA = 'Yes' THEN 1 END) AS VISITS,
                   AVG(TRY_CAST(SURYA_SPA_RESERVATIONS AS FLOAT)) AS RESERVATIONS,
                   AVG(TRY_CAST(SURYA_SPA_THERAPY_ROOMS AS FLOAT)) AS THERAPY_ROOMS,
                   AVG(TRY_CAST(SURYA_SPA_THERAPIST AS FLOAT)) AS THERAPIST,
                   AVG(TRY_CAST(SURYA_SPA_STAFF AS FLOAT)) AS STAFF
            FROM CORE_REVINATE.SURVEYS.{survey_table}
        """, label="survey_surya")

    # Palma
    if "PALMA_FANDB_QUALITY" in cols:
        data["survey_palma"] = safe_query(cur, f"""
            SELECT COUNT(CASE WHEN PALMA = 'Yes' THEN 1 END) AS VISITS,
                   AVG(TRY_CAST(PALMA_FANDB_QUALITY AS FLOAT)) AS FOOD,
                   AVG(TRY_CAST(PALMA_STAFF AS FLOAT)) AS STAFF,
                   AVG(TRY_CAST(PALMA_SERVICE AS FLOAT)) AS SERVICE,
                   AVG(TRY_CAST(PALMA_MENU AS FLOAT)) AS MENU
            FROM CORE_REVINATE.SURVEYS.{survey_table}
        """, label="survey_palma")

    # Pool
    if "POOL_MAINTENANCE" in cols:
        data["survey_pool"] = safe_query(cur, f"""
            SELECT COUNT(CASE WHEN POOL = 'Yes' THEN 1 END) AS VISITS,
                   AVG(TRY_CAST(POOL_MAINTENANCE AS FLOAT)) AS MAINTENANCE,
                   AVG(TRY_CAST(POOL_SERVICE AS FLOAT)) AS SERVICE,
                   AVG(TRY_CAST(POOL_FANDB AS FLOAT)) AS FANDB
            FROM CORE_REVINATE.SURVEYS.{survey_table}
        """, label="survey_pool")


def main():
    print(f"Connecting to Snowflake...")
    conn = connect()
    cur = conn.cursor()

    output_dir = Path(__file__).parent.parent / "data"
    output_dir.mkdir(exist_ok=True)

    target = sys.argv[1] if len(sys.argv) > 1 else None

    for pid, prop in PROPERTIES.items():
        if target and pid != target:
            continue
        print(f"\n{'='*60}")
        print(f"  {prop['name']} ({pid})")
        print(f"{'='*60}")

        data = export_property(cur, pid, prop)

        out_path = output_dir / f"{pid}.json"
        with open(out_path, "w") as f:
            json.dump(data, f)
        size_kb = out_path.stat().st_size / 1024
        row_count = sum(len(v.get("rows", [])) for v in data.values())
        print(f"  -> {out_path.name}: {size_kb:.0f} KB, {row_count} total rows, {len(data)} keys")

    conn.close()
    print(f"\nDone at {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
