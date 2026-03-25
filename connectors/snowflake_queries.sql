-- ============================================================================
-- PROPER HOSPITALITY INTELLIGENCE HUB — SNOWFLAKE QUERIES
-- ============================================================================
-- These queries produce the 40 data keys consumed by the dashboard.
-- Each query is parameterized with :property_code (e.g., 'SMP', 'AUS').
-- Source systems: Duetto (revenue/pace), Revinate (reviews/surveys), Tripleseat (group sales)
--
-- IMPORTANT: Update schema/table names to match your actual Snowflake structure.
-- The naming convention below assumes:
--   PROPER_DW.DUETTO.*       — Revenue management data from Duetto
--   PROPER_DW.REVINATE.*     — Reviews and guest surveys from Revinate
--   PROPER_DW.TRIPLESEAT.*   — Group/event sales from Tripleseat
--   PROPER_DW.OPS.*          — Operational data (glitch reports, etc.)
--   PROPER_DW.CHANNEL.*      — Channel/distribution data from Duetto
-- ============================================================================

-- ============================================================================
-- 1. REVENUE TAB
-- ============================================================================

-- revenue_monthly: Daily revenue with actual, budget, STLY, forecast
-- Source: Duetto RMS daily statistics
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_REVENUE_MONTHLY AS
SELECT
    TO_CHAR(d.STAY_DATE, 'YYYYMMDD')            AS STAY_DATE,
    d.ROOMS_SOLD                                  AS TODAY_ROOMS,
    CASE WHEN d.ROOMS_SOLD > 0
         THEN d.ROOM_REVENUE / d.ROOMS_SOLD ELSE 0 END AS TODAY_ADR,
    d.ROOM_REVENUE                                AS TODAY_ROOM_REVENUE,
    b.BUDGET_ROOMS                                AS BUDGET_ROOMS,
    CASE WHEN b.BUDGET_ROOMS > 0
         THEN b.BUDGET_REVENUE / b.BUDGET_ROOMS ELSE 0 END AS BUDGET_ADR,
    b.BUDGET_REVENUE                              AS BUDGET_ROOM_REVENUE,
    s.ROOMS_SOLD                                  AS STLY_ROOMS,
    CASE WHEN s.ROOMS_SOLD > 0
         THEN s.ROOM_REVENUE / s.ROOMS_SOLD ELSE 0 END AS STLY_ADR,
    s.ROOM_REVENUE                                AS STLY_ROOM_REVENUE,
    f.FORECAST_ROOMS                              AS FORECAST_ROOMS,
    CASE WHEN f.FORECAST_ROOMS > 0
         THEN f.FORECAST_REVENUE / f.FORECAST_ROOMS ELSE 0 END AS FORECAST_ADR,
    f.FORECAST_REVENUE                            AS FORECAST_ROOM_REVENUE
FROM PROPER_DW.DUETTO.DAILY_STATISTICS d
LEFT JOIN PROPER_DW.DUETTO.BUDGET b
    ON d.PROPERTY_CODE = b.PROPERTY_CODE AND d.STAY_DATE = b.STAY_DATE
LEFT JOIN PROPER_DW.DUETTO.DAILY_STATISTICS s
    ON d.PROPERTY_CODE = s.PROPERTY_CODE AND d.STAY_DATE = DATEADD(YEAR, 1, s.STAY_DATE)
LEFT JOIN PROPER_DW.DUETTO.FORECAST f
    ON d.PROPERTY_CODE = f.PROPERTY_CODE AND d.STAY_DATE = f.STAY_DATE
WHERE d.PROPERTY_CODE = :property_code
  AND d.STAY_DATE >= DATEADD(YEAR, -1, CURRENT_DATE())
  AND d.STAY_DATE <= CURRENT_DATE()
ORDER BY d.STAY_DATE;

-- revenue_by_segment: Revenue aggregated by market segment
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_REVENUE_BY_SEGMENT AS
SELECT
    d.MARKET_SEGMENT,
    SUM(d.ROOMS_SOLD)                             AS ROOMS,
    CASE WHEN SUM(d.ROOMS_SOLD) > 0
         THEN SUM(d.ROOM_REVENUE) / SUM(d.ROOMS_SOLD) ELSE 0 END AS ADR,
    SUM(d.ROOM_REVENUE)                           AS REVENUE,
    SUM(s.ROOMS_SOLD)                             AS STLY_ROOMS,
    SUM(s.ROOM_REVENUE)                           AS STLY_REVENUE
FROM PROPER_DW.DUETTO.DAILY_STATISTICS d
LEFT JOIN PROPER_DW.DUETTO.DAILY_STATISTICS s
    ON d.PROPERTY_CODE = s.PROPERTY_CODE
    AND d.MARKET_SEGMENT = s.MARKET_SEGMENT
    AND d.STAY_DATE = DATEADD(YEAR, 1, s.STAY_DATE)
WHERE d.PROPERTY_CODE = :property_code
  AND d.STAY_DATE >= DATE_TRUNC('YEAR', CURRENT_DATE())
  AND d.STAY_DATE <= CURRENT_DATE()
GROUP BY d.MARKET_SEGMENT
ORDER BY REVENUE DESC;

-- portfolio_comparison: All properties side by side
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_PORTFOLIO_COMPARISON AS
SELECT
    p.HOTEL_NAME,
    SUM(d.ROOMS_SOLD)                             AS ROOMS,
    CASE WHEN SUM(d.ROOMS_SOLD) > 0
         THEN SUM(d.ROOM_REVENUE) / SUM(d.ROOMS_SOLD) ELSE 0 END AS ADR,
    SUM(d.ROOM_REVENUE)                           AS REVENUE,
    SUM(b.BUDGET_REVENUE)                         AS BUDGET_REV,
    CASE WHEN SUM(b.BUDGET_REVENUE) > 0
         THEN (SUM(d.ROOM_REVENUE) - SUM(b.BUDGET_REVENUE)) / SUM(b.BUDGET_REVENUE) * 100
         ELSE 0 END                               AS VS_BUD,
    SUM(s.ROOM_REVENUE)                           AS STLY_REV,
    CASE WHEN SUM(s.ROOM_REVENUE) > 0
         THEN (SUM(d.ROOM_REVENUE) - SUM(s.ROOM_REVENUE)) / SUM(s.ROOM_REVENUE) * 100
         ELSE 0 END                               AS VS_STLY
FROM PROPER_DW.DUETTO.DAILY_STATISTICS d
JOIN PROPER_DW.CORE.PROPERTIES p ON d.PROPERTY_CODE = p.PROPERTY_CODE
LEFT JOIN PROPER_DW.DUETTO.BUDGET b
    ON d.PROPERTY_CODE = b.PROPERTY_CODE AND d.STAY_DATE = b.STAY_DATE
LEFT JOIN PROPER_DW.DUETTO.DAILY_STATISTICS s
    ON d.PROPERTY_CODE = s.PROPERTY_CODE AND d.STAY_DATE = DATEADD(YEAR, 1, s.STAY_DATE)
WHERE d.STAY_DATE >= DATE_TRUNC('YEAR', CURRENT_DATE())
  AND d.STAY_DATE <= CURRENT_DATE()
GROUP BY p.HOTEL_NAME
ORDER BY REVENUE DESC;


-- ============================================================================
-- 2. REVIEWS TAB
-- ============================================================================

-- reviews_by_source: Aggregated ratings by review platform
-- Source: Revinate review aggregation
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_REVIEWS_BY_SOURCE AS
SELECT
    r.SOURCE,
    COUNT(*)                                       AS COUNT,
    AVG(r.OVERALL_RATING)                         AS AVG_RATING,
    AVG(r.CLEANLINESS_RATING)                     AS CLEAN,
    AVG(r.SERVICE_RATING)                         AS SERVICE,
    AVG(r.ROOMS_RATING)                           AS ROOMS,
    AVG(r.VALUE_RATING)                           AS VALUE,
    AVG(r.LOCATION_RATING)                        AS LOCATION,
    SUM(CASE WHEN r.RESPONSE_TEXT IS NOT NULL THEN 1 ELSE 0 END) AS RESPONDED,
    MIN(r.OVERALL_RATING)                         AS MIN,
    MAX(r.OVERALL_RATING)                         AS MAX
FROM PROPER_DW.REVINATE.REVIEWS r
WHERE r.PROPERTY_CODE = :property_code
  AND r.REVIEW_DATE >= DATEADD(MONTH, -12, CURRENT_DATE())
GROUP BY r.SOURCE
ORDER BY COUNT DESC;

-- negative_reviews: Individual negative reviews for management attention
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_NEGATIVE_REVIEWS AS
SELECT
    r.SOURCE,
    r.REVIEW_DATE                                  AS DATE,
    r.OVERALL_RATING                               AS RATING,
    r.TITLE,
    r.REVIEW_TEXT                                   AS TEXT,
    CASE WHEN r.RESPONSE_TEXT IS NOT NULL THEN 'true' ELSE 'false' END AS RESPONDED,
    r.RESPONSE_TEXT                                 AS RESPONSE
FROM PROPER_DW.REVINATE.REVIEWS r
WHERE r.PROPERTY_CODE = :property_code
  AND r.OVERALL_RATING <= 3
  AND r.REVIEW_DATE >= DATEADD(MONTH, -6, CURRENT_DATE())
ORDER BY r.REVIEW_DATE DESC
LIMIT 50;

-- property_reviews_aggregate: Overall property review summary (for comp set)
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_PROPERTY_REVIEWS_AGGREGATE AS
SELECT
    p.HOTEL_NAME,
    COUNT(*)                                       AS REVIEW_COUNT,
    AVG(r.OVERALL_RATING)                         AS OVERALL,
    AVG(r.CLEANLINESS_RATING)                     AS CLEANLINESS,
    AVG(r.SERVICE_RATING)                         AS SERVICE,
    AVG(r.ROOMS_RATING)                           AS ROOMS,
    AVG(r.VALUE_RATING)                           AS VALUE
FROM PROPER_DW.REVINATE.REVIEWS r
JOIN PROPER_DW.CORE.PROPERTIES p ON r.PROPERTY_CODE = p.PROPERTY_CODE
WHERE r.PROPERTY_CODE = :property_code
  AND r.REVIEW_DATE >= DATEADD(MONTH, -12, CURRENT_DATE())
GROUP BY p.HOTEL_NAME;


-- ============================================================================
-- 3. GUEST SURVEYS TAB
-- ============================================================================

-- survey_overall: Aggregated survey KPIs
-- Source: Revinate Guest Surveys
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_SURVEY_OVERALL AS
SELECT
    COUNT(*)                                       AS TOTAL_SURVEYS,
    AVG(s.OVERALL_RATING)                         AS AVG_RATING,
    AVG(s.NPS_SCORE)                              AS AVG_NPS,
    AVG(s.ROOM_RATING)                            AS ROOM_RATING,
    AVG(s.BREAKFAST_RATING)                       AS BREAKFAST_RATING,
    AVG(s.ARRIVAL_RATING)                         AS ARRIVAL_RATING,
    AVG(s.DEPARTURE_RATING)                       AS DEPARTURE_RATING,
    AVG(s.SERVICE_RATING)                         AS SERVICE_RATING
FROM PROPER_DW.REVINATE.GUEST_SURVEYS s
WHERE s.PROPERTY_CODE = :property_code
  AND s.SURVEY_DATE >= DATEADD(MONTH, -12, CURRENT_DATE());

-- survey_issues: Top guest complaints
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_SURVEY_ISSUES AS
SELECT
    s.ISSUE_CATEGORY,
    COUNT(*)                                       AS MENTION_COUNT
FROM PROPER_DW.REVINATE.SURVEY_ISSUES s
WHERE s.PROPERTY_CODE = :property_code
  AND s.SURVEY_DATE >= DATEADD(MONTH, -12, CURRENT_DATE())
GROUP BY s.ISSUE_CATEGORY
ORDER BY MENTION_COUNT DESC
LIMIT 20;

-- survey_by_channel: Satisfaction broken down by booking channel
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_SURVEY_BY_CHANNEL AS
SELECT
    s.BOOKING_CHANNEL                              AS CHANNEL,
    COUNT(*)                                       AS SURVEYS,
    AVG(s.OVERALL_RATING)                         AS AVG_RATING,
    AVG(s.NPS_SCORE)                              AS AVG_NPS
FROM PROPER_DW.REVINATE.GUEST_SURVEYS s
WHERE s.PROPERTY_CODE = :property_code
  AND s.SURVEY_DATE >= DATEADD(MONTH, -12, CURRENT_DATE())
GROUP BY s.BOOKING_CHANNEL
ORDER BY SURVEYS DESC;

-- survey_<outlet>: Per-outlet survey ratings (parameterized)
-- Repeat for each outlet. Example for a restaurant:
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_SURVEY_OUTLET AS
SELECT
    s.OUTLET_NAME,
    COUNT(*)                                       AS VISITS,
    AVG(s.FOOD_QUALITY_RATING)                    AS FOOD,
    AVG(s.STAFF_RATING)                           AS STAFF,
    AVG(s.SERVICE_RATING)                         AS SERVICE,
    AVG(s.MENU_RATING)                            AS MENU
FROM PROPER_DW.REVINATE.OUTLET_SURVEYS s
WHERE s.PROPERTY_CODE = :property_code
  AND s.SURVEY_DATE >= DATEADD(MONTH, -12, CURRENT_DATE())
GROUP BY s.OUTLET_NAME;

-- outlet_comments: Recent guest comments per outlet
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_OUTLET_COMMENTS AS
SELECT
    s.COMMENT_TEXT
FROM PROPER_DW.REVINATE.OUTLET_SURVEYS s
WHERE s.PROPERTY_CODE = :property_code
  AND s.OUTLET_NAME = :outlet_name
  AND s.COMMENT_TEXT IS NOT NULL
  AND s.SURVEY_DATE >= DATEADD(MONTH, -6, CURRENT_DATE())
ORDER BY s.SURVEY_DATE DESC
LIMIT 20;


-- ============================================================================
-- 4. GROUP SALES TAB
-- ============================================================================

-- bookings_by_status: Pipeline summary
-- Source: Tripleseat
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_BOOKINGS_BY_STATUS AS
SELECT
    b.STATUS,
    COUNT(*)                                       AS BOOKINGS,
    SUM(b.TOTAL_REVENUE)                          AS TOTAL_REVENUE,
    AVG(b.TOTAL_REVENUE)                          AS AVG_REVENUE,
    SUM(CASE WHEN b.STATUS = 'Definite' THEN 1 ELSE 0 END) AS DEFINITE
FROM PROPER_DW.TRIPLESEAT.BOOKINGS b
WHERE b.PROPERTY_CODE = :property_code
  AND b.EVENT_DATE >= DATE_TRUNC('YEAR', CURRENT_DATE())
GROUP BY b.STATUS
ORDER BY TOTAL_REVENUE DESC;

-- bookings_by_owner: Revenue by sales rep
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_BOOKINGS_BY_OWNER AS
SELECT
    b.SALES_REP                                    AS OWNER,
    COUNT(*)                                       AS BOOKINGS,
    SUM(b.TOTAL_REVENUE)                          AS REVENUE,
    SUM(CASE WHEN b.STATUS = 'Definite' THEN 1 ELSE 0 END) AS DEFINITE,
    SUM(CASE WHEN b.STATUS = 'Tentative' THEN 1 ELSE 0 END) AS TENTATIVE,
    AVG(b.TOTAL_REVENUE)                          AS AVG_DEAL
FROM PROPER_DW.TRIPLESEAT.BOOKINGS b
WHERE b.PROPERTY_CODE = :property_code
  AND b.EVENT_DATE >= DATE_TRUNC('YEAR', CURRENT_DATE())
GROUP BY b.SALES_REP
ORDER BY REVENUE DESC;

-- bookings_monthly: Monthly booking trend
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_BOOKINGS_MONTHLY AS
SELECT
    TO_CHAR(b.EVENT_DATE, 'YYYY-MM')             AS MONTH,
    COUNT(*)                                       AS BOOKINGS,
    SUM(b.TOTAL_REVENUE)                          AS TOTAL_REVENUE,
    SUM(CASE WHEN b.STATUS = 'Definite' THEN 1 ELSE 0 END) AS DEFINITE_COUNT,
    SUM(CASE WHEN b.STATUS = 'Definite' THEN b.TOTAL_REVENUE ELSE 0 END) AS DEFINITE_REVENUE
FROM PROPER_DW.TRIPLESEAT.BOOKINGS b
WHERE b.PROPERTY_CODE = :property_code
  AND b.EVENT_DATE >= DATEADD(MONTH, -14, CURRENT_DATE())
GROUP BY TO_CHAR(b.EVENT_DATE, 'YYYY-MM')
ORDER BY MONTH;

-- tripleseat_segments: Bookings by event type
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_TRIPLESEAT_SEGMENTS AS
SELECT
    b.EVENT_TYPE                                   AS SEGMENT,
    COUNT(*)                                       AS BOOKINGS,
    SUM(b.TOTAL_REVENUE)                          AS REVENUE,
    AVG(b.TOTAL_REVENUE)                          AS AVG_REVENUE,
    SUM(CASE WHEN b.STATUS = 'Definite' THEN 1 ELSE 0 END) AS DEFINITE
FROM PROPER_DW.TRIPLESEAT.BOOKINGS b
WHERE b.PROPERTY_CODE = :property_code
  AND b.EVENT_DATE >= DATE_TRUNC('YEAR', CURRENT_DATE())
GROUP BY b.EVENT_TYPE
ORDER BY REVENUE DESC;


-- ============================================================================
-- 5. COMP SET TAB
-- ============================================================================

-- comp_set: Competitor review ratings
-- Source: Revinate competitive intelligence
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_COMP_SET AS
SELECT
    c.COMPETITOR_NAME,
    COUNT(*)                                       AS REVIEWS,
    AVG(c.OVERALL_RATING)                         AS OVERALL,
    AVG(c.CLEANLINESS_RATING)                     AS CLEANLINESS,
    AVG(c.SERVICE_RATING)                         AS SERVICE,
    AVG(c.ROOMS_RATING)                           AS ROOMS,
    AVG(c.VALUE_RATING)                           AS VALUE
FROM PROPER_DW.REVINATE.COMPETITOR_REVIEWS c
WHERE c.PROPERTY_CODE = :property_code
  AND c.REVIEW_DATE >= DATEADD(MONTH, -12, CURRENT_DATE())
GROUP BY c.COMPETITOR_NAME
ORDER BY OVERALL DESC;


-- ============================================================================
-- 6. GLITCH REPORTS TAB
-- ============================================================================

-- glitch_reports: Full incident log
-- Source: Internal ops system (Alice, Quore, or custom)
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_GLITCH_REPORTS AS
SELECT
    g.GLITCH_ID,
    g.INCIDENT_DATE,
    g.ROOM_NUMBER,
    g.GUEST_NAME,
    g.INCIDENT_TYPE,
    g.ISSUE_CATEGORY,
    g.DESCRIPTION,
    g.COMPENSATION,
    g.GUEST_SATISFACTION,
    g.STATUS
FROM PROPER_DW.OPS.GLITCH_REPORTS g
WHERE g.PROPERTY_CODE = :property_code
  AND g.INCIDENT_DATE >= DATEADD(MONTH, -6, CURRENT_DATE())
ORDER BY g.INCIDENT_DATE DESC;

-- glitch_by_issue / glitch_by_type / glitch_by_status / glitch_by_room / glitch_monthly
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_GLITCH_BY_ISSUE AS
SELECT g.ISSUE_CATEGORY, COUNT(*) AS CNT
FROM PROPER_DW.OPS.GLITCH_REPORTS g
WHERE g.PROPERTY_CODE = :property_code AND g.INCIDENT_DATE >= DATEADD(MONTH, -6, CURRENT_DATE())
GROUP BY g.ISSUE_CATEGORY ORDER BY CNT DESC;

CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_GLITCH_BY_TYPE AS
SELECT g.INCIDENT_TYPE, COUNT(*) AS CNT
FROM PROPER_DW.OPS.GLITCH_REPORTS g
WHERE g.PROPERTY_CODE = :property_code AND g.INCIDENT_DATE >= DATEADD(MONTH, -6, CURRENT_DATE())
GROUP BY g.INCIDENT_TYPE ORDER BY CNT DESC;

CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_GLITCH_BY_STATUS AS
SELECT g.STATUS, COUNT(*) AS CNT
FROM PROPER_DW.OPS.GLITCH_REPORTS g
WHERE g.PROPERTY_CODE = :property_code AND g.INCIDENT_DATE >= DATEADD(MONTH, -6, CURRENT_DATE())
GROUP BY g.STATUS;

CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_GLITCH_BY_ROOM AS
SELECT g.ROOM_NUMBER, COUNT(*) AS INCIDENTS, COUNT(DISTINCT g.ISSUE_CATEGORY) AS UNIQUE_ISSUES
FROM PROPER_DW.OPS.GLITCH_REPORTS g
WHERE g.PROPERTY_CODE = :property_code AND g.INCIDENT_DATE >= DATEADD(MONTH, -6, CURRENT_DATE())
GROUP BY g.ROOM_NUMBER HAVING COUNT(*) >= 2
ORDER BY INCIDENTS DESC;

CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_GLITCH_MONTHLY AS
SELECT TO_CHAR(g.INCIDENT_DATE, 'YYYY-MM') AS MONTH, COUNT(*) AS CNT
FROM PROPER_DW.OPS.GLITCH_REPORTS g
WHERE g.PROPERTY_CODE = :property_code AND g.INCIDENT_DATE >= DATEADD(MONTH, -6, CURRENT_DATE())
GROUP BY TO_CHAR(g.INCIDENT_DATE, 'YYYY-MM') ORDER BY MONTH;

CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_GLITCH_COMPENSATION AS
SELECT g.COMPENSATION, COUNT(*) AS CNT
FROM PROPER_DW.OPS.GLITCH_REPORTS g
WHERE g.PROPERTY_CODE = :property_code AND g.COMPENSATION IS NOT NULL
  AND g.INCIDENT_DATE >= DATEADD(MONTH, -6, CURRENT_DATE())
GROUP BY g.COMPENSATION ORDER BY CNT DESC;


-- ============================================================================
-- 7. F&B DEEP DIVE TAB
-- ============================================================================

-- fb_outlet_ratings: Survey ratings per F&B outlet
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_FB_OUTLET_RATINGS AS
SELECT
    s.OUTLET_NAME                                  AS OUTLET,
    COUNT(*)                                       AS VISITS,
    AVG(s.FOOD_QUALITY_RATING)                    AS FOOD,
    AVG(s.STAFF_RATING)                           AS STAFF,
    AVG(s.SERVICE_RATING)                         AS SERVICE,
    AVG(s.MENU_RATING)                            AS MENU
FROM PROPER_DW.REVINATE.OUTLET_SURVEYS s
WHERE s.PROPERTY_CODE = :property_code
  AND s.SURVEY_DATE >= DATEADD(MONTH, -12, CURRENT_DATE())
GROUP BY s.OUTLET_NAME;

-- fb_glitches: F&B-specific incidents
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_FB_GLITCHES AS
SELECT g.INCIDENT_DATE, g.ROOM_NUMBER, g.ISSUE_CATEGORY, g.DESCRIPTION, g.COMPENSATION, g.STATUS
FROM PROPER_DW.OPS.GLITCH_REPORTS g
WHERE g.PROPERTY_CODE = :property_code
  AND g.INCIDENT_TYPE = 'F&B'
  AND g.INCIDENT_DATE >= DATEADD(MONTH, -6, CURRENT_DATE())
ORDER BY g.INCIDENT_DATE DESC;

-- fb_reviews: Guest reviews mentioning F&B keywords
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_FB_REVIEWS AS
SELECT r.SOURCE, r.REVIEW_DATE, r.OVERALL_RATING, r.REVIEW_TEXT
FROM PROPER_DW.REVINATE.REVIEWS r
WHERE r.PROPERTY_CODE = :property_code
  AND (LOWER(r.REVIEW_TEXT) LIKE '%restaurant%'
    OR LOWER(r.REVIEW_TEXT) LIKE '%food%'
    OR LOWER(r.REVIEW_TEXT) LIKE '%breakfast%'
    OR LOWER(r.REVIEW_TEXT) LIKE '%dinner%'
    OR LOWER(r.REVIEW_TEXT) LIKE '%bar%'
    OR LOWER(r.REVIEW_TEXT) LIKE '%cocktail%'
    OR LOWER(r.REVIEW_TEXT) LIKE '%room service%')
  AND r.REVIEW_DATE >= DATEADD(MONTH, -6, CURRENT_DATE())
ORDER BY r.REVIEW_DATE DESC
LIMIT 30;

-- fb_event_revenue: Banquet/event revenue from Tripleseat
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_FB_EVENT_REVENUE AS
SELECT
    TO_CHAR(b.EVENT_DATE, 'YYYY-MM')             AS MONTH,
    COUNT(*)                                       AS EVENTS,
    SUM(b.FB_REVENUE)                             AS FB_REVENUE
FROM PROPER_DW.TRIPLESEAT.BOOKINGS b
WHERE b.PROPERTY_CODE = :property_code
  AND b.FB_REVENUE > 0
  AND b.EVENT_DATE >= DATEADD(MONTH, -14, CURRENT_DATE())
GROUP BY TO_CHAR(b.EVENT_DATE, 'YYYY-MM')
ORDER BY MONTH;


-- ============================================================================
-- 8. CHANNELS TAB
-- ============================================================================

-- channel_performance: Revenue by distribution channel
-- Source: Duetto segment/channel data
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_CHANNEL_PERFORMANCE AS
SELECT
    d.RATE_CODE_SEGMENT                            AS SEGMENT,
    SUM(d.ROOMS_SOLD)                             AS ROOMS,
    CASE WHEN SUM(d.ROOMS_SOLD) > 0
         THEN SUM(d.ROOM_REVENUE) / SUM(d.ROOMS_SOLD) ELSE 0 END AS ADR,
    SUM(d.ROOM_REVENUE)                           AS REVENUE,
    SUM(b.BUDGET_ROOMS)                           AS BUD_ROOMS,
    SUM(b.BUDGET_REVENUE)                         AS BUD_REVENUE,
    SUM(s.ROOMS_SOLD)                             AS STLY_ROOMS,
    SUM(s.ROOM_REVENUE)                           AS STLY_REVENUE,
    CASE WHEN SUM(s.ROOM_REVENUE) > 0
         THEN (SUM(d.ROOM_REVENUE) - SUM(s.ROOM_REVENUE)) / SUM(s.ROOM_REVENUE) * 100
         ELSE 0 END                               AS VS_STLY_PCT
FROM PROPER_DW.DUETTO.CHANNEL_STATISTICS d
LEFT JOIN PROPER_DW.DUETTO.CHANNEL_BUDGET b
    ON d.PROPERTY_CODE = b.PROPERTY_CODE AND d.STAY_DATE = b.STAY_DATE AND d.RATE_CODE_SEGMENT = b.RATE_CODE_SEGMENT
LEFT JOIN PROPER_DW.DUETTO.CHANNEL_STATISTICS s
    ON d.PROPERTY_CODE = s.PROPERTY_CODE AND d.STAY_DATE = DATEADD(YEAR, 1, s.STAY_DATE) AND d.RATE_CODE_SEGMENT = s.RATE_CODE_SEGMENT
WHERE d.PROPERTY_CODE = :property_code
  AND d.STAY_DATE >= DATE_TRUNC('YEAR', CURRENT_DATE())
  AND d.STAY_DATE <= CURRENT_DATE()
GROUP BY d.RATE_CODE_SEGMENT
ORDER BY REVENUE DESC;

-- channel_monthly: Monthly revenue by channel for stacked chart
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_CHANNEL_MONTHLY AS
SELECT
    TO_CHAR(d.STAY_DATE, 'YYYY-MM')              AS MONTH,
    d.CHANNEL_DISPLAY_NAME                        AS CHANNEL,
    SUM(d.ROOM_REVENUE)                           AS REVENUE
FROM PROPER_DW.DUETTO.CHANNEL_STATISTICS d
WHERE d.PROPERTY_CODE = :property_code
  AND d.STAY_DATE >= DATEADD(MONTH, -12, CURRENT_DATE())
GROUP BY TO_CHAR(d.STAY_DATE, 'YYYY-MM'), d.CHANNEL_DISPLAY_NAME
ORDER BY MONTH, CHANNEL;

-- lra_monthly: LRA vs NLRA (Design Hotels/Bonvoy) breakdown
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_LRA_MONTHLY AS
SELECT
    TO_CHAR(d.STAY_DATE, 'YYYY-MM')              AS MONTH,
    d.RATE_CODE_SEGMENT                           AS SEGMENT,
    SUM(d.ROOMS_SOLD)                             AS ROOMS,
    CASE WHEN SUM(d.ROOMS_SOLD) > 0
         THEN SUM(d.ROOM_REVENUE) / SUM(d.ROOMS_SOLD) ELSE 0 END AS ADR,
    SUM(d.ROOM_REVENUE)                           AS REVENUE,
    SUM(s.ROOMS_SOLD)                             AS STLY_ROOMS,
    SUM(s.ROOM_REVENUE)                           AS STLY_REVENUE
FROM PROPER_DW.DUETTO.CHANNEL_STATISTICS d
LEFT JOIN PROPER_DW.DUETTO.CHANNEL_STATISTICS s
    ON d.PROPERTY_CODE = s.PROPERTY_CODE AND d.STAY_DATE = DATEADD(YEAR, 1, s.STAY_DATE) AND d.RATE_CODE_SEGMENT = s.RATE_CODE_SEGMENT
WHERE d.PROPERTY_CODE = :property_code
  AND d.RATE_CODE_SEGMENT IN ('LRA', 'NLRA')
  AND d.STAY_DATE >= DATEADD(MONTH, -12, CURRENT_DATE())
GROUP BY TO_CHAR(d.STAY_DATE, 'YYYY-MM'), d.RATE_CODE_SEGMENT
ORDER BY MONTH, SEGMENT;


-- ============================================================================
-- 9. PACE & PICKUP TAB
-- ============================================================================

-- pace_monthly: On-the-books pace with 7-day pickup
-- Source: Duetto pace/pickup snapshots
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_PACE_MONTHLY AS
SELECT
    TO_CHAR(p.STAY_MONTH, 'YYYY-MM')             AS MONTH,
    p.OTB_ROOMS                                    AS ROOMS,
    p.OTB_REVENUE                                  AS REVENUE,
    p.PICKUP_ROOMS_7D,
    p.PICKUP_REV_7D,
    s.OTB_ROOMS                                    AS STLY_ROOMS,
    s.OTB_REVENUE                                  AS STLY_REVENUE,
    s.PICKUP_ROOMS_7D                              AS STLY_PICKUP_ROOMS_7D,
    s.PICKUP_REV_7D                                AS STLY_PICKUP_REV_7D,
    f.FORECAST_ROOMS                               AS FCST_ROOMS,
    f.FORECAST_REVENUE                             AS FCST_REVENUE,
    b.BUDGET_ROOMS                                 AS BUD_ROOMS,
    b.BUDGET_REVENUE                               AS BUD_REVENUE
FROM PROPER_DW.DUETTO.PACE_SNAPSHOT p
LEFT JOIN PROPER_DW.DUETTO.PACE_SNAPSHOT s
    ON p.PROPERTY_CODE = s.PROPERTY_CODE AND p.STAY_MONTH = DATEADD(YEAR, 1, s.STAY_MONTH) AND s.SNAPSHOT_DATE = (SELECT MAX(SNAPSHOT_DATE) FROM PROPER_DW.DUETTO.PACE_SNAPSHOT WHERE PROPERTY_CODE = p.PROPERTY_CODE)
LEFT JOIN PROPER_DW.DUETTO.MONTHLY_FORECAST f
    ON p.PROPERTY_CODE = f.PROPERTY_CODE AND p.STAY_MONTH = f.STAY_MONTH
LEFT JOIN PROPER_DW.DUETTO.MONTHLY_BUDGET b
    ON p.PROPERTY_CODE = b.PROPERTY_CODE AND p.STAY_MONTH = b.STAY_MONTH
WHERE p.PROPERTY_CODE = :property_code
  AND p.SNAPSHOT_DATE = (SELECT MAX(SNAPSHOT_DATE) FROM PROPER_DW.DUETTO.PACE_SNAPSHOT WHERE PROPERTY_CODE = :property_code)
  AND p.STAY_MONTH >= DATE_TRUNC('MONTH', CURRENT_DATE())
ORDER BY p.STAY_MONTH;

-- pickup_by_segment: 7-day pickup broken down by market segment
CREATE OR REPLACE VIEW PROPER_DW.DASHBOARD.V_PICKUP_BY_SEGMENT AS
SELECT
    p.MARKET_SEGMENT                               AS SEGMENT,
    SUM(p.PICKUP_ROOMS_7D)                        AS PICKUP_ROOMS,
    SUM(p.PICKUP_REV_7D)                          AS PICKUP_REV,
    SUM(s.PICKUP_ROOMS_7D)                        AS STLY_PICKUP_ROOMS,
    SUM(s.PICKUP_REV_7D)                          AS STLY_PICKUP_REV
FROM PROPER_DW.DUETTO.SEGMENT_PACE p
LEFT JOIN PROPER_DW.DUETTO.SEGMENT_PACE s
    ON p.PROPERTY_CODE = s.PROPERTY_CODE AND p.MARKET_SEGMENT = s.MARKET_SEGMENT AND p.STAY_MONTH = DATEADD(YEAR, 1, s.STAY_MONTH)
WHERE p.PROPERTY_CODE = :property_code
  AND p.SNAPSHOT_DATE = (SELECT MAX(SNAPSHOT_DATE) FROM PROPER_DW.DUETTO.SEGMENT_PACE WHERE PROPERTY_CODE = :property_code)
GROUP BY p.MARKET_SEGMENT
ORDER BY PICKUP_REV DESC;
