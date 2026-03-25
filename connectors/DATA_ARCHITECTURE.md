# Proper Intelligence Hub — Data Architecture & Recommendations

## Current State

### Data Sources in Production
| Source | System | What It Feeds | Refresh Cadence |
|--------|--------|--------------|-----------------|
| **Duetto** | Revenue Management | Revenue, ADR, Pace, Pickup, Channels, Budget, Forecast, STLY | Daily (overnight) |
| **Revinate** | Reputation + Surveys | Reviews, Ratings, Guest Surveys, Comp Set, F&B Outlet Ratings | Daily |
| **Tripleseat** | Group/Event Sales | Bookings, Pipeline, Event Revenue, Segments, Sales Rep Performance | Real-time via API |
| **Internal Ops** | Glitch/Incident System | Glitch Reports, Compensation, Issue Tracking | Real-time |

### Current Pipeline
```
Duetto API ──────┐
Revinate API ────┤── Snowflake (PROPER_DW) ── Static JSON export ── GitHub Pages
Tripleseat API ──┤
Ops System ──────┘
```

### Known Data Issues
1. **Austin F&B outlet survey data is empty** — Peacock, La Piscina, Goldies have `null` food ratings in `fb_outlet_ratings`. Likely the Revinate outlet survey mapping doesn't match Austin's outlet names. **Fix:** Verify Revinate outlet name configuration for Austin.
2. **SMP key naming leaked into Austin** — `smp_comp_set` and `smp_reviews_aggregate` were SMP-specific key names used for both properties. **Fixed** in this hub (renamed to `comp_set` and `property_reviews_aggregate`).
3. **Static JSON = stale data** — Current setup requires manual export to update. No automated refresh pipeline.
4. **No occupancy data exposed** — Revenue data has rooms sold but not room inventory, so occupancy % can't be calculated.
5. **No RevPAR** — Related to above; need total available rooms per property to compute RevPAR.

---

## Recommended Additional Data Sources

### Tier 1 — High Impact, Likely Available Now

#### 1. STR (Smith Travel Research) / CoStar
- **What:** Comp set market performance — occupancy, ADR, RevPAR for your competitive set
- **Why:** Currently comp set tab only has review ratings from Revinate. STR data would add the revenue/rate competitive picture GMs actually care about most.
- **New Dashboard Tab:** Enhance Comp Set with STR index (occupancy index, ADR index, RevPAR index)
- **Connector:** STR provides data via SFTP or API. Load to Snowflake nightly.

#### 2. PMS (Opera/Mews/etc.)
- **What:** Guest profiles, loyalty tiers, room type mix, length of stay, origin markets, repeat guest rate
- **Why:** Connects the "who" to the "what." Revenue data tells you how much; PMS tells you who's buying.
- **New Metrics:** Repeat guest %, avg length of stay, room type ADR, top feeder markets, loyalty tier mix
- **New Dashboard Tab:** "Guest Profile" — feeder market map, repeat vs new, loyalty mix, room type performance

#### 3. Labor / Scheduling (HotSchedules, ADP, etc.)
- **What:** Labor hours, labor cost, covers per labor hour, CPOR (cost per occupied room)
- **Why:** Revenue without cost context is incomplete. GMs need to see labor efficiency alongside revenue.
- **New Metrics:** CPOR, RevPAR vs CPOR, F&B labor cost %, labor hours per cover
- **New Dashboard Tab:** "Labor & Efficiency" — CPOR trend, department-level labor costs, covers per labor hour

#### 4. POS / F&B Revenue (Infrasys, Micros, Toast)
- **What:** Actual F&B revenue by outlet, covers, avg check, daypart mix, menu item performance
- **Why:** Current F&B tab only has survey ratings and Tripleseat events. No actual F&B revenue data.
- **New Metrics:** Revenue per cover, avg check by outlet, daypart revenue split, top/bottom menu items
- **Enhancement:** F&B Deep Dive becomes truly deep — revenue + satisfaction + operational data

### Tier 2 — High Impact, Moderate Effort

#### 5. OTA Extranet Data (Expedia, Booking.com)
- **What:** OTA conversion rates, page views, search impressions, rate parity violations
- **Why:** Channels tab shows OTA revenue but not the funnel. Low conversion = pricing or content problem.
- **New Metrics:** OTA conversion rate, search-to-book ratio, rate parity score
- **Enhancement:** Add to Channels tab

#### 6. Website / Direct Booking Analytics
- **What:** Website traffic, booking engine conversion, abandoned carts, top landing pages
- **Why:** BAR (direct) channel shows revenue but not demand generation. GMs should see web→booking funnel.
- **Source:** Google Analytics 4 via BigQuery or GA API → Snowflake
- **New Metrics:** Web-to-booking conversion, abandoned cart rate, top traffic sources

#### 7. Social Media Sentiment (Sprout Social, Brandwatch)
- **What:** Social mention volume, sentiment score, trending topics, response time
- **Why:** Reviews are lagging indicators. Social is a real-time pulse on guest sentiment.
- **New Metrics:** Daily mention volume, sentiment trend, response time SLA
- **New Dashboard Tab:** "Social & Digital" — mention volume, sentiment, top topics, response metrics

### Tier 3 — Nice to Have

#### 8. Weather & Local Events
- **What:** Weather forecasts, local event calendar, flight search data
- **Why:** Contextualize demand patterns. A bad pickup week might just be weather, not a pricing problem.
- **Source:** OpenWeather API, local CVB event feeds, Google Flights trends
- **Enhancement:** Overlay on Pace & Pickup charts

#### 9. Maintenance / Engineering (HotSOS, Quore)
- **What:** Work orders, preventive maintenance completion, room condition
- **Why:** Glitch reports show guest-facing incidents. Maintenance data shows root causes.
- **Enhancement:** Add to Glitch Reports — "Top rooms by work orders" alongside "top rooms by glitches"

#### 10. Financial (Accounting / ProfitSword / M3)
- **What:** GOP, flow-through, departmental P&L
- **Why:** Full picture from top line to bottom line. Revenue growth means nothing if margins are eroding.
- **New Dashboard Tab:** "Financial Performance" — GOP trend, flow-through %, departmental margins

---

## Recommended Architecture — Next Phase

```
                    ┌─────────────────────────────────────────┐
                    │           SNOWFLAKE (PROPER_DW)          │
                    │                                           │
  Duetto ──────────►│  RAW layer    →   STAGING    →   MARTS   │
  Revinate ────────►│  (raw JSON)      (cleaned)     (dashboard │
  Tripleseat ──────►│                                  views)   │
  Ops System ──────►│                                           │
  STR ─────────────►│  New sources:                             │
  PMS ─────────────►│  PMS, POS, Labor, STR                     │
  POS ─────────────►│                                           │
  Labor ───────────►│                                           │
                    └──────────────┬────────────────────────────┘
                                   │
                         Scheduled Python job
                         (refresh_data.py)
                         runs daily at 6 AM
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │   GitHub Pages (static JSON)  │
                    │   proper-intelligence-hub/     │
                    │     data/smp.json              │
                    │     data/austin.json           │
                    │     index.html + dashboard.html│
                    └──────────────────────────────┘
```

### Automation Options (in order of simplicity)

1. **GitHub Actions cron job** — Run `refresh_data.py` on a schedule, commit updated JSON, auto-deploy to Pages. Zero infrastructure. ✅ Recommended for now.

2. **Snowflake Tasks + Stages** — Snowflake natively exports to S3/GCS, then sync to GitHub. More complex but keeps everything in Snowflake.

3. **Proper API backend** — Replace static JSON with a lightweight API (FastAPI/Cloud Functions) that queries Snowflake on demand. Enables real-time filtering. Best long-term option.

---

## Priority Roadmap for GM Single Source of Truth

### Phase 1 (Now) ✅
- Unified multi-property portal with 9 tabs each
- Portfolio-level cross-property views
- Static JSON from Snowflake
- All existing data: Duetto, Revinate, Tripleseat

### Phase 2 (Next 2-4 weeks)
- Automated daily refresh via GitHub Actions
- Add STR comp set data (ADR/occupancy/RevPAR index)
- Add PMS guest profile data (repeat rate, feeder markets)
- Add occupancy % and RevPAR to Revenue tab
- Fix Austin outlet survey mapping in Revinate

### Phase 3 (Next 1-2 months)
- Add POS/F&B revenue data
- Add labor cost metrics (CPOR, labor %)
- Add OTA conversion funnel
- Add "GM Morning Brief" — auto-generated summary card on landing page

### Phase 4 (Quarterly)
- Real-time API backend (replace static JSON)
- Social media monitoring integration
- Financial P&L integration
- Mobile-optimized responsive design
- Automated alerts (email/Slack when KPI thresholds breached)
