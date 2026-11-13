# Project context (handoff from a claude.ai session, 8 Nov 2026)

Owner: Sairaj, master's student (Data Science and AI for Digital Business Management, Germany).
Course: BI / Big Data semester project, option 5 "Public Transportation Performance Analytics".
Working style he wants: honest feasibility and objections, plain-language explanations, small verified steps.
He asked to be told when something is not doable. Do not claim anything works on real data unless it was run on real data.

## Goal
Regional-rail performance and passenger-demand analytics for Baden-Wuerttemberg (BW):
static GTFS timetable + GTFS-Realtime delay snapshots + NVBW passenger counts.
Warehouse in PostgreSQL (schema `dw`), ETL in Python, dashboards in Metabase, one ML use case.
Deliverables = 12 course tasks (charter, business case/KPIs, source inventory + EDA + DQ report, schema/DDL,
milestone presentation, ETL, dashboards, model, integration, tests, docs, final presentation).

## Folders on his Windows PC (C:\Users\sairaj\BI_BD\)
- `transit_realtime_collector\`  his collector script, `extracted\` (static GTFS .txt files: agency, calendar,
  calendar_dates, feed_info, routes, stop_times, stops, trips), `data\realtime\raw_pb\YYYY\MM\DD\<id>.pb`,
  `data\realtime\parquet\trip_updates|stop_updates\date=YYYY-MM-DD\<snapshot_id>.parquet`, `logs\collector_manifest.jsonl`.
  Helper scripts: `02_profile_snapshots.py`, `03_check_trip_match.py` (read-only).
- `transit_project_data\` earlier downloads incl. passenger file `raw\passenger_counts_2025_h1.csv.gz`.
- Repository actually lives at `transit_realtime_collector\transit-project\` (ETL, SQL, tests, docs);
  config.REALTIME_DIR defaults to `..\data\realtime\parquet\stop_updates`.
Python env is conda `(ds)`. Windows PowerShell.

## Data facts (measured)
- Realtime feed: https://realtime.gtfs.de/realtime-free.pb, Germany-wide, all modes, refreshed about every 10 s, no history, CC BY-SA 4.0.
  Collector saves one snapshot every 5 min (about 50k trips, 323k stop rows, raw .pb 14 MB, stop parquet 3.7 MB, trip parquet 0.6 MB).
- Stop columns: snapshot_id, collected_at_utc, feed_timestamp_utc, entity_id, trip_id, route_id, start_date, start_time,
  trip_schedule_relationship, stop_id, stop_sequence, stop_schedule_relationship(_code), arrival_delay_seconds, arrival_time_epoch,
  arrival_uncertainty_seconds, departure_delay_seconds, departure_time_epoch, departure_uncertainty_seconds.
- Parser verified on a synthetic message (12 checks). Not verified against anything but his real output.
- Realtime trip_ids match static trips.txt ids: 7,200 of 50,595 realtime trips (14%) are in the static regional-rail timetable;
  keeping only those leaves 38,272 of 323,545 stop rows (11.8%). Stop-id match among those trips is NOT yet checked.
- Static timetable: 100,912 trips, 15,742 stops, may be Germany-wide regional rail (BW cut still needed). Newest feed ends 30 Nov 2026; refresh weekly. Trip ids may change between feeds.
- Delays are operator forecasts. Median 0.5 min, mean 1.3 min, 95th pct 5.7 min. About 20 events near -24 h belong to 2 trips (date glitch);
  a few more beyond +-6 h. Rule: set |delay| > 6 h to NULL and count (threshold configurable).
- Trip-level relationship is always SCHEDULED. About 2 % of trips have every stop SKIPPED: treat as cancelled (inference, document it);
  count changes between snapshots (1,129 -> 915).
- The feed lists the whole current service day (72 % of events are in the past, none before 00:00 Berlin).
  Past events: 97.7 % unchanged after 5 min, about 8.5 % disappear from the feed. So keep 5-min snapshots and store the last
  forecast per stop event (`delay_final_forecast`).
- Passenger file: semicolon, cp1252, comma decimals, 14,653,961 rows, 15 Dec 2024 - 14 Jun 2025, sub-networks ddb and kvv,
  DHID is not the GTFS stop_id (match by name, store method and confidence). Planned seats missing on about 46 % of rows: never fill with 0.
- Delay data (Nov 2026 on) and passenger data do not overlap in time: link only via station group, weekday, hour (impact score).

## Not doable (do not promise)
Measured actual delays; train-level join of passengers and delays; statewide passenger totals; delay trends over months before enough history.
Delay prediction is a stretch goal; boardings forecasting is the primary ML task.

## Design (already built in transit_project, tested on invented data only, 20 pytest tests on PostgreSQL 16)
Star schema `dw`: dim_station (station groups via parent), routes, trips, fact_scheduled_stop, fact_delay_observation (change-only rows),
delay_final_forecast, fact_passenger_count, bridge_station_map, bridge_service_date, kpi_param (on time < 300 s), KPI views in sql/04_kpi_views.sql.
Stop delay = COALESCE(arrival_delay, departure_delay) (generated column `stop_delay_s`).
BW membership: official GeoJSON via BW_BOUNDARY_GEOJSON, else bbox (warning). Official boundary still to obtain.
Not verified on real data: loaders, occupancy text format, station-match rates, docker-compose.yml, Metabase.

## Progress
- 8 Nov 2026: tasks 1+2 done. `load_realtime.normalise_snapshot()` maps the collector layout (text relationships ->
  GTFS-RT codes, unknown values raise); `null_implausible_delays()` (config.MAX_ABS_DELAY_SECONDS, default 21600) runs on
  matched rows, count logged as `delay_nulled`. Synthetic test data now uses the exact collector layout. 23/23 pytest pass
  (PostgreSQL 16 in Docker, `transit_pg_test` on port 55432, db transit_test). On the 7 real files: normalise ran OK,
  21-55 delay values per snapshot beyond 6 h (Germany-wide, before trip filter). Full DB load on real data NOT yet run
  (needs static timetable loaded first).
- 9 Nov 2026: collector found stopped (log: "stopped by user" 8 Nov 17:55 Berlin); only 7 snapshots exist.
  Test container stopped (it was `--rm`, so it is gone; restart with
  `docker run --rm -d --name transit_pg_test -p 55432:5432 -e POSTGRES_USER=transit -e POSTGRES_PASSWORD=transit -e POSTGRES_DB=transit_test postgres:16`).
  Open offer: Windows scheduled task to keep the collector running (check PC sleep settings).
- 9 Nov 2026: collector runs on Sairaj's OTHER laptop (this PC has only the 7 snapshots of 8 Nov).
  `04_profile_static.py` (read-only) profiles extracted\. Measured on real files:
  * Static feed gtfs.de `latest-rv-free`: regional rail for ALL Germany, route_type 2 only, 221 agencies, 945 routes.
    Covers 7 Nov - 7 Dec 2026 (31 days); ~42k trips Mon-Fri, ~37k Sat, ~34k Sun. trips.txt has only route/service/trip id.
    Stops: 7,435 stations (location_type 1) + 8,307 platforms, all with parent. Times go up to 36:xx (73k rows after 24:00).
    Rough BW bbox: 3,326 stops, 20,169 trips touch it (~9.6k per weekday, 7.3k on Sun 8 Nov). Bbox includes bits of
    neighbouring states/countries - official boundary still needed.
  * Realtime coverage: of 33,826 static trips scheduled Sun 8 Nov only 7,060 (20.9 %) appear in the feed; BW-bbox 1,668 of
    7,324 (22.8 %). KPIs therefore describe trips WITH realtime data only - state in DQ report; check by agency later.
  * Task 4 stop-id check DONE: for matched trips, stop_sequence found in static 100 %, stop_id identical 100 %.
  * SPARSE UPDATES: the feed lists a median of 2 stops per trip (static median 12); only 8 % of trips list every stop;
    80 % include the first stop. GTFS-RT semantics: a delay propagates to later stops until the next update.
    Current loader stores only listed stops -> station KPIs biased toward first stops. Needs propagation
    (forward-fill along the static stop sequence, flag reported vs propagated). NOT implemented yet.
- Installed requirements.txt into conda env `ds` (sqlalchemy, psycopg, shapely, holidays, pytest were missing).

## Next tasks (in order)
3. Write a daily compaction job: keep only trips present in the static timetable (and the BW cut), write one compact Parquet file per day,
   delete raw .pb older than about 3 days. Estimated about 0.5 MB per snapshot after filtering (unverified).
4. Check stop_id match for the matched trips; compare number of static trips scheduled for today (calendar + calendar_dates) with the 7,170 seen.
5. Load static + passengers on real data, run `etl.quality_checks`, fix real-data surprises.
6. Then dashboards, boardings forecast, tests, documentation, presentations.
Open inputs from Sairaj: team size and roles, milestone and final dates, database/BI choice, official BW boundary GeoJSON,
passenger-data licence, railway-section GeoJSON (optional, unprofiled).

## Rules
Keep scripts read-only unless told otherwise; never delete his data without asking. Run tests on the test DB only (name must contain "test").
Say plainly what was and was not run against real data.
