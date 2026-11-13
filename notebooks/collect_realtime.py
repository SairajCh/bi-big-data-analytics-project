"""Every-5-minutes job: download one GTFS-Realtime snapshot and load it into the warehouse.

    python -m pipeline.collect_realtime                    # download from realtime.gtfs.de
    python -m pipeline.collect_realtime --file snap.pb     # use a saved snapshot (.pb or .pb.gz)

Steps: download -> decode protobuf -> flatten to rows -> keep only trips in our timetable
(trips running only inside Baden-Wuerttemberg) -> load into psa.rt_stage_* -> run sql/60_realtime_upsert.sql.
The provider keeps no history, so every run that does not happen is data lost for good.
"""
import argparse
import gzip
import sys
import urllib.request
from datetime import UTC, date, datetime
from pathlib import Path

from google.transit import gtfs_realtime_pb2 as rt

from pipeline.db import USER_AGENT, bulk_insert, connect, log_run, run_sql_file

URL = "https://realtime.gtfs.de/realtime-free.pb"

STOP_COLUMNS = ["snapshot_utc", "trip_id", "start_date", "trip_status", "stop_sequence", "stop_id",
                "stop_status", "arrival_delay_s", "departure_delay_s",
                "predicted_arrival_utc", "predicted_departure_utc"]
TRIP_COLUMNS = ["snapshot_utc", "trip_id", "start_date", "trip_status", "n_stop_updates"]


def fetch() -> bytes:
    req = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


def _enum_name(enum_type, value: int) -> str:
    try:
        return enum_type.Name(value)
    except ValueError:
        return str(value)


def _ts(seconds: int) -> datetime | None:
    return datetime.fromtimestamp(seconds, UTC) if seconds else None


def flatten(feed: rt.FeedMessage):
    """Return (snapshot time, stop rows, trip rows, number of updates without a start_date)."""
    snapshot = datetime.fromtimestamp(feed.header.timestamp, UTC)
    trip_rel = rt.TripDescriptor.ScheduleRelationship
    stop_rel = rt.TripUpdate.StopTimeUpdate.ScheduleRelationship
    stops, trips, no_date = [], [], 0

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        trip = tu.trip
        if not trip.start_date:
            no_date += 1          # cannot tell which day it belongs to
            continue
        start = datetime.strptime(trip.start_date, "%Y%m%d").date()
        trip_status = _enum_name(trip_rel, trip.schedule_relationship)
        trips.append((snapshot, trip.trip_id, start, trip_status, len(tu.stop_time_update)))

        for su in tu.stop_time_update:
            arr = su.arrival if su.HasField("arrival") else None
            dep = su.departure if su.HasField("departure") else None
            stops.append((
                snapshot,
                trip.trip_id,
                start,
                trip_status,
                su.stop_sequence if su.HasField("stop_sequence") else None,
                su.stop_id or None,
                _enum_name(stop_rel, su.schedule_relationship),
                arr.delay if arr is not None and arr.HasField("delay") else None,
                dep.delay if dep is not None and dep.HasField("delay") else None,
                _ts(arr.time) if arr is not None and arr.HasField("time") else None,
                _ts(dep.time) if dep is not None and dep.HasField("time") else None,
            ))
    return snapshot, stops, trips, no_date


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", type=Path, help="load a saved snapshot instead of downloading")
    args = ap.parse_args(argv)

    started = datetime.now(UTC)
    with connect() as conn:
        try:
            if args.file:
                raw = args.file.read_bytes()
                if args.file.suffix == ".gz":
                    raw = gzip.decompress(raw)
            else:
                raw = fetch()
            feed = rt.FeedMessage()
            feed.ParseFromString(raw)
            snapshot, stops, trips, no_date = flatten(feed)
        except Exception as exc:  # network or decode problem: log it and fail the run
            log_run(conn, "collect_realtime", started, "error", error=repr(exc))
            conn.commit()
            raise

        # the source sometimes publishes an empty feed (header only) during an outage:
        # record it as an error so it does not count as a collection
        if not trips:
            log_run(conn, "collect_realtime", started, "error", snapshot,
                    error="empty feed from source (0 trips)", feed_bytes=len(raw))
            print(f"snapshot {snapshot:%Y-%m-%d %H:%M:%S} UTC: EMPTY feed from gtfs.de (0 trips) - nothing loaded")
            return 0

        already = conn.execute(
            "SELECT 1 FROM ops.etl_log WHERE job = 'collect_realtime' AND status = 'ok' AND snapshot_utc = %s",
            (snapshot,)).fetchone()
        if already:
            print(f"snapshot {snapshot:%Y-%m-%d %H:%M:%S} UTC already loaded - skipped")
            log_run(conn, "collect_realtime", started, "skipped", snapshot)
            return 0

        # keep only trips that exist in our (Baden-Wuerttemberg) timetable
        known = {r[0] for r in conn.execute("SELECT DISTINCT trip_id FROM dwh.dim_trip")}
        if not known:
            log_run(conn, "collect_realtime", started, "error", snapshot, error="no timetable loaded yet")
            conn.commit()
            raise SystemExit("No timetable in dwh.dim_trip - run pipeline.load_timetable first")
        bw_stops = [r for r in stops if r[1] in known]
        bw_trips = [r for r in trips if r[1] in known]

        conn.execute("TRUNCATE psa.rt_stage_stop, psa.rt_stage_trip")
        bulk_insert(conn, "psa.rt_stage_stop", STOP_COLUMNS, bw_stops)
        bulk_insert(conn, "psa.rt_stage_trip", TRIP_COLUMNS, bw_trips)
        run_sql_file(conn, "60_realtime_upsert.sql")

        log_run(conn, "collect_realtime", started, "ok", snapshot,
                feed_trips=len(trips), feed_stop_rows=len(stops), no_start_date=no_date,
                trips=len(bw_trips), stop_rows=len(bw_stops), feed_bytes=len(raw))
        print(f"snapshot {snapshot:%Y-%m-%d %H:%M:%S} UTC: {len(trips):,} trips in feed, "
              f"{len(bw_trips):,} in our timetable, {len(bw_stops):,} stop updates loaded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
