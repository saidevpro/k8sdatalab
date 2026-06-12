import trino
from flask import current_app


def _placeholders(values):
    return ", ".join(["?"] * len(values))


def _connect():
    cfg = current_app.config
    auth = None
    if cfg["TRINO_PASSWORD"]:
        auth = trino.auth.BasicAuthentication(cfg["TRINO_USER"], cfg["TRINO_PASSWORD"])
    return trino.dbapi.connect(
        host=cfg["TRINO_HOST"],
        port=cfg["TRINO_PORT"],
        user=cfg["TRINO_USER"],
        catalog=cfg["TRINO_CATALOG"],
        http_scheme=cfg["TRINO_HTTP_SCHEME"],
        auth=auth,
    )


def _query(sql, params=None):
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or [])
        return cur.fetchall()
    finally:
        conn.close()


def _gold():
    return f"{current_app.config['TRINO_CATALOG']}.{current_app.config['GOLD_SCHEMA']}"


def _silver():
    return f"{current_app.config['TRINO_CATALOG']}.{current_app.config['SILVER_SCHEMA']}"


def resolve_station(name):
    rows = _query(
        f"""
        SELECT DISTINCT stop_id, parent_station_name
        FROM {_gold()}.dim_stops
        WHERE stop_id IS NOT NULL
          AND (upper(trim(stop_name)) LIKE upper(trim(?)) || '%'
               OR upper(trim(parent_station_name)) LIKE upper(trim(?)) || '%')
        """,
        [name, name],
    )
    stop_ids = [int(r[0]) for r in rows]
    station_name = rows[0][1] if rows and rows[0][1] else name
    return stop_ids, station_name


def direct_routes(origin_ids, dest_ids, limit):
    if not origin_ids or not dest_ids:
        return []
    rows = _query(
        f"""
        WITH o AS (
            SELECT trip_id, route_short_name, route_type, stop_name, stop_sequence, departure_seconds
            FROM {_gold()}.trip_schedule
            WHERE stop_id IN ({_placeholders(origin_ids)}) AND departure_seconds IS NOT NULL
        ),
        d AS (
            SELECT trip_id, stop_name, stop_sequence, arrival_seconds
            FROM {_gold()}.trip_schedule
            WHERE stop_id IN ({_placeholders(dest_ids)}) AND arrival_seconds IS NOT NULL
        )
        SELECT o.route_short_name, o.route_type, o.stop_name, d.stop_name,
               o.departure_seconds, d.arrival_seconds, d.arrival_seconds - o.departure_seconds AS duration_sec
        FROM o JOIN d ON o.trip_id = d.trip_id AND d.stop_sequence > o.stop_sequence
        WHERE d.arrival_seconds > o.departure_seconds
        ORDER BY duration_sec
        LIMIT {int(limit)}
        """,
        list(origin_ids) + list(dest_ids),
    )
    return [
        {
            "transfers": 0,
            "lines": [r[0]],
            "route_types": [r[1]],
            "board_stop": r[2],
            "alight_stop": r[3],
            "stations": [r[2], r[3]],
            "transfer_stations": [],
            "duration_sec": int(r[6]),
            "walking_m": 0.0,
            "blocked_no_elevator": False,
        }
        for r in rows
    ]


def transfer_routes(origin_ids, dest_ids, limit):
    if not origin_ids or not dest_ids:
        return []
    rows = _query(
        f"""
        WITH leg1 AS (
            SELECT a.route_short_name AS line1, a.route_type AS rt1, a.stop_name AS board_stop,
                   a.departure_seconds AS dep1, x.stop_id AS x_stop, x.stop_name AS x_name,
                   x.arrival_seconds AS arr_x
            FROM {_gold()}.trip_schedule a
            JOIN {_gold()}.trip_schedule x ON a.trip_id = x.trip_id AND x.stop_sequence > a.stop_sequence
            WHERE a.stop_id IN ({_placeholders(origin_ids)}) AND a.departure_seconds IS NOT NULL
        ),
        leg2 AS (
            SELECT b.route_short_name AS line2, b.route_type AS rt2, y.stop_id AS y_stop,
                   y.stop_name AS y_name, y.departure_seconds AS dep_y, d.stop_name AS alight_stop,
                   d.arrival_seconds AS arr2
            FROM {_gold()}.trip_schedule b
            JOIN {_gold()}.trip_schedule y ON b.trip_id = y.trip_id
            JOIN {_gold()}.trip_schedule d ON b.trip_id = d.trip_id AND d.stop_sequence > y.stop_sequence
            WHERE d.stop_id IN ({_placeholders(dest_ids)}) AND d.arrival_seconds IS NOT NULL
        )
        SELECT leg1.line1, leg1.rt1, leg2.line2, leg2.rt2, leg1.board_stop, leg1.x_name,
               leg2.y_name, leg2.alight_stop, leg2.arr2 - leg1.dep1 AS duration_sec,
               t.pathway_length_m, t.has_stairs, t.has_elevator
        FROM leg1
        JOIN {_gold()}.transfer_walking t ON t.from_stop_id = leg1.x_stop
        JOIN leg2 ON leg2.y_stop = t.to_stop_id
        WHERE leg2.dep_y >= leg1.arr_x + COALESCE(t.min_transfer_time, {current_app.config['DEFAULT_TRANSFER_TIME_SEC']})
          AND leg2.arr2 > leg1.dep1
          AND leg1.line1 <> leg2.line2
        ORDER BY duration_sec
        LIMIT {int(limit)}
        """,
        list(origin_ids) + list(dest_ids),
    )
    return [
        {
            "transfers": 1,
            "lines": [r[0], r[2]],
            "route_types": [r[1], r[3]],
            "board_stop": r[4],
            "alight_stop": r[7],
            "stations": [r[4], r[5], r[6], r[7]],
            "transfer_stations": [r[5], r[6]],
            "duration_sec": int(r[8]),
            "walking_m": float(r[9]) if r[9] is not None else 0.0,
            "blocked_no_elevator": bool(r[10]) and not bool(r[11]),
        }
        for r in rows
    ]


def line_reliability(lines):
    lines = [l for l in lines if l]
    if not lines:
        return {}
    rows = _query(
        f"""
        SELECT upper(trim(published_line)) AS line_key, avg(pct_on_time) AS pct_on_time
        FROM {_gold()}.delays_by_stop
        WHERE upper(trim(published_line)) IN ({_placeholders(lines)})
        GROUP BY upper(trim(published_line))
        """,
        [l.upper().strip() for l in lines],
    )
    return {r[0]: float(r[1]) for r in rows if r[1] is not None}


def station_crowding(stations, hour, cat_jour):
    stations = [s for s in stations if s]
    if not stations:
        return {}
    rows = _query(
        f"""
        SELECT upper(trim(libelle_arret)) AS station_key, avg(estimated_validations) AS crowd
        FROM {_gold()}.crowding_features
        WHERE hour_start = ? AND cat_jour = ?
          AND upper(trim(libelle_arret)) IN ({_placeholders(stations)})
        GROUP BY upper(trim(libelle_arret))
        """,
        [int(hour), cat_jour] + [s.upper().strip() for s in stations],
    )
    return {r[0]: float(r[1]) for r in rows if r[1] is not None}


def elevator_status(stations):
    stations = [s for s in stations if s]
    if not stations:
        return {}
    rows = _query(
        f"""
        SELECT upper(trim(station_name)) AS station_key, min(pct_available) AS pct_available
        FROM {_gold()}.elevators_availability
        WHERE upper(trim(station_name)) IN ({_placeholders(stations)})
        GROUP BY upper(trim(station_name))
        """,
        [s.upper().strip() for s in stations],
    )
    return {r[0]: float(r[1]) for r in rows if r[1] is not None}


def day_category(service_date):
    rows = _query(
        f"""
        SELECT cat_jour FROM {_silver()}.day_type_calendar
        WHERE date = ? LIMIT 1
        """,
        [service_date.isoformat()],
    )
    if rows:
        return rows[0][0]
    return "SAHV" if service_date.weekday() >= 5 else "JOHV"
