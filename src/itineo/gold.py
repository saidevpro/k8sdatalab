import trino
from flask import current_app


def _placeholders(values):
    return ", ".join(["?"] * len(values))


def _connect():
    cfg = current_app.config
    auth = None
    if cfg["TRINO_PASSWORD"] and cfg["TRINO_HTTP_SCHEME"] == "https":
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


def _isoformat(value):
    return value.isoformat() if value is not None else None


def _geo_coordinates(geo_point):
    """Extract longitude/latitude from the WKT strings produced by the Gold jobs."""
    if not geo_point:
        return None, None
    try:
        coordinates = geo_point[geo_point.index("(") + 1 : geo_point.rindex(")")].split()
        return float(coordinates[0]), float(coordinates[1])
    except (AttributeError, ValueError, IndexError):
        return None, None


def _route_type_name(route_type):
    return {
        0: "tram",
        1: "metro",
        2: "rail",
        3: "bus",
        4: "ferry",
        5: "cable_tram",
        6: "aerial_lift",
        7: "funicular",
        11: "trolleybus",
        12: "monorail",
    }.get(route_type, "unknown")


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


def nearest_stops(lat, lon, limit, max_distance_m):
    rows = _query(
        f"""
        SELECT stop_id, stop_name,
               great_circle_distance(stop_lat, stop_lon, ?, ?) * 1000 AS distance_m
        FROM {_gold()}.dim_stops
        WHERE stop_lat IS NOT NULL AND stop_lon IS NOT NULL AND stop_id IS NOT NULL
        ORDER BY distance_m
        LIMIT {int(limit)}
        """,
        [float(lat), float(lon)],
    )
    stops = [{"stop_id": int(r[0]), "stop_name": r[1], "distance_m": float(r[2])} for r in rows]
    within = [s for s in stops if s["distance_m"] <= max_distance_m]
    if within:
        return within
    if stops and stops[0]["distance_m"] <= 5000:
        return stops[:1]
    raise ValueError("no transit stops found near this location")


def direct_routes(origin_ids, dest_ids, limit, dep_from, dep_to):
    if not origin_ids or not dest_ids:
        return []
    rows = _query(
        f"""
        WITH o AS (
            SELECT trip_id, route_short_name, route_type, stop_id, stop_name, stop_sequence, departure_seconds
            FROM {_gold()}.trip_schedule
            WHERE stop_id IN ({_placeholders(origin_ids)}) AND departure_seconds IS NOT NULL
              AND departure_seconds BETWEEN {int(dep_from)} AND {int(dep_to)}
        ),
        d AS (
            SELECT trip_id, stop_id, stop_name, stop_sequence, arrival_seconds
            FROM {_gold()}.trip_schedule
            WHERE stop_id IN ({_placeholders(dest_ids)}) AND arrival_seconds IS NOT NULL
        )
        SELECT o.route_short_name, o.route_type, o.stop_id, o.stop_name, d.stop_id, d.stop_name,
               MIN(d.arrival_seconds - o.departure_seconds) AS duration_sec
        FROM o JOIN d ON o.trip_id = d.trip_id AND d.stop_sequence > o.stop_sequence
        WHERE d.arrival_seconds > o.departure_seconds
        GROUP BY o.route_short_name, o.route_type, o.stop_id, o.stop_name, d.stop_id, d.stop_name
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
            "board_stop_id": int(r[2]),
            "board_stop": r[3],
            "alight_stop_id": int(r[4]),
            "alight_stop": r[5],
            "stations": [r[3], r[5]],
            "transfer_stations": [],
            "duration_sec": int(r[6]),
            "walking_m": 0.0,
            "blocked_no_elevator": False,
        }
        for r in rows
    ]


def transfer_routes(origin_ids, dest_ids, limit, dep_from, dep_to):
    if not origin_ids or not dest_ids:
        return []
    rows = _query(
        f"""
        WITH leg1 AS (
            SELECT a.route_short_name AS line1, a.route_type AS rt1, a.stop_id AS board_stop_id,
                   a.stop_name AS board_stop, a.departure_seconds AS dep1, x.stop_id AS x_stop,
                   x.stop_name AS x_name, x.arrival_seconds AS arr_x
            FROM {_gold()}.trip_schedule a
            JOIN {_gold()}.trip_schedule x ON a.trip_id = x.trip_id AND x.stop_sequence > a.stop_sequence
            WHERE a.stop_id IN ({_placeholders(origin_ids)}) AND a.departure_seconds IS NOT NULL
              AND a.departure_seconds BETWEEN {int(dep_from)} AND {int(dep_to)}
              AND x.arrival_seconds IS NOT NULL
              AND x.stop_id IN (SELECT from_stop_id FROM {_gold()}.transfer_walking)
        ),
        leg2 AS (
            SELECT y.route_short_name AS line2, y.route_type AS rt2, y.stop_id AS y_stop,
                   y.stop_name AS y_name, y.departure_seconds AS dep_y, d.stop_id AS alight_stop_id,
                   d.stop_name AS alight_stop, d.arrival_seconds AS arr2
            FROM {_gold()}.trip_schedule d
            JOIN {_gold()}.trip_schedule y ON d.trip_id = y.trip_id AND y.stop_sequence < d.stop_sequence
            WHERE d.stop_id IN ({_placeholders(dest_ids)}) AND d.arrival_seconds IS NOT NULL
              AND y.departure_seconds IS NOT NULL
              AND y.stop_id IN (SELECT to_stop_id FROM {_gold()}.transfer_walking)
        )
        SELECT leg1.line1, leg1.rt1, leg2.line2, leg2.rt2, leg1.board_stop_id, leg1.board_stop,
               leg1.x_name, leg2.y_name, leg2.alight_stop_id, leg2.alight_stop,
               MIN(leg2.arr2 - leg1.dep1) AS duration_sec,
               MIN(t.pathway_length_m) AS walking_m,
               bool_and(COALESCE(t.has_stairs, false)) AS has_stairs_all,
               bool_or(COALESCE(t.has_elevator, false)) AS has_elevator_any
        FROM leg1
        JOIN {_gold()}.transfer_walking t ON t.from_stop_id = leg1.x_stop
        JOIN leg2 ON leg2.y_stop = t.to_stop_id
        WHERE leg2.dep_y >= leg1.arr_x + COALESCE(t.min_transfer_time, {current_app.config['DEFAULT_TRANSFER_TIME_SEC']})
          AND leg2.dep_y <= leg1.arr_x + {current_app.config['MAX_TRANSFER_WAIT_SEC']}
          AND leg2.arr2 > leg1.dep1
          AND leg1.line1 <> leg2.line2
        GROUP BY leg1.line1, leg1.rt1, leg2.line2, leg2.rt2, leg1.board_stop_id, leg1.board_stop,
                 leg1.x_name, leg2.y_name, leg2.alight_stop_id, leg2.alight_stop
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
            "board_stop_id": int(r[4]),
            "board_stop": r[5],
            "alight_stop_id": int(r[8]),
            "alight_stop": r[9],
            "stations": [r[5], r[6], r[7], r[9]],
            "transfer_stations": [r[6], r[7]],
            "duration_sec": int(r[10]),
            "walking_m": float(r[11]) if r[11] is not None else 0.0,
            "blocked_no_elevator": bool(r[12]) and not bool(r[13]),
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
        WHERE "date" = CAST(? AS DATE) LIMIT 1
        """,
        [service_date.isoformat()],
    )
    if rows:
        return rows[0][0]
    return "SAHV" if service_date.weekday() >= 5 else "JOHV"


def accessibility_summary():
    """Return network-wide station accessibility indicators from the gold layer."""
    rows = _query(
        f"""
        WITH stations AS (
            SELECT parent_station_id,
                   max(CASE WHEN wheelchair_boarding = 1 THEN 1 ELSE 0 END) AS accessible_flag,
                   max(COALESCE(nb_elevators, 0)) AS nb_elevators,
                   max(COALESCE(nb_available, 0)) AS nb_available,
                   max(pct_elevators_available) AS pct_elevators_available,
                   max(pct_uptime_30d) AS pct_uptime_30d
            FROM {_gold()}.station_accessibility
            WHERE parent_station_id IS NOT NULL
            GROUP BY parent_station_id
        )
        SELECT count(*) AS total_stations,
               sum(accessible_flag) AS accessible_stations,
               count(*) - sum(accessible_flag) AS non_accessible_or_unknown_stations,
               round(100.0 * sum(accessible_flag) / nullif(count(*), 0), 1) AS accessible_rate,
               sum(CASE WHEN nb_elevators > 0 THEN 1 ELSE 0 END) AS stations_with_elevators,
               sum(CASE WHEN nb_elevators > 0 AND nb_available = 0 THEN 1 ELSE 0 END)
                   AS stations_without_available_elevator,
               round(avg(pct_elevators_available), 1) AS avg_elevator_availability,
               round(avg(pct_uptime_30d), 1) AS avg_elevator_uptime_30d
        FROM stations
        """
    )
    if not rows:
        return {
            "total_stations": 0,
            "accessible_stations": 0,
            "non_accessible_or_unknown_stations": 0,
            "accessible_rate": 0.0,
            "stations_with_elevators": 0,
            "stations_without_available_elevator": 0,
            "avg_elevator_availability": None,
            "avg_elevator_uptime_30d": None,
        }

    row = rows[0]
    return {
        "total_stations": int(row[0] or 0),
        "accessible_stations": int(row[1] or 0),
        "non_accessible_or_unknown_stations": int(row[2] or 0),
        "accessible_rate": float(row[3] or 0.0),
        "stations_with_elevators": int(row[4] or 0),
        "stations_without_available_elevator": int(row[5] or 0),
        "avg_elevator_availability": float(row[6]) if row[6] is not None else None,
        "avg_elevator_uptime_30d": float(row[7]) if row[7] is not None else None,
    }


def accessibility_stations(search=None, accessible=None, limit=100, offset=0, station_id=None):
    """Return one row per parent station, optionally filtered and paginated."""
    where = ["parent_station_id IS NOT NULL"]
    params = []
    if station_id is not None:
        where.append("parent_station_id = ?")
        params.append(int(station_id))
    if search:
        where.append("upper(trim(parent_station_name)) LIKE '%' || upper(trim(?)) || '%'")
        params.append(search)

    having = ""
    if accessible is not None:
        having = "HAVING max(CASE WHEN wheelchair_boarding = 1 THEN 1 ELSE 0 END) = ?"
        params.append(1 if accessible else 0)

    rows = _query(
        f"""
        SELECT parent_station_id,
               max(parent_station_name) AS station_name,
               max(CASE WHEN wheelchair_boarding = 1 THEN 1 ELSE 0 END) AS accessible_flag,
               count(DISTINCT stop_id) AS stop_count,
               max(COALESCE(nb_elevators, 0)) AS nb_elevators,
               max(COALESCE(nb_available, 0)) AS nb_available,
               max(pct_elevators_available) AS pct_elevators_available,
               max(COALESCE(nb_outages_30d, 0)) AS nb_outages_30d,
               max(pct_uptime_30d) AS pct_uptime_30d,
               max(COALESCE(nb_lines_served, 0)) AS nb_lines_served,
               max(COALESCE(nb_active_disruptions_today, 0)) AS nb_active_disruptions_today,
               max(avg_daily_validations_30d) AS avg_daily_validations_30d,
               max(peak_hour_pct_validations) AS peak_hour_pct_validations
        FROM {_gold()}.station_accessibility
        WHERE {' AND '.join(where)}
        GROUP BY parent_station_id
        {having}
        ORDER BY station_name
        OFFSET {int(offset)} LIMIT {int(limit)}
        """,
        params,
    )
    return [
        {
            "station_id": int(row[0]),
            "station_name": row[1],
            "accessible": bool(row[2]),
            "accessibility_status": "accessible" if row[2] else "non_accessible_or_unknown",
            "stop_count": int(row[3] or 0),
            "elevators": {
                "total": int(row[4] or 0),
                "available": int(row[5] or 0),
                "availability_pct": float(row[6]) if row[6] is not None else None,
                "outages_30d": int(row[7] or 0),
                "uptime_30d_pct": float(row[8]) if row[8] is not None else None,
            },
            "lines_count": int(row[9] or 0),
            "active_disruptions_today": int(row[10] or 0),
            "avg_daily_validations_30d": float(row[11]) if row[11] is not None else None,
            "peak_hour_validations_pct": float(row[12]) if row[12] is not None else None,
        }
        for row in rows
    ]


def elevator_availability(search=None, available=None, limit=100, offset=0):
    """Return current elevator availability by station and transport mode."""
    where = ["1 = 1"]
    params = []
    if search:
        where.append("upper(trim(station_name)) LIKE '%' || upper(trim(?)) || '%'")
        params.append(search)
    if available is not None:
        where.append("nb_available > 0" if available else "nb_available = 0")

    rows = _query(
        f"""
        SELECT station_area_id, station_name, transport_mode,
               nb_elevators, nb_available, pct_available,
               station_latitude, station_longitude
        FROM {_gold()}.elevators_availability
        WHERE {' AND '.join(where)}
        ORDER BY station_name, transport_mode
        OFFSET {int(offset)} LIMIT {int(limit)}
        """,
        params,
    )
    return [
        {
            "station_area_id": row[0],
            "station_name": row[1],
            "transport_mode": row[2],
            "elevators_total": int(row[3] or 0),
            "elevators_available": int(row[4] or 0),
            "elevators_unavailable": int((row[3] or 0) - (row[4] or 0)),
            "availability_pct": float(row[5]) if row[5] is not None else None,
            "latitude": float(row[6]) if row[6] is not None else None,
            "longitude": float(row[7]) if row[7] is not None else None,
        }
        for row in rows
    ]


def accessibility_transfers(search=None, accessible=None, max_distance_m=None, limit=100, offset=0):
    """Return accessible-transfer facts enriched with the endpoint station names."""
    where = ["1 = 1"]
    params = []
    if search:
        where.append(
            "(upper(trim(fs.stop_name)) LIKE '%' || upper(trim(?)) || '%' "
            "OR upper(trim(ts.stop_name)) LIKE '%' || upper(trim(?)) || '%')"
        )
        params.extend([search, search])
    if accessible is not None:
        where.append("t.accessible_transfer = ?")
        params.append(bool(accessible))
    if max_distance_m is not None:
        where.append("t.pathway_length_m <= ?")
        params.append(float(max_distance_m))

    rows = _query(
        f"""
        SELECT t.from_stop_id, fs.stop_name AS from_stop_name,
               t.to_stop_id, ts.stop_name AS to_stop_name,
               t.transfer_type, t.min_transfer_time,
               t.pathway_length_m, t.pathway_traversal_time,
               t.has_stairs, t.has_escalator, t.has_elevator,
               t.nb_elevators, t.nb_available, t.pct_uptime_30d,
               t.accessible_transfer
        FROM {_gold()}.transfer_accessibility t
        LEFT JOIN {_gold()}.dim_stops fs ON t.from_stop_id = fs.stop_id
        LEFT JOIN {_gold()}.dim_stops ts ON t.to_stop_id = ts.stop_id
        WHERE {' AND '.join(where)}
        ORDER BY t.pathway_length_m DESC NULLS LAST
        OFFSET {int(offset)} LIMIT {int(limit)}
        """,
        params,
    )
    return [
        {
            "from_stop_id": int(row[0]),
            "from_stop_name": row[1],
            "to_stop_id": int(row[2]),
            "to_stop_name": row[3],
            "transfer_type": int(row[4]) if row[4] is not None else None,
            "min_transfer_time_sec": int(row[5]) if row[5] is not None else None,
            "pathway_length_m": float(row[6]) if row[6] is not None else None,
            "pathway_traversal_time_sec": int(row[7]) if row[7] is not None else None,
            "has_stairs": bool(row[8]),
            "has_escalator": bool(row[9]),
            "has_elevator": bool(row[10]),
            "elevators_total": int(row[11]) if row[11] is not None else None,
            "elevators_available": int(row[12]) if row[12] is not None else None,
            "elevator_uptime_30d_pct": float(row[13]) if row[13] is not None else None,
            "accessible": bool(row[14]),
        }
        for row in rows
    ]


def elevator_reliability(search=None, limit=100, offset=0):
    """Return 30/90-day elevator reliability indicators by station."""
    where = ["1 = 1"]
    params = []
    if search:
        where.append("upper(trim(station_name)) LIKE '%' || upper(trim(?)) || '%'")
        params.append(search)

    rows = _query(
        f"""
        SELECT station_area_id, station_name, transport_mode, nb_elevators,
               nb_outages_30d, downtime_minutes_30d, avg_outage_minutes_30d,
               nb_outages_90d, downtime_minutes_90d, avg_outage_minutes_90d,
               pct_uptime_30d, pct_uptime_90d
        FROM {_gold()}.elevators_reliability
        WHERE {' AND '.join(where)}
        ORDER BY pct_uptime_30d ASC NULLS FIRST, station_name
        OFFSET {int(offset)} LIMIT {int(limit)}
        """,
        params,
    )
    return [
        {
            "station_area_id": row[0],
            "station_name": row[1],
            "transport_mode": row[2],
            "elevators_total": int(row[3] or 0),
            "outages_30d": int(row[4] or 0),
            "downtime_minutes_30d": float(row[5] or 0.0),
            "avg_outage_minutes_30d": float(row[6]) if row[6] is not None else None,
            "outages_90d": int(row[7] or 0),
            "downtime_minutes_90d": float(row[8] or 0.0),
            "avg_outage_minutes_90d": float(row[9]) if row[9] is not None else None,
            "uptime_30d_pct": float(row[10]) if row[10] is not None else None,
            "uptime_90d_pct": float(row[11]) if row[11] is not None else None,
        }
        for row in rows
    ]


def elevator_outages(search=None, ongoing=None, since_days=None, limit=100, offset=0):
    """Return elevator outage events, including the duration of ongoing events."""
    where = ["1 = 1"]
    params = []
    if search:
        where.append("upper(trim(station_name)) LIKE '%' || upper(trim(?)) || '%'")
        params.append(search)
    if ongoing is not None:
        where.append("ended_at IS NULL" if ongoing else "ended_at IS NOT NULL")
    if since_days is not None:
        where.append(
            f"status_updated_at >= localtimestamp - INTERVAL '{int(since_days)}' DAY"
        )

    rows = _query(
        f"""
        SELECT elevator_key, elevator_id, station_area_id, station_name,
               transport_mode, status, status_reason, status_updated_at, ended_at,
               COALESCE(
                   downtime_sec,
                   date_diff('second', status_updated_at, localtimestamp)
               ) AS effective_downtime_sec
        FROM {_gold()}.elevators_downtime
        WHERE {' AND '.join(where)}
        ORDER BY status_updated_at DESC NULLS LAST
        OFFSET {int(offset)} LIMIT {int(limit)}
        """,
        params,
    )
    return [
        {
            "elevator_key": row[0],
            "elevator_id": row[1],
            "station_area_id": row[2],
            "station_name": row[3],
            "transport_mode": row[4],
            "status": row[5],
            "reason": row[6],
            "started_at": _isoformat(row[7]),
            "ended_at": _isoformat(row[8]),
            "ongoing": row[8] is None,
            "downtime_sec": int(row[9]) if row[9] is not None else None,
            "downtime_minutes": round(float(row[9]) / 60, 1) if row[9] is not None else None,
        }
        for row in rows
    ]


def crowding_summary(date_from=None, date_to=None):
    """Return global daily-validation indicators without counting hourly rows twice."""
    where = ["id_refa_lda IS NOT NULL"]
    params = []
    if date_from:
        where.append("jour >= CAST(? AS DATE)")
        params.append(date_from)
    if date_to:
        where.append("jour <= CAST(? AS DATE)")
        params.append(date_to)

    rows = _query(
        f"""
        WITH stop_days AS (
            SELECT jour, id_refa_lda, stif_trns, stif_res, stif_arret,
                   max(daily_total_validations) AS total_validations
            FROM {_gold()}.crowding_features
            WHERE {' AND '.join(where)}
            GROUP BY jour, id_refa_lda, stif_trns, stif_res, stif_arret
        ),
        station_days AS (
            SELECT jour, id_refa_lda, sum(total_validations) AS total_validations
            FROM stop_days
            GROUP BY jour, id_refa_lda
        )
        SELECT count(DISTINCT id_refa_lda) AS stations,
               min(jour) AS date_from,
               max(jour) AS date_to,
               sum(total_validations) AS total_validations,
               round(avg(total_validations), 1) AS avg_daily_validations_per_station,
               max(total_validations) AS max_daily_validations_at_one_station
        FROM station_days
        """,
        params,
    )
    row = rows[0] if rows else (0, None, None, 0, None, None)
    return {
        "stations": int(row[0] or 0),
        "date_from": _isoformat(row[1]),
        "date_to": _isoformat(row[2]),
        "total_validations": int(row[3] or 0),
        "avg_daily_validations_per_station": float(row[4]) if row[4] is not None else None,
        "max_daily_validations_at_one_station": int(row[5]) if row[5] is not None else None,
    }


def crowding_stations(
    search=None,
    hour=None,
    cat_jour=None,
    date_from=None,
    date_to=None,
    limit=100,
    offset=0,
):
    """Rank stations by observed and estimated validations."""
    where = ["id_refa_lda IS NOT NULL"]
    params = []
    if search:
        where.append("upper(trim(libelle_arret)) LIKE '%' || upper(trim(?)) || '%'")
        params.append(search)
    if hour is not None:
        where.append("hour_start = ?")
        params.append(int(hour))
    if cat_jour:
        where.append("upper(trim(cat_jour)) = upper(trim(?))")
        params.append(cat_jour)
    if date_from:
        where.append("jour >= CAST(? AS DATE)")
        params.append(date_from)
    if date_to:
        where.append("jour <= CAST(? AS DATE)")
        params.append(date_to)

    rows = _query(
        f"""
        WITH filtered AS (
            SELECT *
            FROM {_gold()}.crowding_features
            WHERE {' AND '.join(where)}
        ),
        stop_days AS (
            SELECT jour, id_refa_lda, stif_trns, stif_res, stif_arret,
                   max(libelle_arret) AS station_name,
                   max(daily_total_validations) AS daily_validations,
                   max(geo_point) AS geo_point
            FROM filtered
            GROUP BY jour, id_refa_lda, stif_trns, stif_res, stif_arret
        ),
        station_days AS (
            SELECT jour, id_refa_lda,
                   max(station_name) AS station_name,
                   sum(daily_validations) AS daily_validations,
                   max(geo_point) AS geo_point
            FROM stop_days
            GROUP BY jour, id_refa_lda
        ),
        daily_stats AS (
            SELECT id_refa_lda,
                   max(station_name) AS station_name,
                   round(avg(daily_validations), 1) AS avg_daily_validations,
                   max(geo_point) AS geo_point
            FROM station_days
            GROUP BY id_refa_lda
        ),
        station_hours AS (
            SELECT f.jour, f.id_refa_lda, f.hour_start,
                   sum(f.estimated_validations) AS estimated_validations,
                   100.0 * sum(f.estimated_validations)
                       / nullif(max(d.daily_validations), 0) AS validations_pct
            FROM filtered f
            JOIN station_days d
              ON f.jour = d.jour AND f.id_refa_lda = d.id_refa_lda
            GROUP BY f.jour, f.id_refa_lda, f.hour_start
        ),
        hourly_stats AS (
            SELECT id_refa_lda,
                   round(avg(estimated_validations), 1) AS avg_estimated_validations,
                   round(max(estimated_validations), 1) AS peak_estimated_validations,
                   round(max(validations_pct), 1) AS peak_hour_validations_pct
            FROM station_hours
            GROUP BY id_refa_lda
        ),
        modes AS (
            SELECT id_refa_lda,
                   array_sort(array_agg(stif_trns)) AS transport_modes
            FROM (
                SELECT DISTINCT id_refa_lda, stif_trns
                FROM filtered
                WHERE stif_trns IS NOT NULL
            ) distinct_modes
            GROUP BY id_refa_lda
        )
        SELECT d.id_refa_lda, d.station_name, d.avg_daily_validations,
               h.avg_estimated_validations, h.peak_estimated_validations,
               h.peak_hour_validations_pct, d.geo_point, m.transport_modes
        FROM daily_stats d
        JOIN hourly_stats h ON d.id_refa_lda = h.id_refa_lda
        LEFT JOIN modes m ON d.id_refa_lda = m.id_refa_lda
        ORDER BY h.avg_estimated_validations DESC NULLS LAST, d.station_name
        OFFSET {int(offset)} LIMIT {int(limit)}
        """,
        params,
    )
    items = []
    for row in rows:
        longitude, latitude = _geo_coordinates(row[6])
        items.append(
            {
                "station_id": int(row[0]),
                "station_name": row[1],
                "avg_daily_validations": float(row[2]) if row[2] is not None else None,
                "avg_estimated_validations": float(row[3]) if row[3] is not None else None,
                "peak_estimated_validations": float(row[4]) if row[4] is not None else None,
                "peak_hour_validations_pct": float(row[5]) if row[5] is not None else None,
                "longitude": longitude,
                "latitude": latitude,
                "transport_modes": [mode for mode in (row[7] or []) if mode is not None],
            }
        )
    return items


def crowding_timeseries(search=None, date_from=None, date_to=None, limit=100, offset=0):
    """Return total validations per day from the dashboard-ready Gold table."""
    where = ["1 = 1"]
    params = []
    if search:
        where.append("upper(trim(libelle_arret)) LIKE '%' || upper(trim(?)) || '%'")
        params.append(search)
    if date_from:
        where.append("jour >= CAST(? AS DATE)")
        params.append(date_from)
    if date_to:
        where.append("jour <= CAST(? AS DATE)")
        params.append(date_to)

    rows = _query(
        f"""
        SELECT jour, sum(total_validations) AS total_validations,
               count(DISTINCT id_refa_lda) AS stations
        FROM {_gold()}.validations_daily_by_stop
        WHERE {' AND '.join(where)}
        GROUP BY jour
        ORDER BY jour DESC
        OFFSET {int(offset)} LIMIT {int(limit)}
        """,
        params,
    )
    return [
        {
            "date": _isoformat(row[0]),
            "total_validations": int(row[1] or 0),
            "stations": int(row[2] or 0),
        }
        for row in rows
    ]


def crowding_ticket_categories(search=None, date_from=None, date_to=None, limit=100, offset=0):
    """Return validation volumes and shares by ticket category."""
    where = ["1 = 1"]
    params = []
    if search:
        where.append("upper(trim(libelle_arret)) LIKE '%' || upper(trim(?)) || '%'")
        params.append(search)
    if date_from:
        where.append("jour >= CAST(? AS DATE)")
        params.append(date_from)
    if date_to:
        where.append("jour <= CAST(? AS DATE)")
        params.append(date_to)

    rows = _query(
        f"""
        SELECT categorie_titre,
               sum(nb_vald) AS validations,
               round(100.0 * sum(nb_vald) / nullif(sum(sum(nb_vald)) OVER (), 0), 1)
                   AS validations_pct
        FROM {_gold()}.validations_by_category
        WHERE {' AND '.join(where)}
        GROUP BY categorie_titre
        ORDER BY validations DESC NULLS LAST
        OFFSET {int(offset)} LIMIT {int(limit)}
        """,
        params,
    )
    return [
        {
            "ticket_category": row[0],
            "validations": int(row[1] or 0),
            "validations_pct": float(row[2]) if row[2] is not None else None,
        }
        for row in rows
    ]


def delays_summary(line=None):
    """Return weighted network or line-level punctuality indicators."""
    where = ["1 = 1"]
    params = []
    if line:
        where.append(
            "(upper(trim(published_line)) LIKE '%' || upper(trim(?)) || '%' "
            "OR upper(trim(line_ref)) LIKE '%' || upper(trim(?)) || '%')"
        )
        params.extend([line, line])

    rows = _query(
        f"""
        SELECT count(DISTINCT stop_point_ref) AS stops,
               count(DISTINCT COALESCE(published_line, line_ref)) AS lines,
               sum(nb_passages) AS passages,
               sum(nb_delayed) AS delayed_passages,
               round(
                   sum(avg_arrival_delay_sec * nb_passages) / nullif(sum(nb_passages), 0),
                   1
               ) AS avg_arrival_delay_sec,
               max(max_arrival_delay_sec) AS max_arrival_delay_sec,
               round(
                   100.0 * (1 - CAST(sum(nb_delayed) AS DOUBLE) / nullif(sum(nb_passages), 0)),
                   1
               ) AS pct_on_time
        FROM {_gold()}.delays_by_stop
        WHERE {' AND '.join(where)}
        """,
        params,
    )
    row = rows[0] if rows else (0, 0, 0, 0, None, None, None)
    return {
        "stops": int(row[0] or 0),
        "lines": int(row[1] or 0),
        "passages": int(row[2] or 0),
        "delayed_passages": int(row[3] or 0),
        "avg_arrival_delay_sec": float(row[4]) if row[4] is not None else None,
        "max_arrival_delay_sec": int(row[5]) if row[5] is not None else None,
        "on_time_pct": float(row[6]) if row[6] is not None else None,
    }


def delays_by_line(line=None, limit=100, offset=0):
    """Rank lines using passage-weighted punctuality metrics."""
    where = ["1 = 1"]
    params = []
    if line:
        where.append(
            "(upper(trim(published_line)) LIKE '%' || upper(trim(?)) || '%' "
            "OR upper(trim(line_ref)) LIKE '%' || upper(trim(?)) || '%')"
        )
        params.extend([line, line])

    rows = _query(
        f"""
        SELECT line_ref, published_line,
               count(DISTINCT stop_point_ref) AS stops,
               sum(nb_passages) AS passages,
               sum(nb_delayed) AS delayed_passages,
               round(
                   sum(avg_arrival_delay_sec * nb_passages) / nullif(sum(nb_passages), 0),
                   1
               ) AS avg_arrival_delay_sec,
               max(max_arrival_delay_sec) AS max_arrival_delay_sec,
               round(
                   100.0 * (1 - CAST(sum(nb_delayed) AS DOUBLE) / nullif(sum(nb_passages), 0)),
                   1
               ) AS pct_on_time
        FROM {_gold()}.delays_by_stop
        WHERE {' AND '.join(where)}
        GROUP BY line_ref, published_line
        ORDER BY pct_on_time ASC NULLS FIRST, passages DESC
        OFFSET {int(offset)} LIMIT {int(limit)}
        """,
        params,
    )
    return [
        {
            "line_ref": row[0],
            "published_line": row[1],
            "stops": int(row[2] or 0),
            "passages": int(row[3] or 0),
            "delayed_passages": int(row[4] or 0),
            "avg_arrival_delay_sec": float(row[5]) if row[5] is not None else None,
            "max_arrival_delay_sec": int(row[6]) if row[6] is not None else None,
            "on_time_pct": float(row[7]) if row[7] is not None else None,
        }
        for row in rows
    ]


def recent_delays(line=None, min_delay_sec=60, since_hours=None, limit=100, offset=0):
    """Return the most recently ingested delayed passages."""
    where = [
        "greatest(COALESCE(arrival_delay_sec, 0), COALESCE(departure_delay_sec, 0)) >= ?"
    ]
    params = [int(min_delay_sec)]
    if line:
        where.append(
            "(upper(trim(published_line)) LIKE '%' || upper(trim(?)) || '%' "
            "OR upper(trim(line_ref)) LIKE '%' || upper(trim(?)) || '%')"
        )
        params.extend([line, line])
    if since_hours is not None:
        where.append(
            f"batch_time >= localtimestamp - INTERVAL '{int(since_hours)}' HOUR"
        )

    rows = _query(
        f"""
        SELECT journey_ref, stop_point_ref, line_ref, published_line, direction,
               destination_name, aimed_arrival, expected_arrival,
               arrival_delay_sec, departure_delay_sec,
               arrival_status, departure_status, batch_time
        FROM {_gold()}.next_stop_delays
        WHERE {' AND '.join(where)}
        ORDER BY batch_time DESC NULLS LAST,
                 greatest(COALESCE(arrival_delay_sec, 0), COALESCE(departure_delay_sec, 0)) DESC
        OFFSET {int(offset)} LIMIT {int(limit)}
        """,
        params,
    )
    return [
        {
            "journey_ref": row[0],
            "stop_point_ref": row[1],
            "line_ref": row[2],
            "published_line": row[3],
            "direction": row[4],
            "destination_name": row[5],
            "aimed_arrival": _isoformat(row[6]),
            "expected_arrival": _isoformat(row[7]),
            "arrival_delay_sec": int(row[8]) if row[8] is not None else None,
            "departure_delay_sec": int(row[9]) if row[9] is not None else None,
            "arrival_status": row[10],
            "departure_status": row[11],
            "batch_time": _isoformat(row[12]),
        }
        for row in rows
    ]


def active_disruptions(search=None, line=None, severity=None, limit=100, offset=0):
    """Return currently active disruption messages."""
    where = ["is_active = true"]
    params = []
    if search:
        where.append(
            "(upper(COALESCE(title, '')) LIKE '%' || upper(trim(?)) || '%' "
            "OR upper(COALESCE(short_message, '')) LIKE '%' || upper(trim(?)) || '%' "
            "OR upper(COALESCE(message, '')) LIKE '%' || upper(trim(?)) || '%')"
        )
        params.extend([search, search, search])
    if line:
        where.append(
            "any_match(COALESCE(impacted_lines, CAST(ARRAY[] AS ARRAY(VARCHAR))), "
            "line_id -> upper(trim(line_id)) = upper(trim(?)))"
        )
        params.append(line)
    if severity:
        where.append("upper(trim(severity)) = upper(trim(?))")
        params.append(severity)

    rows = _query(
        f"""
        SELECT id, cause, severity, title, short_message, message,
               begin_time, end_time, last_update, impacted_lines, batch_time
        FROM {_gold()}.disruptions_active
        WHERE {' AND '.join(where)}
        ORDER BY last_update DESC NULLS LAST
        OFFSET {int(offset)} LIMIT {int(limit)}
        """,
        params,
    )
    return [
        {
            "id": row[0],
            "cause": row[1],
            "severity": row[2],
            "title": row[3],
            "short_message": row[4],
            "message": row[5],
            "begin_time": _isoformat(row[6]),
            "end_time": _isoformat(row[7]),
            "last_update": _isoformat(row[8]),
            "impacted_lines": row[9] or [],
            "batch_time": _isoformat(row[10]),
        }
        for row in rows
    ]


def disruptions_history(line=None, date_from=None, date_to=None, limit=100, offset=0):
    """Return daily disruption counts by line."""
    where = ["1 = 1"]
    params = []
    if line:
        where.append("upper(trim(line_id)) LIKE '%' || upper(trim(?)) || '%'")
        params.append(line)
    if date_from:
        where.append("day >= CAST(? AS DATE)")
        params.append(date_from)
    if date_to:
        where.append("day <= CAST(? AS DATE)")
        params.append(date_to)

    rows = _query(
        f"""
        SELECT line_id, day, nb_disruptions, severities
        FROM {_gold()}.disruptions_by_line
        WHERE {' AND '.join(where)}
        ORDER BY day DESC, nb_disruptions DESC
        OFFSET {int(offset)} LIMIT {int(limit)}
        """,
        params,
    )
    return [
        {
            "line_id": row[0],
            "date": _isoformat(row[1]),
            "disruptions": int(row[2] or 0),
            "severities": row[3] or [],
        }
        for row in rows
    ]


def network_lines(search=None, limit=100, offset=0):
    """Return the line catalogue and station coverage from station_lines."""
    where = ["route_id IS NOT NULL"]
    params = []
    if search:
        where.append(
            "(upper(trim(route_id)) LIKE '%' || upper(trim(?)) || '%' "
            "OR upper(trim(route_short_name)) LIKE '%' || upper(trim(?)) || '%' "
            "OR upper(trim(route_long_name)) LIKE '%' || upper(trim(?)) || '%')"
        )
        params.extend([search, search, search])

    rows = _query(
        f"""
        SELECT route_id, max(route_short_name), max(route_long_name),
               max(route_type), max(agency_id),
               count(DISTINCT COALESCE(parent_station_id, stop_id)) AS stations,
               count(DISTINCT stop_id) AS stops
        FROM {_gold()}.station_lines
        WHERE {' AND '.join(where)}
        GROUP BY route_id
        ORDER BY max(route_type), max(route_short_name), route_id
        OFFSET {int(offset)} LIMIT {int(limit)}
        """,
        params,
    )
    return [
        {
            "route_id": row[0],
            "route_short_name": row[1],
            "route_long_name": row[2],
            "route_type": int(row[3]) if row[3] is not None else None,
            "transport_mode": _route_type_name(int(row[3])) if row[3] is not None else "unknown",
            "agency_id": row[4],
            "stations": int(row[5] or 0),
            "stops": int(row[6] or 0),
        }
        for row in rows
    ]


def network_line_stations(route_id, limit=500, offset=0):
    """Return the unique stations served by one GTFS route."""
    rows = _query(
        f"""
        SELECT COALESCE(sl.parent_station_id, sl.stop_id) AS station_id,
               max(COALESCE(sl.parent_station_name, ds.stop_name)) AS station_name,
               max(COALESCE(ds.parent_station_lat, ds.stop_lat)) AS latitude,
               max(COALESCE(ds.parent_station_lon, ds.stop_lon)) AS longitude,
               count(DISTINCT sl.stop_id) AS stops
        FROM {_gold()}.station_lines sl
        LEFT JOIN {_gold()}.dim_stops ds ON sl.stop_id = ds.stop_id
        WHERE sl.route_id = ?
        GROUP BY COALESCE(sl.parent_station_id, sl.stop_id)
        ORDER BY station_name
        OFFSET {int(offset)} LIMIT {int(limit)}
        """,
        [route_id],
    )
    return [
        {
            "station_id": int(row[0]),
            "station_name": row[1],
            "latitude": float(row[2]) if row[2] is not None else None,
            "longitude": float(row[3]) if row[3] is not None else None,
            "stops": int(row[4] or 0),
        }
        for row in rows
    ]
