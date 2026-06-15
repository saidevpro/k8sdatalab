from dataclasses import dataclass, field
from datetime import date, datetime

from flask import current_app

from . import geocoding, gold, routing, scoring


@dataclass
class SearchParams:
    origin_ids: list
    dest_ids: list
    access_m: dict = field(default_factory=dict)
    egress_m: dict = field(default_factory=dict)
    accessible_required: bool = False
    crowding_sensitivity: int = 0
    minimize_walking: bool = False
    max_transfers: int = 2
    hour: int = 8
    top_n: int = 3


def resolve_location(loc):
    if loc.get("station"):
        ids, name = gold.resolve_station(loc["station"])
        return ids, {}, name

    if loc.get("lat") is not None and loc.get("lon") is not None:
        lat, lon, label = float(loc["lat"]), float(loc["lon"]), loc.get("label", "point")
    elif loc.get("address"):
        lat, lon, label = geocoding.geocode(loc["address"])
    else:
        raise ValueError("location requires 'station', 'address', or 'lat'+'lon'")

    stops = gold.nearest_stops(
        lat, lon, current_app.config["NEAREST_STOPS"], current_app.config["MAX_ACCESS_WALK_M"]
    )
    ids = [s["stop_id"] for s in stops]
    access = {s["stop_id"]: s["distance_m"] for s in stops}
    return ids, access, label


def search(params, service_date):
    cfg = current_app.config
    dep_from = params.hour * 3600 - cfg["DEPARTURE_LOOKBACK_SEC"]
    dep_to = params.hour * 3600 + cfg["DEPARTURE_WINDOW_SEC"]
    candidates = routing.build_candidates(
        params.origin_ids, params.dest_ids, params.max_transfers, params.accessible_required, dep_from, dep_to
    )
    for c in candidates:
        c["walking_m"] += params.access_m.get(c["board_stop_id"], 0.0)
        c["walking_m"] += params.egress_m.get(c["alight_stop_id"], 0.0)

    ranked = scoring.rank_routes(candidates, params, service_date)
    return [_format(c, rank) for rank, c in enumerate(ranked, start=1)]


def params_from_request(data):
    origin_ids, access_m, origin_label = resolve_location(data["origin"])
    dest_ids, egress_m, dest_label = resolve_location(data["destination"])

    when = data.get("time")
    hour = datetime.strptime(when, "%H:%M").hour if when else datetime.now().hour

    params = SearchParams(
        origin_ids=origin_ids,
        dest_ids=dest_ids,
        access_m=access_m,
        egress_m=egress_m,
        accessible_required=bool(data.get("accessible_required", False)),
        crowding_sensitivity=int(data.get("crowding_sensitivity", 0)),
        minimize_walking=bool(data.get("minimize_walking", False)),
        max_transfers=int(data.get("max_transfers", 2)),
        hour=hour,
        top_n=int(data.get("top_n", current_app.config["TOP_ROUTES"])),
    )
    return params, origin_label, dest_label


def params_from_subscription(subscription):
    origin_ids, access_m, _ = resolve_location({"station": subscription.origin_station})
    dest_ids, egress_m, _ = resolve_location({"station": subscription.destination_station})
    return SearchParams(
        origin_ids=origin_ids,
        dest_ids=dest_ids,
        access_m=access_m,
        egress_m=egress_m,
        accessible_required=subscription.accessible_required,
        crowding_sensitivity=subscription.crowding_sensitivity,
        minimize_walking=subscription.minimize_walking,
        max_transfers=subscription.max_transfers,
        hour=subscription.notify_time.hour,
        top_n=current_app.config["TOP_ROUTES"],
    )


def _format(candidate, rank):
    return {
        "rank": rank,
        "score": candidate["score"],
        "lines": candidate["lines"],
        "transfers": candidate["transfers"],
        "board_stop": candidate["board_stop"],
        "alight_stop": candidate["alight_stop"],
        "transfer_stations": candidate["transfer_stations"],
        "duration_min": round(candidate["duration_sec"] / 60, 1),
        "walking_m": round(candidate["walking_m"], 1),
        "reliability_pct": round(candidate["reliability_pct"], 1),
        "crowding_est": round(candidate["crowding_est"], 1),
    }
