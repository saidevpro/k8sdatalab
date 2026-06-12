from flask import current_app

from . import gold


def _normalize(values, lower_is_better):
    lo, hi = min(values), max(values)
    if hi == lo:
        return [1.0 for _ in values]
    if lower_is_better:
        return [(hi - v) / (hi - lo) for v in values]
    return [(v - lo) / (hi - lo) for v in values]


def _weights(subscription):
    cfg = current_app.config
    w = {
        "duration": cfg["WEIGHT_DURATION"],
        "reliability": cfg["WEIGHT_RELIABILITY"],
        "crowding": cfg["WEIGHT_CROWDING"] * (1 + 0.5 * subscription.crowding_sensitivity),
        "walking": cfg["WEIGHT_WALKING"] * (2 if subscription.minimize_walking else 1),
    }
    total = sum(w.values())
    return {k: v / total for k, v in w.items()}


def rank_routes(candidates, subscription, service_date):
    if not candidates:
        return []

    hour = subscription.notify_time.hour
    cat_jour = gold.day_category(service_date)

    all_lines = {l for c in candidates for l in c["lines"]}
    all_stations = {s for c in candidates for s in c["stations"]}
    transfer_stations = {s for c in candidates for s in c["transfer_stations"]}

    reliability = gold.line_reliability(list(all_lines))
    crowding = gold.station_crowding(list(all_stations), hour, cat_jour)
    elevators = gold.elevator_status(list(transfer_stations)) if subscription.accessible_required else {}

    kept = []
    for c in candidates:
        if subscription.accessible_required and _elevator_out(c, elevators):
            continue
        c["reliability_pct"] = _avg([reliability.get(l.upper().strip()) for l in c["lines"]], default=85.0)
        c["crowding_est"] = _sum([crowding.get(s.upper().strip()) for s in c["stations"]])
        kept.append(c)

    if not kept:
        return []

    weights = _weights(subscription)
    dur = _normalize([c["duration_sec"] for c in kept], lower_is_better=True)
    rel = _normalize([c["reliability_pct"] for c in kept], lower_is_better=False)
    crw = _normalize([c["crowding_est"] for c in kept], lower_is_better=True)
    wlk = _normalize([c["walking_m"] for c in kept], lower_is_better=True)

    for i, c in enumerate(kept):
        c["score"] = round(
            weights["duration"] * dur[i]
            + weights["reliability"] * rel[i]
            + weights["crowding"] * crw[i]
            + weights["walking"] * wlk[i],
            4,
        )

    kept.sort(key=lambda c: c["score"], reverse=True)
    return kept[: current_app.config["TOP_ROUTES"]]


def _elevator_out(candidate, elevators):
    return any(elevators.get(s.upper().strip(), 100.0) <= 0 for s in candidate["transfer_stations"])


def _avg(values, default):
    values = [v for v in values if v is not None]
    return sum(values) / len(values) if values else default


def _sum(values):
    return sum(v for v in values if v is not None)
