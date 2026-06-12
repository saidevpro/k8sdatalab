from . import routing, scoring


def recommend(subscription, service_date):
    candidates = routing.build_candidates(subscription)
    ranked = scoring.rank_routes(candidates, subscription, service_date)
    return [_format(c, rank) for rank, c in enumerate(ranked, start=1)]


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
