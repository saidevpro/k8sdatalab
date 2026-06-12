from flask import current_app

from . import gold


def build_candidates(subscription):
    origin_ids, _ = gold.resolve_station(subscription.origin_station)
    dest_ids, _ = gold.resolve_station(subscription.destination_station)
    if not origin_ids or not dest_ids:
        return []

    limit = current_app.config["MAX_CANDIDATES"]
    candidates = gold.direct_routes(origin_ids, dest_ids, limit)
    if subscription.max_transfers >= 1:
        candidates += gold.transfer_routes(origin_ids, dest_ids, limit)

    return [c for c in candidates if _is_allowed(c, subscription)]


def _is_allowed(candidate, subscription):
    if candidate["transfers"] > subscription.max_transfers:
        return False
    if subscription.accessible_required and candidate["blocked_no_elevator"]:
        return False
    return True
