from flask import current_app

from . import gold


def build_candidates(origin_ids, dest_ids, max_transfers, accessible_required, dep_from, dep_to):
    if not origin_ids or not dest_ids:
        return []

    limit = current_app.config["MAX_CANDIDATES"]
    candidates = gold.direct_routes(origin_ids, dest_ids, limit, dep_from, dep_to)
    if max_transfers >= 1:
        candidates += gold.transfer_routes(origin_ids, dest_ids, limit, dep_from, dep_to)

    allowed = [c for c in candidates if _is_allowed(c, max_transfers, accessible_required)]
    valid = [c for c in allowed if c["duration_sec"] >= 60]
    return _dedup(valid)


def _dedup(candidates):
    seen = {}
    for c in candidates:
        key = (c["board_stop_id"], c["alight_stop_id"], tuple(c["lines"]))
        if key not in seen or c["duration_sec"] < seen[key]["duration_sec"]:
            seen[key] = c
    return list(seen.values())


def _is_allowed(candidate, max_transfers, accessible_required):
    if candidate["transfers"] > max_transfers:
        return False
    if accessible_required and candidate["blocked_no_elevator"]:
        return False
    return True
