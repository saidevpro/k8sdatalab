from flask import Blueprint, current_app, jsonify, request
from requests import RequestException
from trino.exceptions import Error as TrinoError

from . import gold

bp = Blueprint("accessibility", __name__, url_prefix="/accessibility")

MAX_PAGE_SIZE = 500
DEFAULT_PAGE_SIZE = 100


def _optional_bool(name):
    raw = request.args.get(name)
    if raw is None:
        return None
    normalized = raw.strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise ValueError(f"{name} must be true or false")


def _pagination():
    try:
        limit = int(request.args.get("limit", DEFAULT_PAGE_SIZE))
        offset = int(request.args.get("offset", 0))
    except ValueError as exc:
        raise ValueError("limit and offset must be integers") from exc
    if not 1 <= limit <= MAX_PAGE_SIZE:
        raise ValueError(f"limit must be between 1 and {MAX_PAGE_SIZE}")
    if offset < 0:
        raise ValueError("offset must be greater than or equal to 0")
    return limit, offset


def _search_term():
    return (request.args.get("q") or "").strip() or None


def _collection_response(items, limit, offset):
    return jsonify(count=len(items), limit=limit, offset=offset, items=items)


@bp.errorhandler(TrinoError)
@bp.errorhandler(RequestException)
def gold_layer_unavailable(error):
    current_app.logger.error("Gold layer request failed", exc_info=error)
    return jsonify(error="gold layer unavailable"), 503


@bp.get("/summary")
def summary():
    """Return network-wide accessibility indicators.
    ---
    tags: [accessibility]
    responses:
      200:
        description: station accessibility and elevator summary from the gold layer
    """
    return jsonify(gold.accessibility_summary())


@bp.get("/stations")
def stations():
    """List station accessibility data.
    ---
    tags: [accessibility]
    parameters:
      - {in: query, name: q, type: string, description: partial station name}
      - {in: query, name: accessible, type: boolean}
      - {in: query, name: limit, type: integer, default: 100, maximum: 500}
      - {in: query, name: offset, type: integer, default: 0}
    responses:
      200: {description: paginated station accessibility data}
      400: {description: invalid query parameter}
    """
    try:
        limit, offset = _pagination()
        accessible = _optional_bool("accessible")
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    items = gold.accessibility_stations(_search_term(), accessible, limit, offset)
    return _collection_response(items, limit, offset)


@bp.get("/stations/<int:station_id>")
def station(station_id):
    """Get accessibility details for one parent station.
    ---
    tags: [accessibility]
    parameters:
      - {in: path, name: station_id, type: integer, required: true}
    responses:
      200: {description: station accessibility details}
      404: {description: station not found}
    """
    items = gold.accessibility_stations(limit=1, offset=0, station_id=station_id)
    if not items:
        return jsonify(error="station not found"), 404
    return jsonify(items[0])


@bp.get("/elevators")
def elevators():
    """List current elevator availability.
    ---
    tags: [accessibility]
    parameters:
      - {in: query, name: q, type: string, description: partial station name}
      - {in: query, name: available, type: boolean, description: at least one elevator available}
      - {in: query, name: limit, type: integer, default: 100, maximum: 500}
      - {in: query, name: offset, type: integer, default: 0}
    responses:
      200: {description: paginated real-time elevator availability}
      400: {description: invalid query parameter}
    """
    try:
        limit, offset = _pagination()
        available = _optional_bool("available")
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    items = gold.elevator_availability(_search_term(), available, limit, offset)
    return _collection_response(items, limit, offset)


@bp.get("/elevators/reliability")
def elevator_reliability():
    """List 30/90-day elevator reliability indicators.
    ---
    tags: [accessibility]
    parameters:
      - {in: query, name: q, type: string, description: partial station name}
      - {in: query, name: limit, type: integer, default: 100, maximum: 500}
      - {in: query, name: offset, type: integer, default: 0}
    responses:
      200: {description: paginated elevator reliability indicators}
      400: {description: invalid query parameter}
    """
    try:
        limit, offset = _pagination()
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    items = gold.elevator_reliability(_search_term(), limit, offset)
    return _collection_response(items, limit, offset)


@bp.get("/elevators/outages")
def elevator_outages():
    """List historical or ongoing elevator outages.
    ---
    tags: [accessibility]
    parameters:
      - {in: query, name: q, type: string, description: partial station name}
      - {in: query, name: ongoing, type: boolean}
      - {in: query, name: since_days, type: integer, minimum: 1, maximum: 3650}
      - {in: query, name: limit, type: integer, default: 100, maximum: 500}
      - {in: query, name: offset, type: integer, default: 0}
    responses:
      200: {description: paginated elevator outage events}
      400: {description: invalid query parameter}
    """
    try:
        limit, offset = _pagination()
        ongoing = _optional_bool("ongoing")
        raw_since_days = request.args.get("since_days")
        since_days = int(raw_since_days) if raw_since_days is not None else None
        if since_days is not None and not 1 <= since_days <= 3650:
            raise ValueError("since_days must be between 1 and 3650")
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    items = gold.elevator_outages(
        _search_term(), ongoing, since_days, limit, offset
    )
    return _collection_response(items, limit, offset)


@bp.get("/transfers")
def transfers():
    """List transfer accessibility data.
    ---
    tags: [accessibility]
    parameters:
      - {in: query, name: q, type: string, description: partial endpoint station name}
      - {in: query, name: accessible, type: boolean}
      - {in: query, name: max_distance_m, type: number, minimum: 0}
      - {in: query, name: limit, type: integer, default: 100, maximum: 500}
      - {in: query, name: offset, type: integer, default: 0}
    responses:
      200: {description: paginated transfer accessibility data}
      400: {description: invalid query parameter}
    """
    try:
        limit, offset = _pagination()
        accessible = _optional_bool("accessible")
        raw_distance = request.args.get("max_distance_m")
        max_distance_m = float(raw_distance) if raw_distance is not None else None
        if max_distance_m is not None and max_distance_m < 0:
            raise ValueError("max_distance_m must be greater than or equal to 0")
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    items = gold.accessibility_transfers(
        _search_term(), accessible, max_distance_m, limit, offset
    )
    return _collection_response(items, limit, offset)
