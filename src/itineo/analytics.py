from datetime import date

from flask import Blueprint, current_app, jsonify, request
from requests import RequestException
from trino.exceptions import Error as TrinoError

from . import gold


bp = Blueprint("analytics", __name__)

DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 500


def _pagination(default=DEFAULT_PAGE_SIZE, maximum=MAX_PAGE_SIZE):
    try:
        limit = int(request.args.get("limit", default))
        offset = int(request.args.get("offset", 0))
    except ValueError as exc:
        raise ValueError("limit and offset must be integers") from exc
    if not 1 <= limit <= maximum:
        raise ValueError(f"limit must be between 1 and {maximum}")
    if offset < 0:
        raise ValueError("offset must be greater than or equal to 0")
    return limit, offset


def _optional_integer(name, default=None, minimum=None, maximum=None):
    raw = request.args.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be greater than or equal to {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be less than or equal to {maximum}")
    return value


def _optional_date(name):
    raw = (request.args.get(name) or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError as exc:
        raise ValueError(f"{name} must use YYYY-MM-DD format") from exc


def _date_range():
    date_from = _optional_date("date_from")
    date_to = _optional_date("date_to")
    if date_from and date_to and date_from > date_to:
        raise ValueError("date_from must be before or equal to date_to")
    return date_from, date_to


def _term(name="q"):
    return (request.args.get(name) or "").strip() or None


def _collection(items, limit, offset):
    return jsonify(count=len(items), limit=limit, offset=offset, items=items)


def _bad_request(error):
    return jsonify(error=str(error)), 400


@bp.errorhandler(TrinoError)
@bp.errorhandler(RequestException)
def gold_layer_unavailable(error):
    current_app.logger.error("Gold layer request failed", exc_info=error)
    return jsonify(error="gold layer unavailable"), 503


@bp.get("/crowding/summary")
def crowding_summary():
    """Get network-wide crowding indicators.
    ---
    tags: [crowding]
    parameters:
      - {in: query, name: date_from, type: string, format: date}
      - {in: query, name: date_to, type: string, format: date}
    responses:
      200: {description: aggregate validation indicators}
      400: {description: invalid date range}
    """
    try:
        date_from, date_to = _date_range()
    except ValueError as exc:
        return _bad_request(exc)
    return jsonify(gold.crowding_summary(date_from, date_to))


@bp.get("/crowding/stations")
def crowding_stations():
    """Rank stations by validation volumes.
    ---
    tags: [crowding]
    parameters:
      - {in: query, name: q, type: string}
      - {in: query, name: hour, type: integer, minimum: 0, maximum: 23}
      - {in: query, name: cat_jour, type: string}
      - {in: query, name: date_from, type: string, format: date}
      - {in: query, name: date_to, type: string, format: date}
      - {in: query, name: limit, type: integer, default: 100, maximum: 500}
      - {in: query, name: offset, type: integer, default: 0}
    responses:
      200: {description: stations ranked by estimated crowding}
      400: {description: invalid query parameter}
    """
    try:
        limit, offset = _pagination()
        hour = _optional_integer("hour", minimum=0, maximum=23)
        date_from, date_to = _date_range()
    except ValueError as exc:
        return _bad_request(exc)
    items = gold.crowding_stations(
        _term(), hour, _term("cat_jour"), date_from, date_to, limit, offset
    )
    return _collection(items, limit, offset)


@bp.get("/crowding/timeseries")
def crowding_timeseries():
    """Get daily validation totals.
    ---
    tags: [crowding]
    parameters:
      - {in: query, name: q, type: string, description: partial station name}
      - {in: query, name: date_from, type: string, format: date}
      - {in: query, name: date_to, type: string, format: date}
      - {in: query, name: limit, type: integer, default: 100, maximum: 500}
      - {in: query, name: offset, type: integer, default: 0}
    responses:
      200: {description: daily validation time series}
      400: {description: invalid query parameter}
    """
    try:
        limit, offset = _pagination()
        date_from, date_to = _date_range()
    except ValueError as exc:
        return _bad_request(exc)
    items = gold.crowding_timeseries(_term(), date_from, date_to, limit, offset)
    return _collection(items, limit, offset)


@bp.get("/crowding/ticket-categories")
def crowding_ticket_categories():
    """Get validation distribution by ticket category.
    ---
    tags: [crowding]
    parameters:
      - {in: query, name: q, type: string, description: partial station name}
      - {in: query, name: date_from, type: string, format: date}
      - {in: query, name: date_to, type: string, format: date}
      - {in: query, name: limit, type: integer, default: 100, maximum: 500}
      - {in: query, name: offset, type: integer, default: 0}
    responses:
      200: {description: validation counts and percentages by ticket category}
      400: {description: invalid query parameter}
    """
    try:
        limit, offset = _pagination()
        date_from, date_to = _date_range()
    except ValueError as exc:
        return _bad_request(exc)
    items = gold.crowding_ticket_categories(
        _term(), date_from, date_to, limit, offset
    )
    return _collection(items, limit, offset)


@bp.get("/delays/summary")
def delays_summary():
    """Get weighted punctuality indicators.
    ---
    tags: [delays]
    parameters:
      - {in: query, name: line, type: string}
    responses:
      200: {description: network or filtered-line punctuality summary}
    """
    return jsonify(gold.delays_summary(_term("line")))


@bp.get("/delays/lines")
def delays_lines():
    """Rank lines by punctuality.
    ---
    tags: [delays]
    parameters:
      - {in: query, name: line, type: string}
      - {in: query, name: limit, type: integer, default: 100, maximum: 500}
      - {in: query, name: offset, type: integer, default: 0}
    responses:
      200: {description: passage-weighted line punctuality}
      400: {description: invalid pagination}
    """
    try:
        limit, offset = _pagination()
    except ValueError as exc:
        return _bad_request(exc)
    items = gold.delays_by_line(_term("line"), limit, offset)
    return _collection(items, limit, offset)


@bp.get("/delays/recent")
def delays_recent():
    """Get the most recent delayed passages.
    ---
    tags: [delays]
    parameters:
      - {in: query, name: line, type: string}
      - {in: query, name: min_delay_sec, type: integer, default: 60, minimum: 0}
      - {in: query, name: since_hours, type: integer, minimum: 1, maximum: 744}
      - {in: query, name: limit, type: integer, default: 100, maximum: 500}
      - {in: query, name: offset, type: integer, default: 0}
    responses:
      200: {description: latest passages exceeding the delay threshold}
      400: {description: invalid query parameter}
    """
    try:
        limit, offset = _pagination()
        min_delay_sec = _optional_integer(
            "min_delay_sec", default=60, minimum=0, maximum=86400
        )
        since_hours = _optional_integer("since_hours", minimum=1, maximum=744)
    except ValueError as exc:
        return _bad_request(exc)
    items = gold.recent_delays(
        _term("line"), min_delay_sec, since_hours, limit, offset
    )
    return _collection(items, limit, offset)


@bp.get("/disruptions/active")
def disruptions_active():
    """Get active disruption messages.
    ---
    tags: [disruptions]
    parameters:
      - {in: query, name: q, type: string}
      - {in: query, name: line, type: string}
      - {in: query, name: severity, type: string}
      - {in: query, name: limit, type: integer, default: 100, maximum: 500}
      - {in: query, name: offset, type: integer, default: 0}
    responses:
      200: {description: active disruptions and impacted lines}
      400: {description: invalid pagination}
    """
    try:
        limit, offset = _pagination()
    except ValueError as exc:
        return _bad_request(exc)
    items = gold.active_disruptions(
        _term(), _term("line"), _term("severity"), limit, offset
    )
    return _collection(items, limit, offset)


@bp.get("/disruptions/history")
def disruptions_history():
    """Get daily disruption counts by line.
    ---
    tags: [disruptions]
    parameters:
      - {in: query, name: line, type: string}
      - {in: query, name: date_from, type: string, format: date}
      - {in: query, name: date_to, type: string, format: date}
      - {in: query, name: limit, type: integer, default: 100, maximum: 500}
      - {in: query, name: offset, type: integer, default: 0}
    responses:
      200: {description: daily disruption history}
      400: {description: invalid query parameter}
    """
    try:
        limit, offset = _pagination()
        date_from, date_to = _date_range()
    except ValueError as exc:
        return _bad_request(exc)
    items = gold.disruptions_history(
        _term("line"), date_from, date_to, limit, offset
    )
    return _collection(items, limit, offset)


@bp.get("/network/lines")
def network_lines():
    """Get the GTFS line catalogue.
    ---
    tags: [network]
    parameters:
      - {in: query, name: q, type: string}
      - {in: query, name: limit, type: integer, default: 100, maximum: 500}
      - {in: query, name: offset, type: integer, default: 0}
    responses:
      200: {description: lines with mode and station coverage}
      400: {description: invalid pagination}
    """
    try:
        limit, offset = _pagination()
    except ValueError as exc:
        return _bad_request(exc)
    items = gold.network_lines(_term(), limit, offset)
    return _collection(items, limit, offset)


@bp.get("/network/lines/<path:route_id>/stations")
def network_line_stations(route_id):
    """Get stations served by one GTFS route.
    ---
    tags: [network]
    parameters:
      - {in: path, name: route_id, type: string, required: true}
      - {in: query, name: limit, type: integer, default: 500, maximum: 500}
      - {in: query, name: offset, type: integer, default: 0}
    responses:
      200: {description: unique stations served by the route}
      400: {description: invalid pagination}
    """
    try:
        limit, offset = _pagination(default=500)
    except ValueError as exc:
        return _bad_request(exc)
    items = gold.network_line_stations(route_id, limit, offset)
    return _collection(items, limit, offset)
