from datetime import date

from flask import Blueprint, jsonify, request

from . import engine

bp = Blueprint("search", __name__, url_prefix="/search")


@bp.post("")
def search_itineraries():
    """Search the best itineraries between two locations.
    ---
    tags: [search]
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          required: [origin, destination]
          properties:
            origin:
              type: object
              description: "one of: {address}, {lat, lon}, {station}"
              example: {address: "29 rue de Rivoli, Paris"}
            destination:
              type: object
              example: {station: "La Défense"}
            time: {type: string, example: "08:00"}
            accessible_required: {type: boolean, example: false}
            crowding_sensitivity: {type: integer, example: 2}
            minimize_walking: {type: boolean, example: true}
            max_transfers: {type: integer, example: 1}
            top_n: {type: integer, example: 3}
    responses:
      200: {description: ranked itineraries with resolved origin/destination}
      400: {description: invalid or unresolvable locations}
    """
    data = request.get_json(silent=True) or {}
    if not data.get("origin") or not data.get("destination"):
        return jsonify(error="origin and destination are required"), 400
    try:
        params, origin_label, dest_label = engine.params_from_request(data)
    except (KeyError, ValueError) as exc:
        return jsonify(error=str(exc)), 400

    routes = engine.search(params, date.today())
    return jsonify(origin=origin_label, destination=dest_label, count=len(routes), routes=routes)
