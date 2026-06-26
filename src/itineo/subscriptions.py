from datetime import datetime

from flask import Blueprint, jsonify, request
from flask_jwt_extended import get_jwt_identity, jwt_required

from .extensions import db
from .models import Subscription

bp = Blueprint("subscriptions", __name__, url_prefix="/subscriptions")


def _parse(data):
    return Subscription(
        origin_station=data["origin_station"].strip(),
        destination_station=data["destination_station"].strip(),
        notify_time=datetime.strptime(data["notify_time"], "%H:%M").time(),
        accessible_required=bool(data.get("accessible_required", False)),
        crowding_sensitivity=int(data.get("crowding_sensitivity", 0)),
        minimize_walking=bool(data.get("minimize_walking", False)),
        max_transfers=int(data.get("max_transfers", 2)),
    )


@bp.get("")
@jwt_required()
def list_subscriptions():
    """List my subscriptions.
    ---
    tags: [subscriptions]
    security: [{Bearer: []}]
    responses:
      200: {description: array of subscriptions}
      401: {description: missing or invalid token}
    """
    items = Subscription.query.filter_by(user_id=int(get_jwt_identity())).all()
    return jsonify([s.to_dict() for s in items])


@bp.post("")
@jwt_required()
def create_subscription():
    """Create a subscription.
    ---
    tags: [subscriptions]
    security: [{Bearer: []}]
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          required: [origin_station, destination_station, notify_time]
          properties:
            origin_station: {type: string, example: Châtelet}
            destination_station: {type: string, example: La Défense}
            notify_time: {type: string, example: "07:30"}
            accessible_required: {type: boolean, example: false}
            crowding_sensitivity: {type: integer, example: 2}
            minimize_walking: {type: boolean, example: true}
            max_transfers: {type: integer, example: 1}
    responses:
      201: {description: subscription created}
      400: {description: invalid payload}
      401: {description: missing or invalid token}
    """
    data = request.get_json(silent=True) or {}
    try:
        subscription = _parse(data)
    except (KeyError, ValueError) as exc:
        return jsonify(error=f"invalid payload: {exc}"), 400
    subscription.user_id = int(get_jwt_identity())
    db.session.add(subscription)
    db.session.commit()
    return jsonify(subscription.to_dict()), 201


@bp.delete("/<int:subscription_id>")
@jwt_required()
def delete_subscription(subscription_id):
    """Delete one of my subscriptions.
    ---
    tags: [subscriptions]
    security: [{Bearer: []}]
    parameters:
      - in: path
        name: subscription_id
        required: true
        type: integer
    responses:
      200: {description: deleted}
      404: {description: not found}
      401: {description: missing or invalid token}
    """
    subscription = Subscription.query.filter_by(
        id=subscription_id, user_id=int(get_jwt_identity())
    ).first()
    if not subscription:
        return jsonify(error="not found"), 404
    db.session.delete(subscription)
    db.session.commit()
    return jsonify(deleted=subscription_id)
