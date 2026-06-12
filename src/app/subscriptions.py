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
    items = Subscription.query.filter_by(user_id=int(get_jwt_identity())).all()
    return jsonify([s.to_dict() for s in items])


@bp.post("")
@jwt_required()
def create_subscription():
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
    subscription = Subscription.query.filter_by(
        id=subscription_id, user_id=int(get_jwt_identity())
    ).first()
    if not subscription:
        return jsonify(error="not found"), 404
    db.session.delete(subscription)
    db.session.commit()
    return jsonify(deleted=subscription_id)
