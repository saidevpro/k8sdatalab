from datetime import date, datetime

from flask import Blueprint, jsonify
from flask_jwt_extended import get_jwt_identity, jwt_required

from . import recommender
from .extensions import db
from .models import Notification, Subscription

bp = Blueprint("notifications", __name__, url_prefix="/notifications")


def _build_sms(subscription, routes):
    if not routes:
        return f"{subscription.origin_station} -> {subscription.destination_station}: aucun itineraire disponible aujourd'hui."
    lines = [f"{subscription.origin_station} -> {subscription.destination_station}, top {len(routes)}:"]
    for r in routes:
        lines.append(
            f"#{r['rank']} {'/'.join(r['lines'])} {r['duration_min']}min "
            f"{r['transfers']} corresp. fiab {r['reliability_pct']}%"
        )
    return "\n".join(lines)


def _send_sms(phone, message):
    print(f"[SMS] to={phone}\n{message}")


def dispatch(subscription, service_date):
    existing = Notification.query.filter_by(
        subscription_id=subscription.id, notify_date=service_date
    ).first()
    if existing:
        return existing

    routes = recommender.recommend(subscription, service_date)
    _send_sms(subscription.user.phone, _build_sms(subscription, routes))

    notification = Notification(
        subscription_id=subscription.id,
        notify_date=service_date,
        status="sent" if routes else "no_route",
        routes=routes,
    )
    db.session.add(notification)
    db.session.commit()
    return notification


def run_due_notifications():
    now = datetime.now()
    today = now.date()
    subscriptions = Subscription.query.filter_by(active=True).all()
    for subscription in subscriptions:
        if subscription.notify_time.hour == now.hour and subscription.notify_time.minute == now.minute:
            dispatch(subscription, today)


@bp.get("")
@jwt_required()
def history():
    """List my notification history.
    ---
    tags: [notifications]
    security: [{Bearer: []}]
    responses:
      200: {description: array of notifications}
      401: {description: missing or invalid token}
    """
    items = (
        Notification.query.join(Subscription)
        .filter(Subscription.user_id == int(get_jwt_identity()))
        .order_by(Notification.created_at.desc())
        .all()
    )
    return jsonify([n.to_dict() for n in items])


@bp.post("/preview/<int:subscription_id>")
@jwt_required()
def preview(subscription_id):
    """Compute the top-2 routes for a subscription now.
    ---
    tags: [notifications]
    security: [{Bearer: []}]
    parameters:
      - in: path
        name: subscription_id
        required: true
        type: integer
    responses:
      200: {description: ranked routes}
      404: {description: not found}
      401: {description: missing or invalid token}
    """
    subscription = Subscription.query.filter_by(
        id=subscription_id, user_id=int(get_jwt_identity())
    ).first()
    if not subscription:
        return jsonify(error="not found"), 404
    return jsonify(recommender.recommend(subscription, date.today()))
