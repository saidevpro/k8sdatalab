from datetime import datetime, timezone

from flask import Blueprint, current_app, jsonify, request
from flask_jwt_extended import current_user, jwt_required

from .extensions import db
from .models import User


bp = Blueprint("privacy", __name__, url_prefix="/privacy")


def _isoformat(value):
    return value.isoformat() if value is not None else None


def _account_data(user):
    return {
        "id": user.id,
        "email": user.email,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "phone": user.phone,
        "created_at": _isoformat(user.created_at),
    }


def _subscription_data(subscription):
    data = subscription.to_dict()
    data["created_at"] = _isoformat(subscription.created_at)
    data["notifications"] = [
        notification.to_dict()
        for notification in sorted(subscription.notifications, key=lambda item: item.id)
    ]
    return data


@bp.get("")
def privacy_notice():
    """Return the API privacy notice and data-subject rights.
    ---
    tags: [privacy]
    responses:
      200: {description: privacy notice and available GDPR rights endpoints}
    """
    contact_email = current_app.config["PRIVACY_CONTACT_EMAIL"]
    return jsonify(
        policy_version=current_app.config["PRIVACY_POLICY_VERSION"],
        controller={
            "name": current_app.config["PRIVACY_CONTROLLER_NAME"],
            "contact_email": contact_email,
            "contact_configured": bool(contact_email),
        },
        purposes=[
            {
                "purpose": "account and subscription management",
                "legal_basis": "performance of a contract (GDPR Article 6(1)(b))",
            },
            {
                "purpose": "journey recommendation and notification delivery",
                "legal_basis": "performance of a contract (GDPR Article 6(1)(b))",
            },
        ],
        personal_data_categories=[
            "identity and contact details",
            "journey subscriptions and accessibility preferences",
            "notification history and recommended routes",
        ],
        data_source="provided directly by the account holder",
        required_data={
            "account": ["email", "password", "first_name", "last_name", "phone"],
            "consequence_if_not_provided": "the account and notification service cannot be created",
        },
        recipients=[
            "authorised application operators",
            "a notification delivery provider when one is configured",
        ],
        international_transfers=(
            "none configured by the application code; the deployment operator must document "
            "its infrastructure and providers"
        ),
        retention={
            "active_database": (
                "account data is kept while the account is active, then erased on deletion"
            ),
            "backups": "expire according to the deployment backup-retention policy",
            "operational_logs": "expire according to the platform log-retention policy",
        },
        rights={
            "access_and_portability": "GET /privacy/me",
            "rectification": "PATCH /privacy/me",
            "erasure": "DELETE /privacy/me",
            "restriction_and_objection": "contact the data controller",
            "contact": contact_email,
            "complaint": "https://www.cnil.fr/fr/plaintes",
        },
        automated_decision_making={
            "description": "routes are automatically ranked from mobility preferences",
            "legal_or_similarly_significant_effect": False,
        },
        configuration_warning=(
            None
            if contact_email
            else "PRIVACY_CONTACT_EMAIL must be configured before production use"
        ),
    )


@bp.get("/me")
@jwt_required()
def export_my_data():
    """Export all personal data associated with the authenticated account.
    ---
    tags: [privacy]
    security: [{Bearer: []}]
    responses:
      200: {description: portable JSON export of account data}
      401: {description: missing, invalid or orphaned token}
    """
    payload = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "format": "application/json",
        "account": _account_data(current_user),
        "subscriptions": [
            _subscription_data(subscription)
            for subscription in sorted(current_user.subscriptions, key=lambda item: item.id)
        ],
    }
    response = jsonify(payload)
    response.headers["Content-Disposition"] = (
        'attachment; filename="itineo-personal-data.json"'
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return response


@bp.patch("/me")
@jwt_required()
def rectify_my_data():
    """Rectify identity and contact information.
    ---
    tags: [privacy]
    security: [{Bearer: []}]
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          required: [password]
          properties:
            password: {type: string, description: current password confirmation}
            email: {type: string}
            first_name: {type: string}
            last_name: {type: string}
            phone: {type: string}
    responses:
      200: {description: corrected account data}
      400: {description: invalid or empty update}
      401: {description: invalid password or token}
      409: {description: email already registered}
    """
    data = request.get_json(silent=True) or {}
    if not current_user.check_password(data.get("password") or ""):
        return jsonify(error="invalid credentials"), 401

    updates = {}
    for field in ("email", "first_name", "last_name", "phone"):
        if field not in data:
            continue
        value = (data.get(field) or "").strip()
        if not value:
            return jsonify(error=f"{field} must not be empty"), 400
        updates[field] = value.lower() if field == "email" else value

    if not updates:
        return jsonify(error="at least one account field must be provided"), 400
    if "email" in updates:
        existing = User.query.filter(
            User.email == updates["email"], User.id != current_user.id
        ).first()
        if existing:
            return jsonify(error="email already registered"), 409

    for field, value in updates.items():
        setattr(current_user, field, value)
    db.session.commit()
    return jsonify(_account_data(current_user))


@bp.delete("/me")
@jwt_required()
def erase_my_data():
    """Erase the authenticated account and its dependent application data.
    ---
    tags: [privacy]
    security: [{Bearer: []}]
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          required: [password, confirm]
          properties:
            password: {type: string, description: current password confirmation}
            confirm: {type: string, enum: [DELETE]}
    responses:
      200: {description: account, subscriptions and notifications erased}
      400: {description: missing DELETE confirmation}
      401: {description: invalid password or token}
    """
    data = request.get_json(silent=True) or {}
    if data.get("confirm") != "DELETE":
        return jsonify(error='confirm must be exactly "DELETE"'), 400
    if not current_user.check_password(data.get("password") or ""):
        return jsonify(error="invalid credentials"), 401

    user_id = current_user.id
    subscriptions = list(current_user.subscriptions)
    deleted_subscriptions = len(subscriptions)
    deleted_notifications = sum(len(item.notifications) for item in subscriptions)

    db.session.delete(current_user)
    db.session.commit()
    return jsonify(
        deleted=True,
        user_id=user_id,
        deleted_resources={
            "accounts": 1,
            "subscriptions": deleted_subscriptions,
            "notifications": deleted_notifications,
        },
    )
