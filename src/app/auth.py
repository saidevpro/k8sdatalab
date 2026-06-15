from flask import Blueprint, jsonify, request
from flask_jwt_extended import create_access_token

from .extensions import db
from .models import User

bp = Blueprint("auth", __name__, url_prefix="/auth")


@bp.post("/signup")
def signup():
    """Create a user account.
    ---
    tags: [auth]
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          required: [email, password, first_name, last_name, phone]
          properties:
            email: {type: string, example: test@itineo.fr}
            password: {type: string, example: secret123}
            first_name: {type: string, example: Said}
            last_name: {type: string, example: Sow}
            phone: {type: string, example: "+33612345678"}
    responses:
      201: {description: account created}
      400: {description: missing required fields}
      409: {description: email already registered}
    """
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    first_name = (data.get("first_name") or "").strip()
    last_name = (data.get("last_name") or "").strip()
    phone = (data.get("phone") or "").strip()
    if not email or not password or not first_name or not last_name or not phone:
        return jsonify(error="email, password, first_name, last_name and phone are required"), 400
    if User.query.filter_by(email=email).first():
        return jsonify(error="email already registered"), 409

    user = User(email=email, first_name=first_name, last_name=last_name, phone=phone)
    user.set_password(password)
    db.session.add(user)
    db.session.commit()
    return jsonify(id=user.id, email=user.email), 201


@bp.post("/login")
def login():
    """Authenticate and get a JWT access token.
    ---
    tags: [auth]
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          required: [email, password]
          properties:
            email: {type: string, example: test@itineo.fr}
            password: {type: string, example: secret123}
    responses:
      200: {description: returns access_token}
      401: {description: invalid credentials}
    """
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    user = User.query.filter_by(email=email).first()
    if not user or not user.check_password(password):
        return jsonify(error="invalid credentials"), 401
    return jsonify(access_token=create_access_token(identity=str(user.id)))
