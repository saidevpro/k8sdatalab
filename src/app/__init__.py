from flasgger import Swagger
from flask import Flask, Response, jsonify, request

from .config import Config
from .extensions import db, jwt

SWAGGER_TEMPLATE = {
    "swagger": "2.0",
    "info": {
        "title": "IDFM Route Notification API",
        "description": "Subscriptions and daily best-route SMS notifications.",
        "version": "1.0.0",
    },
    "securityDefinitions": {
        "Bearer": {
            "type": "apiKey",
            "name": "Authorization",
            "in": "header",
            "description": "JWT access token as: Bearer <token>",
        }
    },
}


def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    db.init_app(app)
    jwt.init_app(app)
    Swagger(app, template=SWAGGER_TEMPLATE)

    @app.before_request
    def protect_swagger():
        if not (request.path.startswith("/apidocs") or request.path.startswith("/apispec")):
            return None
        user = app.config.get("SWAGGER_USER")
        password = app.config.get("SWAGGER_PASSWORD")
        if not user or not password:
            return None
        auth = request.authorization
        if not auth or auth.username != user or auth.password != password:
            return Response(
                "Authentication required", 401, {"WWW-Authenticate": 'Basic realm="Swagger"'}
            )
        return None

    from . import auth, notifications, search, subscriptions

    app.register_blueprint(auth.bp)
    app.register_blueprint(subscriptions.bp)
    app.register_blueprint(notifications.bp)
    app.register_blueprint(search.bp)

    @app.get("/health")
    def health():
        """Liveness probe.
        ---
        tags: [system]
        responses:
          200: {description: ok}
        """
        return jsonify(status="ok")

    with app.app_context():
        db.create_all()

    if app.config["ENABLE_SCHEDULER"]:
        from .scheduler import start_scheduler

        start_scheduler(app)

    return app
