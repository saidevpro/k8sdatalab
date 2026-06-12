from flask import Flask, jsonify

from .config import Config
from .extensions import db, jwt


def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    db.init_app(app)
    jwt.init_app(app)

    from . import auth, notifications, subscriptions

    app.register_blueprint(auth.bp)
    app.register_blueprint(subscriptions.bp)
    app.register_blueprint(notifications.bp)

    @app.get("/health")
    def health():
        return jsonify(status="ok")

    with app.app_context():
        db.create_all()

    if app.config["ENABLE_SCHEDULER"]:
        from .scheduler import start_scheduler

        start_scheduler(app)

    return app
