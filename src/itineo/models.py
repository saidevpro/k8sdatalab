from datetime import datetime, timezone

from werkzeug.security import check_password_hash, generate_password_hash

from .extensions import db


class User(db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    first_name = db.Column(db.String(120), nullable=False)
    last_name = db.Column(db.String(120), nullable=False)
    phone = db.Column(db.String(32), nullable=False)
    created_at = db.Column(db.DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    subscriptions = db.relationship("Subscription", back_populates="user", cascade="all, delete-orphan")

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


class Subscription(db.Model):
    __tablename__ = "subscriptions"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    origin_station = db.Column(db.String(255), nullable=False)
    destination_station = db.Column(db.String(255), nullable=False)
    notify_time = db.Column(db.Time, nullable=False)
    accessible_required = db.Column(db.Boolean, nullable=False, default=False)
    crowding_sensitivity = db.Column(db.Integer, nullable=False, default=0)
    minimize_walking = db.Column(db.Boolean, nullable=False, default=False)
    max_transfers = db.Column(db.Integer, nullable=False, default=2)
    active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    user = db.relationship("User", back_populates="subscriptions")
    notifications = db.relationship("Notification", back_populates="subscription", cascade="all, delete-orphan")

    def to_dict(self):
        return {
            "id": self.id,
            "origin_station": self.origin_station,
            "destination_station": self.destination_station,
            "notify_time": self.notify_time.strftime("%H:%M"),
            "accessible_required": self.accessible_required,
            "crowding_sensitivity": self.crowding_sensitivity,
            "minimize_walking": self.minimize_walking,
            "max_transfers": self.max_transfers,
            "active": self.active,
        }


class Notification(db.Model):
    __tablename__ = "notifications"

    id = db.Column(db.Integer, primary_key=True)
    subscription_id = db.Column(db.Integer, db.ForeignKey("subscriptions.id"), nullable=False, index=True)
    notify_date = db.Column(db.Date, nullable=False)
    status = db.Column(db.String(32), nullable=False, default="sent")
    routes = db.Column(db.JSON, nullable=False, default=list)
    created_at = db.Column(db.DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    subscription = db.relationship("Subscription", back_populates="notifications")

    __table_args__ = (db.UniqueConstraint("subscription_id", "notify_date", name="uq_subscription_day"),)

    def to_dict(self):
        return {
            "id": self.id,
            "subscription_id": self.subscription_id,
            "notify_date": self.notify_date.isoformat(),
            "status": self.status,
            "routes": self.routes,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
