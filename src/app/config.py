import os


class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "change-me")
    JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", SECRET_KEY)

    SQLALCHEMY_DATABASE_URI = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg2://idfm:idfm@localhost:5432/idfm",
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    TRINO_HOST = os.getenv("TRINO_HOST", "trino.data-platform.svc.cluster.local")
    TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
    TRINO_USER = os.getenv("TRINO_USER", "idfm-app")
    TRINO_PASSWORD = os.getenv("TRINO_PASSWORD")
    TRINO_HTTP_SCHEME = os.getenv("TRINO_HTTP_SCHEME", "http")
    TRINO_CATALOG = os.getenv("TRINO_CATALOG", "dev")
    GOLD_SCHEMA = os.getenv("GOLD_SCHEMA", "gold")
    SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")

    MAX_CANDIDATES = int(os.getenv("MAX_CANDIDATES", "40"))
    TOP_ROUTES = int(os.getenv("TOP_ROUTES", "2"))
    DEFAULT_TRANSFER_TIME_SEC = int(os.getenv("DEFAULT_TRANSFER_TIME_SEC", "180"))

    WEIGHT_DURATION = float(os.getenv("WEIGHT_DURATION", "0.40"))
    WEIGHT_RELIABILITY = float(os.getenv("WEIGHT_RELIABILITY", "0.25"))
    WEIGHT_CROWDING = float(os.getenv("WEIGHT_CROWDING", "0.20"))
    WEIGHT_WALKING = float(os.getenv("WEIGHT_WALKING", "0.15"))

    ENABLE_SCHEDULER = os.getenv("ENABLE_SCHEDULER", "true").lower() == "true"
