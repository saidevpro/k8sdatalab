-- IDFM Route Notification App - PostgreSQL schema
-- Run the role/database section as a superuser (e.g. postgres), then the rest
-- connected to the application database:
--   psql -U postgres -f db.sql

-- =====================================================================
-- Role & database (run once, as superuser)
-- =====================================================================
CREATE ROLE idfm WITH LOGIN PASSWORD 'idfm';
CREATE DATABASE idfm OWNER idfm;

\connect idfm

-- =====================================================================
-- Tables
-- =====================================================================
CREATE TABLE users (
    id            SERIAL PRIMARY KEY,
    email         VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    first_name    VARCHAR(120) NOT NULL,
    last_name     VARCHAR(120) NOT NULL,
    phone         VARCHAR(32) NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE subscriptions (
    id                   SERIAL PRIMARY KEY,
    user_id              INTEGER NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    origin_station       VARCHAR(255) NOT NULL,
    destination_station  VARCHAR(255) NOT NULL,
    notify_time          TIME NOT NULL,
    accessible_required  BOOLEAN NOT NULL DEFAULT FALSE,
    crowding_sensitivity INTEGER NOT NULL DEFAULT 0,
    minimize_walking     BOOLEAN NOT NULL DEFAULT FALSE,
    max_transfers        INTEGER NOT NULL DEFAULT 2,
    active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_crowding_sensitivity CHECK (crowding_sensitivity BETWEEN 0 AND 3),
    CONSTRAINT ck_max_transfers CHECK (max_transfers >= 0)
);

CREATE TABLE notifications (
    id              SERIAL PRIMARY KEY,
    subscription_id INTEGER NOT NULL REFERENCES subscriptions (id) ON DELETE CASCADE,
    notify_date     DATE NOT NULL,
    status          VARCHAR(32) NOT NULL DEFAULT 'sent',
    routes          JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_subscription_day UNIQUE (subscription_id, notify_date)
);

-- =====================================================================
-- Indexes
-- =====================================================================
CREATE INDEX ix_subscriptions_user_id ON subscriptions (user_id);
CREATE INDEX ix_notifications_subscription_id ON notifications (subscription_id);

-- =====================================================================
-- Grants
-- =====================================================================
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO idfm;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO idfm;
