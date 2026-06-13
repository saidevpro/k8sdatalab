-- IDFM Route Notification App - PostgreSQL schema
-- The "itineo" database, the "admin" role and its grants already exist.
-- Run this connected to the application database as admin:
--   psql -U admin -d itineo -f db.sql

\connect itineo

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
