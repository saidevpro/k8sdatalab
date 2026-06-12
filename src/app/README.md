# IDFM Route Notification App

Flask service implementing the capstone functional goal: users subscribe to an
origin/destination with a daily notification time and preferences; every day at
that time the service computes the **2 best routes** ranked by a score combining
trip duration, predicted reliability, estimated crowding, transfer walking
distance, and accessibility constraints.

## Architecture

- **PostgreSQL** (SQLAlchemy) stores operational data: `users`, `subscriptions`,
  `notifications`.
- **Trino** queries the **gold** lakehouse tables (Nessie/Iceberg) for routing and
  scoring signals — no analytical data is copied into Postgres.
- **APScheduler** triggers the daily dispatch; notifications are stored in the
  `notifications` table (stub delivery).

| Module | Responsibility |
|---|---|
| `models.py` | Postgres models |
| `auth.py` / `subscriptions.py` | JWT auth, subscription CRUD |
| `gold.py` | Trino client + gold queries |
| `routing.py` | candidate routes (direct + 1 transfer) from gold |
| `scoring.py` | 5-criteria score, top-2 selection |
| `recommender.py` | orchestration for one subscription |
| `notifications.py` / `scheduler.py` | daily dispatch + history |

## Scoring

For each candidate route the score (higher = better) is a weighted, min-max
normalized blend over the candidate set:

```
score = w_duration   * (shorter trip)
      + w_reliability * (avg pct_on_time of the lines used)
      + w_crowding    * (lower estimated validations at stops, at the travel hour)
      + w_walking     * (lower cumulative transfer walking distance)
```

Preferences adjust the weights: `crowding_sensitivity` scales `w_crowding`,
`minimize_walking` doubles `w_walking`. Accessibility is a **hard filter**: routes
with a stairs-only transfer, or a transfer station whose elevators are out of
service (real-time `elevators_availability`), are excluded when
`accessible_required` is set. `max_transfers` caps the number of legs.

## Endpoints

| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/auth/signup` | — | create account |
| POST | `/auth/login` | — | get JWT |
| GET/POST | `/subscriptions` | JWT | list / create subscription |
| DELETE | `/subscriptions/<id>` | JWT | delete subscription |
| POST | `/notifications/preview/<id>` | JWT | compute top-2 routes now |
| GET | `/notifications` | JWT | notification history |
| GET | `/health` | — | liveness |

Subscription payload:

```json
{
  "origin_station": "Châtelet",
  "destination_station": "La Défense",
  "notify_time": "07:30",
  "accessible_required": true,
  "crowding_sensitivity": 2,
  "minimize_walking": true,
  "max_transfers": 1
}
```

## Run locally

```bash
pip install -r requirements.txt
cp .env.example .env   # adjust DATABASE_URL / TRINO_*
export $(grep -v '^#' .env | xargs)
python -m app.wsgi     # from src/
```

## Notes / known simplifications

- Routing covers **direct and single-transfer** itineraries built from the GTFS
  gold tables, not a full multi-leg planner (OTP/RAPTOR can be plugged into
  `routing.py` later).
- Reliability uses historical `delays_by_stop.pct_on_time` at line level; the
  MLflow delay model can replace it in `scoring.py`.
- Crowding / accessibility / elevator joins reconcile referentials by **station
  name** (the same `upper(trim(...))` key used by the gold jobs).
- `TRINO_CATALOG=dev` matches the Nessie branch the Spark/ML jobs write to; switch
  to `lakehouse` once gold is promoted to `main`.
- Run a **single** scheduler instance; the `(subscription_id, notify_date)` unique
  constraint keeps dispatch idempotent if it ever runs concurrently.
```
