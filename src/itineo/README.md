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
| `accessibility.py` | public read-only Gold accessibility endpoints |
| `analytics.py` | public crowding, delay, disruption and network endpoints |
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
| POST | `/search` | — | search top-N itineraries from a point/address/station |
| GET | `/accessibility/summary` | — | network-wide station accessibility indicators |
| GET | `/accessibility/stations` | — | paginated station accessibility data |
| GET | `/accessibility/stations/<station_id>` | — | accessibility details for one station |
| GET | `/accessibility/elevators` | — | current elevator availability by station |
| GET | `/accessibility/elevators/reliability` | — | 30/90-day elevator reliability |
| GET | `/accessibility/elevators/outages` | — | ongoing and historical elevator outages |
| GET | `/accessibility/transfers` | — | accessible-transfer facts and walking constraints |
| GET | `/crowding/summary` | — | network-wide validation indicators |
| GET | `/crowding/stations` | — | stations ranked by estimated crowding |
| GET | `/crowding/timeseries` | — | daily validation time series |
| GET | `/crowding/ticket-categories` | — | validation distribution by ticket category |
| GET | `/delays/summary` | — | weighted punctuality summary |
| GET | `/delays/lines` | — | line punctuality ranking |
| GET | `/delays/recent` | — | most recently ingested delayed passages |
| GET | `/disruptions/active` | — | active disruption messages and impacted lines |
| GET | `/disruptions/history` | — | daily disruption counts by line |
| GET | `/network/lines` | — | GTFS line catalogue and coverage |
| GET | `/network/lines/<route_id>/stations` | — | stations served by a GTFS route |
| POST | `/auth/signup` | — | create account |
| POST | `/auth/login` | — | get JWT |
| GET/POST | `/subscriptions` | JWT | list / create subscription |
| DELETE | `/subscriptions/<id>` | JWT | delete subscription |
| POST | `/notifications/preview/<id>` | JWT | compute best routes now |
| GET | `/notifications` | JWT | notification history |
| GET | `/health` | — | liveness |

### Accessibility data

The `/accessibility` endpoints query the Trino Gold layer directly. Collection
responses use the same envelope:

```json
{
  "count": 1,
  "limit": 100,
  "offset": 0,
  "items": []
}
```

Examples:

```bash
curl "http://localhost:8000/accessibility/summary"
curl "http://localhost:8000/accessibility/stations?q=Chatelet&accessible=true"
curl "http://localhost:8000/accessibility/stations/71517"
curl "http://localhost:8000/accessibility/elevators?available=false&limit=50"
curl "http://localhost:8000/accessibility/elevators/reliability?q=Nation"
curl "http://localhost:8000/accessibility/elevators/outages?ongoing=true&since_days=30"
curl "http://localhost:8000/accessibility/transfers?accessible=true&max_distance_m=300"
```

Supported collection parameters:

- `q`: case-insensitive partial station name;
- `accessible`: `true` or `false` for stations and transfers;
- `available`: `true` or `false` for elevators;
- `max_distance_m`: maximum transfer walking distance;
- `limit`: from 1 to 500, default 100;
- `offset`: zero-based result offset.

The station endpoints aggregate `gold.station_accessibility` by
`parent_station_id`, so a station with multiple stop points is returned and
counted only once. A station is accessible when at least one stop point has
`wheelchair_boarding = 1`; other values are exposed as
`non_accessible_or_unknown`.

### Mobility analytics data

The analytics endpoints expose aggregates designed for dashboards and consumer
applications. They do not expose large technical or machine-learning tables row
by row. Dates use `YYYY-MM-DD`; every collection accepts `limit` (1–500) and
`offset`.

```bash
curl "http://localhost:8000/crowding/summary?date_from=2025-01-01&date_to=2025-01-31"
curl "http://localhost:8000/crowding/stations?hour=8&cat_jour=JOHV&limit=20"
curl "http://localhost:8000/crowding/timeseries?q=Chatelet"
curl "http://localhost:8000/crowding/ticket-categories?q=Nation"
curl "http://localhost:8000/delays/summary?line=A"
curl "http://localhost:8000/delays/lines?limit=20"
curl "http://localhost:8000/delays/recent?line=A&min_delay_sec=120&since_hours=6"
curl "http://localhost:8000/disruptions/active?line=IDFM:C01371"
curl "http://localhost:8000/disruptions/history?date_from=2025-01-01"
curl "http://localhost:8000/network/lines?q=metro"
curl "http://localhost:8000/network/lines/IDFM:C01371/stations"
```

The Gold-layer exposure is intentionally curated:

| Gold table | API use |
|---|---|
| `station_accessibility` | `/accessibility/summary`, `/accessibility/stations*` |
| `transfer_accessibility` | `/accessibility/transfers` |
| `elevators_availability` | `/accessibility/elevators` and route scoring |
| `elevators_reliability` | `/accessibility/elevators/reliability` |
| `elevators_downtime` | `/accessibility/elevators/outages` |
| `crowding_features` | `/crowding/summary`, `/crowding/stations` and route scoring |
| `validations_daily_by_stop` | `/crowding/timeseries` |
| `validations_by_category` | `/crowding/ticket-categories` |
| `delays_by_stop` | `/delays/summary`, `/delays/lines` and route scoring |
| `next_stop_delays` | `/delays/recent` |
| `disruptions_active` | `/disruptions/active` |
| `disruptions_by_line` | `/disruptions/history` and accessibility enrichment |
| `station_lines` | `/network/lines` and `/network/lines/<route_id>/stations` |
| `dim_stops` | station resolution, proximity and line-station coordinates |
| `trip_schedule` | internal itinerary calculation; not exposed row by row |
| `transfer_walking` | internal transfer calculation; not exposed row by row |
| `service_calendar` | technical GTFS calendar; not exposed directly |
| `next_stop_schedule` | raw real-time schedule; not exposed directly |
| `next_stop_features` | ML feature table; not exposed directly |

`/search` payload — `origin`/`destination` each accept one of `{address}`,
`{lat, lon}`, or `{station}`:

```json
{
  "origin": {"address": "29 rue de Rivoli, Paris"},
  "destination": {"station": "La Défense"},
  "time": "08:00",
  "accessible_required": true,
  "crowding_sensitivity": 2,
  "minimize_walking": true,
  "max_transfers": 1,
  "top_n": 3
}
```

Addresses are geocoded via the French BAN API; coordinates are matched to the
nearest stops (`dim_stops`), and the point→stop walking distance feeds the walking
criterion. Subscriptions reuse the same engine (`engine.params_from_subscription`).

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
