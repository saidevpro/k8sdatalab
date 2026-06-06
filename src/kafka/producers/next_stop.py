import hashlib
import json
import os
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer


IDFM_API_KEY = os.environ["IDFM_API_KEY"]

IDFM_API_URL = os.getenv(
    "IDFM_API_URL",
    "https://prim.iledefrance-mobilites.fr/marketplace/estimated-timetable?LineRef=ALL",
)

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "600"))

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "180"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "idfm-next-stop-raw",
)


producer = Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "idfm-next-stop-producer",
        "acks": "all",
        "enable.idempotence": True,
    }
)


def now_utc():
    return datetime.now(timezone.utc)


def sha1(value):
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def fetch_estimated_timetable():
    response = requests.get(
        IDFM_API_URL,
        headers={
            "accept": "application/json",
            "apikey": IDFM_API_KEY,
        },
        timeout=REQUEST_TIMEOUT,
    )

    response.raise_for_status()
    return response.json()


def extract_items(payload):
    if isinstance(payload, dict):
        siri = payload.get("Siri", {})
        service_delivery = siri.get("ServiceDelivery", {})
        timetable_delivery = service_delivery.get(
            "EstimatedTimetableDelivery", [])

        if isinstance(timetable_delivery, dict):
            timetable_delivery = [timetable_delivery]

        journeys = []

        for delivery in timetable_delivery:
            version_frames = delivery.get("EstimatedJourneyVersionFrame", [])

            if isinstance(version_frames, dict):
                version_frames = [version_frames]

            for frame in version_frames:
                vehicle_journeys = frame.get("EstimatedVehicleJourney", [])

                if isinstance(vehicle_journeys, dict):
                    vehicle_journeys = [vehicle_journeys]

                journeys.extend(vehicle_journeys)

        if journeys:
            return "estimated_vehicle_journey", journeys

    return "raw_payload", [payload]


def get_event_id(item):
    if isinstance(item, dict):
        for key in ["DatedVehicleJourneyRef", "EstimatedVehicleJourneyCode", "VehicleJourneyRef", "id"]:
            value = item.get(key)
            if value:
                return str(value)

    raw = json.dumps(item, ensure_ascii=False, sort_keys=True)
    return sha1(raw)


def publish_event(event):
    key = event["event_id"]

    producer.produce(
        topic=KAFKA_TOPIC,
        key=key.encode("utf-8"),
        value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
    )


def main():
    print("Start next stop producers")
    while True:
        try:
            fetched_at = now_utc()
            batch_time = fetched_at.strftime("%Y-%m-%d %H:%M:%S")
            payload = fetch_estimated_timetable()
            payload_type, items = extract_items(payload)

            for item in items:
                raw_json = json.dumps(item, ensure_ascii=False, sort_keys=True)
                event_id = get_event_id(item)

                event = {
                    "event_id": event_id,
                    "payload_type": payload_type,
                    "source": "idfm_estimated_timetable",
                    "fetched_at": fetched_at.isoformat(),
                    "event_date": fetched_at.date().isoformat(),
                    "batch_time": batch_time,
                    "raw_json": raw_json,
                }

                publish_event(event)

            producer.flush()

            print(
                json.dumps(
                    {
                        "status": "ok",
                        "topic": KAFKA_TOPIC,
                        "events": len(items),
                        "fetched_at": fetched_at.isoformat(),
                    },
                    ensure_ascii=False,
                )
            )

        except Exception as exc:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    },
                    ensure_ascii=False,
                )
            )

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
