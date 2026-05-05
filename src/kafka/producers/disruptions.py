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
    "https://prim.iledefrance-mobilites.fr/marketplace/disruptions_bulk",
)

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "idfm-disruptions-raw",
)


producer = Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "idfm-disruptions-producer",
        "acks": "all",
        "enable.idempotence": True,
    }
)


def now_utc():
    return datetime.now(timezone.utc)


def sha1(value):
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def fetch_disruptions():
    response = requests.get(
        IDFM_API_URL,
        headers={
            "accept": "application/json",
            "apikey": IDFM_API_KEY,
        },
        timeout=30,
    )

    response.raise_for_status()
    return response.json()


def extract_items(payload):
    if isinstance(payload, dict) and isinstance(payload.get("disruptions"), list):
        return "disruption", payload["disruptions"]

    if isinstance(payload, dict) and isinstance(payload.get("Disruption"), list):
        return "disruption", payload["Disruption"]

    if isinstance(payload, dict):
        siri = payload.get("Siri", {})
        service_delivery = siri.get("ServiceDelivery", {})
        situation_exchange = service_delivery.get(
            "SituationExchangeDelivery", [])

        if isinstance(situation_exchange, dict):
            situation_exchange = [situation_exchange]

        situations = []

        for delivery in situation_exchange:
            situation_list = delivery.get("Situations", {})
            pt_situations = situation_list.get("PtSituationElement", [])

            if isinstance(pt_situations, dict):
                pt_situations = [pt_situations]

            situations.extend(pt_situations)

        if situations:
            return "pt_situation", situations

    return "raw_payload", [payload]


def get_event_id(item):
    if isinstance(item, dict):
        for key in ["id", "disruption_id", "DisruptionId", "SituationNumber", "id_"]:
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
    while True:
        try:
            fetched_at = now_utc()
            payload = fetch_disruptions()
            payload_type, items = extract_items(payload)

            for item in items:
                raw_json = json.dumps(item, ensure_ascii=False, sort_keys=True)
                event_id = get_event_id(item)

                event = {
                    "event_id": event_id,
                    "payload_type": payload_type,
                    "source": "idfm_disruptions_bulk",
                    "fetched_at": fetched_at.isoformat(),
                    "event_date": fetched_at.date().isoformat(),
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
