import requests
from flask import current_app


def geocode(address):
    response = requests.get(
        current_app.config["BAN_GEOCODER_URL"],
        params={"q": address, "limit": 1, "type": "housenumber"},
        timeout=10,
    )
    response.raise_for_status()
    features = response.json().get("features") or []
    if not features:
        raise ValueError(f"address not found: {address}")
    lon, lat = features[0]["geometry"]["coordinates"]
    label = features[0]["properties"].get("label", address)
    return float(lat), float(lon), label
