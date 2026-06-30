import requests
from flask import current_app


_MIN_SCORE = 0.5
_IDF_LAT = 48.8
_IDF_LON = 2.3


def geocode(address):
    response = requests.get(
        current_app.config["BAN_GEOCODER_URL"],
        params={"q": address, "limit": 1, "lat": _IDF_LAT, "lon": _IDF_LON},
        timeout=10,
    )
    response.raise_for_status()
    features = response.json().get("features") or []
    if not features or features[0]["properties"].get("score", 0) < _MIN_SCORE:
        raise ValueError(f"address not found: {address}")
    lon, lat = features[0]["geometry"]["coordinates"]
    label = features[0]["properties"].get("label", address)
    return float(lat), float(lon), label
