import unittest
from unittest.mock import patch

from trino.exceptions import Error as TrinoError

from itineo import create_app
from itineo.config import Config


class TestConfig(Config):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = "sqlite://"
    ENABLE_SCHEDULER = False
    SWAGGER_USER = None
    SWAGGER_PASSWORD = None


class AccessibilityRoutesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = create_app(TestConfig)
        cls.client = cls.app.test_client()

    @patch("itineo.accessibility.gold.accessibility_summary")
    def test_summary(self, summary):
        summary.return_value = {
            "total_stations": 10,
            "accessible_stations": 6,
            "accessible_rate": 60.0,
        }

        response = self.client.get("/accessibility/summary")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["accessible_stations"], 6)
        summary.assert_called_once_with()

    @patch("itineo.accessibility.gold.accessibility_stations")
    def test_station_collection_forwards_filters_and_pagination(self, stations):
        stations.return_value = [{"station_id": 71517, "accessible": True}]

        response = self.client.get(
            "/accessibility/stations?q=Chatelet&accessible=true&limit=25&offset=50"
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["count"], 1)
        stations.assert_called_once_with("Chatelet", True, 25, 50)

    @patch("itineo.accessibility.gold.accessibility_stations")
    def test_station_detail_returns_404(self, stations):
        stations.return_value = []

        response = self.client.get("/accessibility/stations/999999")

        self.assertEqual(response.status_code, 404)
        stations.assert_called_once_with(limit=1, offset=0, station_id=999999)

    @patch("itineo.accessibility.gold.accessibility_stations")
    def test_invalid_boolean_is_rejected_before_query(self, stations):
        response = self.client.get("/accessibility/stations?accessible=maybe")

        self.assertEqual(response.status_code, 400)
        self.assertIn("must be true or false", response.get_json()["error"])
        stations.assert_not_called()

    @patch("itineo.accessibility.gold.elevator_availability")
    def test_elevator_filters(self, elevators):
        elevators.return_value = []

        response = self.client.get("/accessibility/elevators?q=Nation&available=false")

        self.assertEqual(response.status_code, 200)
        elevators.assert_called_once_with("Nation", False, 100, 0)

    @patch("itineo.accessibility.gold.elevator_reliability")
    def test_elevator_reliability_filters(self, reliability):
        reliability.return_value = []

        response = self.client.get(
            "/accessibility/elevators/reliability?q=Nation&limit=20&offset=5"
        )

        self.assertEqual(response.status_code, 200)
        reliability.assert_called_once_with("Nation", 20, 5)

    @patch("itineo.accessibility.gold.elevator_outages")
    def test_elevator_outage_filters(self, outages):
        outages.return_value = []

        response = self.client.get(
            "/accessibility/elevators/outages?q=Nation&ongoing=true&since_days=30"
        )

        self.assertEqual(response.status_code, 200)
        outages.assert_called_once_with("Nation", True, 30, 100, 0)

    @patch("itineo.accessibility.gold.elevator_outages")
    def test_invalid_elevator_outage_window_is_rejected(self, outages):
        response = self.client.get("/accessibility/elevators/outages?since_days=0")

        self.assertEqual(response.status_code, 400)
        outages.assert_not_called()

    @patch("itineo.accessibility.gold.accessibility_transfers")
    def test_transfer_filters(self, transfers):
        transfers.return_value = []

        response = self.client.get(
            "/accessibility/transfers?q=Nation&accessible=true&max_distance_m=250"
        )

        self.assertEqual(response.status_code, 200)
        transfers.assert_called_once_with("Nation", True, 250.0, 100, 0)

    @patch("itineo.accessibility.gold.accessibility_transfers")
    def test_negative_transfer_distance_is_rejected(self, transfers):
        response = self.client.get("/accessibility/transfers?max_distance_m=-1")

        self.assertEqual(response.status_code, 400)
        transfers.assert_not_called()

    @patch("itineo.accessibility.gold.accessibility_summary")
    def test_trino_errors_return_json_503(self, summary):
        summary.side_effect = TrinoError("unavailable")

        response = self.client.get("/accessibility/summary")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.get_json(), {"error": "gold layer unavailable"})


if __name__ == "__main__":
    unittest.main()
