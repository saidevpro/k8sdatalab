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


class AnalyticsRoutesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = create_app(TestConfig)
        cls.client = cls.app.test_client()

    @patch("itineo.analytics.gold.crowding_summary")
    def test_crowding_summary_date_range(self, summary):
        summary.return_value = {"stations": 12}

        response = self.client.get(
            "/crowding/summary?date_from=2025-01-01&date_to=2025-01-31"
        )

        self.assertEqual(response.status_code, 200)
        summary.assert_called_once_with("2025-01-01", "2025-01-31")

    @patch("itineo.analytics.gold.crowding_stations")
    def test_crowding_station_filters(self, stations):
        stations.return_value = [{"station_id": 71517}]

        response = self.client.get(
            "/crowding/stations?q=Chatelet&hour=8&cat_jour=JOHV"
            "&date_from=2025-01-01&date_to=2025-01-31&limit=25&offset=10"
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["count"], 1)
        stations.assert_called_once_with(
            "Chatelet", 8, "JOHV", "2025-01-01", "2025-01-31", 25, 10
        )

    @patch("itineo.analytics.gold.crowding_stations")
    def test_invalid_crowding_hour_is_rejected(self, stations):
        response = self.client.get("/crowding/stations?hour=24")

        self.assertEqual(response.status_code, 400)
        stations.assert_not_called()

    @patch("itineo.analytics.gold.crowding_timeseries")
    def test_invalid_date_range_is_rejected(self, timeseries):
        response = self.client.get(
            "/crowding/timeseries?date_from=2025-02-01&date_to=2025-01-01"
        )

        self.assertEqual(response.status_code, 400)
        timeseries.assert_not_called()

    @patch("itineo.analytics.gold.crowding_ticket_categories")
    def test_ticket_category_filters(self, categories):
        categories.return_value = []

        response = self.client.get(
            "/crowding/ticket-categories?q=Nation&date_from=2025-01-01"
        )

        self.assertEqual(response.status_code, 200)
        categories.assert_called_once_with("Nation", "2025-01-01", None, 100, 0)

    @patch("itineo.analytics.gold.delays_summary")
    def test_delays_summary_line_filter(self, summary):
        summary.return_value = {"on_time_pct": 89.5}

        response = self.client.get("/delays/summary?line=RER%20A")

        self.assertEqual(response.status_code, 200)
        summary.assert_called_once_with("RER A")

    @patch("itineo.analytics.gold.recent_delays")
    def test_recent_delay_filters(self, delays):
        delays.return_value = []

        response = self.client.get(
            "/delays/recent?line=A&min_delay_sec=120&since_hours=6&limit=20&offset=5"
        )

        self.assertEqual(response.status_code, 200)
        delays.assert_called_once_with("A", 120, 6, 20, 5)

    @patch("itineo.analytics.gold.recent_delays")
    def test_invalid_delay_window_is_rejected(self, delays):
        response = self.client.get("/delays/recent?since_hours=0")

        self.assertEqual(response.status_code, 400)
        delays.assert_not_called()

    @patch("itineo.analytics.gold.active_disruptions")
    def test_active_disruption_filters(self, disruptions):
        disruptions.return_value = []

        response = self.client.get(
            "/disruptions/active?q=travaux&line=A&severity=blocking&limit=50"
        )

        self.assertEqual(response.status_code, 200)
        disruptions.assert_called_once_with("travaux", "A", "blocking", 50, 0)

    @patch("itineo.analytics.gold.disruptions_history")
    def test_disruption_history_filters(self, history):
        history.return_value = []

        response = self.client.get(
            "/disruptions/history?line=A&date_from=2025-01-01&date_to=2025-01-31"
        )

        self.assertEqual(response.status_code, 200)
        history.assert_called_once_with("A", "2025-01-01", "2025-01-31", 100, 0)

    @patch("itineo.analytics.gold.network_lines")
    def test_network_line_search(self, lines):
        lines.return_value = []

        response = self.client.get("/network/lines?q=metro&limit=10")

        self.assertEqual(response.status_code, 200)
        lines.assert_called_once_with("metro", 10, 0)

    @patch("itineo.analytics.gold.network_line_stations")
    def test_network_line_stations_support_route_ids_with_slashes(self, stations):
        stations.return_value = []

        response = self.client.get("/network/lines/IDFM%3AC01371%2FA/stations")

        self.assertEqual(response.status_code, 200)
        stations.assert_called_once_with("IDFM:C01371/A", 500, 0)

    @patch("itineo.analytics.gold.crowding_summary")
    def test_trino_errors_return_json_503(self, summary):
        summary.side_effect = TrinoError("unavailable")

        response = self.client.get("/crowding/summary")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.get_json(), {"error": "gold layer unavailable"})


if __name__ == "__main__":
    unittest.main()
