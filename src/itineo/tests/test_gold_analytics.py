import unittest
from datetime import date, datetime
from unittest.mock import patch

from itineo import create_app, gold
from itineo.config import Config


class TestConfig(Config):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = "sqlite://"
    ENABLE_SCHEDULER = False
    SWAGGER_USER = None
    SWAGGER_PASSWORD = None
    TRINO_CATALOG = "lakehouse"
    GOLD_SCHEMA = "gold"


class GoldAnalyticsQueriesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = create_app(TestConfig)

    @patch("itineo.gold._query")
    def test_crowding_stations_converts_map_coordinates(self, query):
        query.return_value = [
            (
                71517,
                "Châtelet",
                10000.0,
                800.0,
                1200.0,
                12.0,
                "POINT (2.347 48.858)",
                ["METRO", None],
            )
        ]

        with self.app.app_context():
            result = gold.crowding_stations(
                "Chat", 8, "JOHV", "2025-01-01", "2025-01-31", 20, 5
            )

        self.assertEqual(result[0]["longitude"], 2.347)
        self.assertEqual(result[0]["latitude"], 48.858)
        self.assertEqual(result[0]["transport_modes"], ["METRO"])
        sql, params = query.call_args.args
        self.assertIn("lakehouse.gold.crowding_features", sql)
        self.assertEqual(params, ["Chat", 8, "JOHV", "2025-01-01", "2025-01-31"])

    @patch("itineo.gold._query")
    def test_recent_delays_serializes_timestamps(self, query):
        timestamp = datetime(2025, 1, 1, 8, 5)
        query.return_value = [
            (
                "journey",
                "stop",
                "line-ref",
                "A",
                "outbound",
                "Poissy",
                timestamp,
                timestamp,
                120,
                90,
                "delayed",
                "delayed",
                timestamp,
            )
        ]

        with self.app.app_context():
            result = gold.recent_delays("A", 60, 6, 10, 0)

        self.assertEqual(result[0]["aimed_arrival"], "2025-01-01T08:05:00")
        self.assertEqual(result[0]["arrival_delay_sec"], 120)
        sql, params = query.call_args.args
        self.assertIn("lakehouse.gold.next_stop_delays", sql)
        self.assertIn("INTERVAL '6' HOUR", sql)
        self.assertEqual(params, [60, "A", "A"])

    @patch("itineo.gold._query")
    def test_disruption_history_serializes_dates_and_arrays(self, query):
        query.return_value = [("A", date(2025, 1, 2), 3, ["blocking", "information"])]

        with self.app.app_context():
            result = gold.disruptions_history("A", "2025-01-01", None, 10, 0)

        self.assertEqual(result[0]["date"], "2025-01-02")
        self.assertEqual(result[0]["severities"], ["blocking", "information"])
        self.assertIn("lakehouse.gold.disruptions_by_line", query.call_args.args[0])

    @patch("itineo.gold._query")
    def test_elevator_outage_marks_open_event_as_ongoing(self, query):
        started_at = datetime(2025, 1, 1, 7, 0)
        query.return_value = [
            (
                "key",
                "elevator",
                "station",
                "Nation",
                "metro",
                "unavailable",
                "maintenance",
                started_at,
                None,
                3600,
            )
        ]

        with self.app.app_context():
            result = gold.elevator_outages("Nation", True, 30, 10, 0)

        self.assertTrue(result[0]["ongoing"])
        self.assertEqual(result[0]["downtime_minutes"], 60.0)
        self.assertIn("INTERVAL '30' DAY", query.call_args.args[0])

    @patch("itineo.gold._query")
    def test_network_lines_maps_gtfs_route_type(self, query):
        query.return_value = [("route", "1", "La Défense", 1, "agency", 25, 40)]

        with self.app.app_context():
            result = gold.network_lines("1", 10, 0)

        self.assertEqual(result[0]["transport_mode"], "metro")
        self.assertEqual(result[0]["stations"], 25)
        self.assertIn("lakehouse.gold.station_lines", query.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
