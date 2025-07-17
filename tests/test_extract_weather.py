from src.extract_weather import fetch_weather_data
from src.helper import setup_logger, save_to_csv
from pathlib import Path
import pandas as pd
import logging
from unittest.mock import patch, MagicMock

# Logger Setup
t_logger = logging.getLogger(__name__)
setup_logger()

# Sample fake API JSON response
fake_api_response = {
    "latitude": 52.52,
    "longitude": 13.419998,
    "timezone": "GMT",
    "hourly_units": {"time": "iso8601", "temperature_2m": "°C"},
    "hourly": {
        "time": ["2025-07-01T00:00", "2025-07-01T01:00"],
        "temperature_2m": [17.8, 17.3],
    },
}


class TestFetchWeatherData:

    target_function = "fetch_weather_data"

    @patch("src.extract_weather.requests.get")
    def test_fetch_weather_data_valid_return_value(self, mock_get):
        description = f"{self.target_function:<30}: Test for returning valid value"

        mock_resp = MagicMock()
        mock_resp.json.return_value = fake_api_response
        mock_resp.raise_for_status = lambda: None
        mock_get.return_value = mock_resp
        try:
            result = fetch_weather_data("fake_url")
            assert result == fake_api_response
        except AssertionError:
            t_logger.info("FAILED: %s", description)
            raise
        else:
            t_logger.info("PASSED: %s", description)


class TestSaveToCsv:

    target_function = "save_to_csv"

    def test_save_to_csv(self, tmp_path):
        description = (
            f"{self.target_function:<30}: Test for file saving and correct return value"
        )
        df = pd.DataFrame(
            {
                "time": ["2025-07-01T00:00", "2025-07-01T01:00"],
                "temperature_2m": [17.8, 17.3],
            }
        )
        try:
            filename = save_to_csv(
                df, "weather", output_dir=str(tmp_path), logger=t_logger
            )
            assert Path(filename).exists()
            df_loaded = pd.read_csv(filename)
            pd.testing.assert_frame_equal(df, df_loaded)
        except AssertionError:
            t_logger.info("FAILED: %s", description)
        else:
            t_logger.info("PASSED: %s", description)
