from pathlib import Path
import pandas as pd
import logging
import json
from unittest.mock import patch, MagicMock
from src.extract_weather import fetch_weather_data, save_to_csv

# Logger Setup
t_logger = logging.getLogger(__name__)


def setup_logging():
    config_file = "config/logging.json"
    try:
        with open(config_file, encoding="utf-8") as file:
            config = json.load(file)
        logging.config.dictConfig(config)
    except Exception as e:
        logging.exception("Logging setup failed: %s", e)
        raise


setup_logging()

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


# class TestTransformWeatherData:

#     target_function = "transform_weather_data"

#     def test_transform_weather_data(self):
#         description = f"{self.target_function:<30}: Test for returning valid values"
#         try:
#             df = transform_weather_data(fake_api_response)
#             assert "latitude" in df.columns
#             assert "longitude" in df.columns
#             assert "timezone" in df.columns
#             assert df.shape[0] == 2
#         except AssertionError:
#             t_logger.info("FAILED: %s", description)
#             raise
#         else:
#             t_logger.info("PASSED: %s", description)


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
            filename = save_to_csv(df, "weather", output_dir=str(tmp_path))
            assert Path(filename).exists()
            df_loaded = pd.read_csv(filename)
            pd.testing.assert_frame_equal(df, df_loaded)
        except AssertionError:
            t_logger.info("FAILED: %s", description)
        else:
            t_logger.info("PASSED: %s", description)
