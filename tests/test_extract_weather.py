import os
import pandas as pd
import logging.config
import logging
import json
from unittest.mock import patch, MagicMock
from src.extract_weather import fetch_weather_data, transform_weather_data, save_to_csv

# Logger Setup
t_logger = logging.getLogger("test_logger")


def setup_logging():
    config_file = "config/logging.json"
    try:
        with open(config_file, encoding="utf-8") as file:
            config = json.load(file)
    except FileNotFoundError:
        t_logger.exception("Logging config file '%s' does not exist", config_file)
        raise
    logging.config.dictConfig(config)


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


@patch("src.extract_weather.requests.get")
def test_fetch_weather_data(mock_get):
    t_logger.info("Testing api call")
    mock_resp = MagicMock()
    mock_resp.json.return_value = fake_api_response
    mock_resp.raise_for_status = lambda: None
    mock_get.return_value = mock_resp

    result = fetch_weather_data("fake_url")
    assert result == fake_api_response


def test_transform_weather_data():
    t_logger.info("Testing data manipulation")
    df = transform_weather_data(fake_api_response)
    assert "latitude" in df.columns
    assert "longitude" in df.columns
    assert "timezone" in df.columns
    assert df.shape[0] == 2


def test_save_to_csv(tmp_path):
    t_logger.info("Testing saving data to csv file")
    df = pd.DataFrame(
        {
            "time": ["2025-07-01T00:00", "2025-07-01T01:00"],
            "temperature_2m": [17.8, 17.3],
        }
    )

    output_dir = tmp_path

    filename = save_to_csv(df, output_dir=str(output_dir))
    assert os.path.isfile(filename)

    df_loaded = pd.read_csv(filename)
    pd.testing.assert_frame_equal(df, df_loaded)
