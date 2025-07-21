from src.helper import setup_logger
from helper import check_for_raised_exception
from src.transform_weather import (
    get_csv_df,
    process_hourly,
    process_weather_data,
)
import logging
import pandas as pd
import pandas.api.types as ptypes

test_logger = logging.getLogger(__name__)

setup_logger()


class TestGetCsvDf:
    target_function = "get_csv_df"

    def test_missing_file(self):
        description = f"{self.target_function:<30}: Test for handling missing file"

        bad_filename = "/dev/null/subdir"
        check_for_raised_exception(
            FileNotFoundError, description, test_logger, get_csv_df, bad_filename
        )

    def test_emtpy_file(self, tmp_path):
        description = f"{self.target_function:<30}: Test for handling empty file"

        empty_file = tmp_path / "empty_file.csv"
        with open(empty_file, "w", encoding="utf-8") as file:
            file.write("")
        check_for_raised_exception(
            ValueError, description, test_logger, get_csv_df, empty_file
        )

    def test_valid_return(self, tmp_path):
        description = f"{self.target_function:<30}: Test for valid return value"

        csv_content = """,latitude,longitude,generationtime_ms,utc_offset_seconds,timezone,timezone_abbreviation,elevation
time,52.52,13.419998,0.03802776336669922,0,GMT,GMT,38.0
temperature_2m,52.52,13.419998,0.048279762268066406,0,GMT,GMT,38.0"""
        good_df_columns = [
            "",
            "latitude",
            "longitude",
            "generationtime_ms",
            "utc_offset_seconds",
            "timezone",
            "timezone_abbreviation",
            "elevation",
        ]
        good_df_rows = [
            ["time", 52.52, 13.419998, 0.03802776336669922, 0, "GMT", "GMT", 38.0],
            [
                "temperature_2m",
                52.52,
                13.419998,
                0.048279762268066406,
                0,
                "GMT",
                "GMT",
                38.0,
            ],
        ]
        csv_file = tmp_path / "csv_file.csv"
        with open(csv_file, "w", encoding="utf-8") as file:
            file.write(csv_content)

        result = get_csv_df(csv_file)
        result.rename(columns={result.columns[0]: ""}, inplace=True)
        good_df = pd.DataFrame(good_df_rows, columns=good_df_columns)
        try:
            pd.testing.assert_frame_equal(result, good_df), "df's not equal"
        except AssertionError as e:
            test_logger.exception("FAILED: %s: %s", description, e)
            raise
        else:
            test_logger.info("PASSED: %s", description)


class TestProcessHourly:
    target_function = "process_hourly"

    def test_valid_return(self):
        description = f"{self.target_function:<30}: Test for valid return value"

        try:
            test_csv = "tests/test_csv.csv"
            test_df = pd.read_csv(test_csv)
            result_df = process_hourly(test_df)
        except Exception as e:
            test_logger.exception("Failed to read test csv '%s': %s", test_csv, e)
            raise

        try:
            column_count = len(result_df.columns)
            row_count = len(result_df)
            assert column_count == 2, "Columns count is {column_count}; should be 2"
            column_time = result_df.columns[0]
            assert row_count == 168, f"Row count is {row_count}; should be 168"
            assert ptypes.is_datetime64_any_dtype(
                result_df[column_time]
            ), f"'{column_time}' column is dtype {result_df[column_time].dtypes}; should be 'datetime64[ns]'"
            assert all(
                ptypes.is_float_dtype(result_df[column])
                for column in result_df.columns
                if not column.lower().startswith("time")
            ), "Some columns (except 'time' column) are not type 'float'"
        except AssertionError as e:
            test_logger.exception("FAILED: %s: %s", description, e)
            raise
        else:
            test_logger.info("PASSED: %s", description)


class TestProcessWeatherData:
    target_function = "process_weather_data"

    def test_empty_df(self):
        description = f"{self.target_function:<30}: Test for empty data frame"

        empty_df = pd.DataFrame()
        check_for_raised_exception(
            ValueError, description, test_logger, process_weather_data, empty_df
        )

    def test_valid_return(self):
        description = f"{self.target_function:<30}: Test for valid return value"

        try:
            test_csv = "tests/test_csv.csv"
            test_df = pd.read_csv(test_csv)
        except Exception as e:
            test_logger.exception("Failed to read test csv '%s': %s", test_csv, e)
            raise
        result_df = process_weather_data(test_df)

        try:
            column_count = len(result_df.columns)
            assert column_count == 9, "Columns count is {column_count}; should be 9"
            row_count = len(result_df)
            assert row_count == 168, f"Row count is {row_count}; should be 168"
            # Check that each column, from the subset, has the same values on all rows
            subset = result_df.loc[:, "latitude":"elevation"]
            sample = subset.iloc[0]
            assert (subset != sample).any(axis=1).any() == False
        except AssertionError as e:
            test_logger.exception("FAILED: %s: %s", description, e)
            raise
        else:
            test_logger.info("PASSED: %s", description)
