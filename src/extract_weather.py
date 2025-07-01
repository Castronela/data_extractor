import requests
import pandas as pd
from datetime import datetime

def fetch_weather_data(api_url: str) -> dict:
	response = requests.get(api_url)
	response.raise_for_status()
	return response.json()


def transform_weather_data(data: dict) -> pd.DataFrame:
	hourly = data['hourly']
	hourly_df = pd.DataFrame(hourly)

	metadata = data.copy()
	del metadata['hourly']
	del metadata['hourly_units']

	for key, value in metadata.items():
		hourly_df[key] = value
	return hourly_df

def save_to_csv(df: pd.DataFrame, output_dir="data") -> str:
	today_str = datetime.today().strftime("%Y%m%d")
	filename = f"{output_dir}/weather_{today_str}.csv"
	df.to_csv(filename, index=False)
	return filename

def main():
	api_url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m"
	weather_json = fetch_weather_data(api_url)
	# print(pd.Series(weather_json))
	df = transform_weather_data(weather_json)
	filename = save_to_csv(df)
	print(f"Saved to {filename}")
		

if __name__ == "__main__":
	main()