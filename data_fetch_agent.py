# agents/data_fetch_agent.py

import requests
import json
from datetime import datetime

def fetch_and_save(year, save_dir="data"):
    """
    Fetch data for a given year and save as JSON file.
    """
    import os
    os.makedirs(save_dir, exist_ok=True)

    if year == datetime.now().year:
        end_date = datetime.now().strftime("%Y-%m-%d")
    else:
        end_date = f"{year}-12-31"

    start_date = f"{year}-01-01"

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 28.61,
        "longitude": 77.21,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,relativehumidity_2m,windspeed_10m",
        "timezone": "Asia/Kolkata"
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        file_path = os.path.join(save_dir, f"weather_delhi_{year}.json")
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"✅ Data for {year} fetched and saved to {file_path}!")
    else:
        print(f"❌ Failed for {year}. Status code: {response.status_code}")

def fetch_all_years(start_year=2015, end_year=datetime.now().year, save_dir="data"):
    for year in range(start_year, end_year + 1):
        fetch_and_save(year, save_dir=save_dir)
