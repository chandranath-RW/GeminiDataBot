import os
import json
import pandas as pd
from tqdm import tqdm
from sentence_transformers import SentenceTransformer
import chromadb

# ✅ Load embedding model
model = SentenceTransformer("all-MiniLM-L6-v2")

# ✅ Initialize Chroma client
client = chromadb.PersistentClient(path="./chroma")
collection = client.get_or_create_collection(name="delhi_weather")

def process_file(file_path):
    with open(file_path, "r") as f:
        data = json.load(f)

    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    humidities = hourly.get("relativehumidity_2m", [])
    winds = hourly.get("windspeed_10m", [])

    df = pd.DataFrame({
        "time": pd.to_datetime(times),
        "temperature": temps,
        "humidity": humidities,
        "wind_speed": winds,
    })
    df["year"] = df["time"].dt.year
    df["month"] = df["time"].dt.month
    df["day"] = df["time"].dt.day
    df["hour"] = df["time"].dt.hour
    return df

def describe_stat(series):
    return {
        "mean": series.mean(),
        "median": series.median(),
        "mode": series.mode().iloc[0] if not series.mode().empty else None,
        "min": series.min(),
        "max": series.max()
    }

def generate_stat_texts(period_str, attr_name, stats, period_type):
    texts = [
        f"{period_str}, the mean {attr_name} was {stats['mean']:.2f}{'°C' if attr_name=='temperature' else '%' if attr_name=='humidity' else ' m/s'}.",
        f"{period_str}, the median {attr_name} was {stats['median']:.2f}{'°C' if attr_name=='temperature' else '%' if attr_name=='humidity' else ' m/s'}.",
        f"{period_str}, the mode {attr_name} was {stats['mode']}{'°C' if attr_name=='temperature' else '%' if attr_name=='humidity' else ' m/s'}.",
        f"{period_str}, the minimum {attr_name} was {stats['min']:.2f}{'°C' if attr_name=='temperature' else '%' if attr_name=='humidity' else ' m/s'}.",
        f"{period_str}, the maximum {attr_name} was {stats['max']:.2f}{'°C' if attr_name=='temperature' else '%' if attr_name=='humidity' else ' m/s'}."
    ]
    return texts

def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

if __name__ == "__main__":
    all_texts = []
    all_ids = []

    for year in range(2015, 2026):
        file_name = f"weather_delhi_{year}.json"
        if not os.path.exists(file_name):
            continue

        print(f"⚡ Processing {file_name}...")
        df = process_file(file_name)

        # ✅ Hourly individual records (3 separate lines per row)
        for idx, row in df.iterrows():
            t_str = row['time']
            all_texts.append(f"On {t_str} in Delhi, the temperature was {row['temperature']}°C.")
            all_ids.append(f"{year}_hour_temp_{idx}")

            all_texts.append(f"On {t_str} in Delhi, the humidity was {row['humidity']}%.")
            all_ids.append(f"{year}_hour_hum_{idx}")

            all_texts.append(f"On {t_str} in Delhi, the wind speed was {row['wind_speed']} m/s.")
            all_ids.append(f"{year}_hour_wind_{idx}")

        # ✅ Day-level summaries
        grouped_day = df.groupby(["year", "month", "day"])
        for keys, group_df in grouped_day:
            y, m, d = keys
            period_str = f"On {y}-{m:02d}-{d:02d} in Delhi"

            temp_stats = describe_stat(group_df["temperature"])
            hum_stats = describe_stat(group_df["humidity"])
            wind_stats = describe_stat(group_df["wind_speed"])

            all_texts.extend(generate_stat_texts(period_str, "temperature", temp_stats, "day"))
            all_ids.extend([f"{y}_{m}_{d}_day_temp_{i}" for i in range(5)])

            all_texts.extend(generate_stat_texts(period_str, "humidity", hum_stats, "day"))
            all_ids.extend([f"{y}_{m}_{d}_day_hum_{i}" for i in range(5)])

            all_texts.extend(generate_stat_texts(period_str, "wind speed", wind_stats, "day"))
            all_ids.extend([f"{y}_{m}_{d}_day_wind_{i}" for i in range(5)])

        # ✅ Month-level summaries
        grouped_month = df.groupby(["year", "month"])
        for keys, group_df in grouped_month:
            y, m = keys
            period_str = f"In {pd.to_datetime(f'{y}-{m}-01').strftime('%B %Y')} in Delhi"

            temp_stats = describe_stat(group_df["temperature"])
            hum_stats = describe_stat(group_df["humidity"])
            wind_stats = describe_stat(group_df["wind_speed"])

            all_texts.extend(generate_stat_texts(period_str, "temperature", temp_stats, "month"))
            all_ids.extend([f"{y}_{m}_month_temp_{i}" for i in range(5)])

            all_texts.extend(generate_stat_texts(period_str, "humidity", hum_stats, "month"))
            all_ids.extend([f"{y}_{m}_month_hum_{i}" for i in range(5)])

            all_texts.extend(generate_stat_texts(period_str, "wind speed", wind_stats, "month"))
            all_ids.extend([f"{y}_{m}_month_wind_{i}" for i in range(5)])

        # ✅ Year-level summaries
        grouped_year = df.groupby(["year"])
        for y, group_df in grouped_year:
            period_str = f"In {y} in Delhi"

            temp_stats = describe_stat(group_df["temperature"])
            hum_stats = describe_stat(group_df["humidity"])
            wind_stats = describe_stat(group_df["wind_speed"])

            all_texts.extend(generate_stat_texts(period_str, "temperature", temp_stats, "year"))
            all_ids.extend([f"{y}_year_temp_{i}" for i in range(5)])

            all_texts.extend(generate_stat_texts(period_str, "humidity", hum_stats, "year"))
            all_ids.extend([f"{y}_year_hum_{i}" for i in range(5)])

            all_texts.extend(generate_stat_texts(period_str, "wind speed", wind_stats, "year"))
            all_ids.extend([f"{y}_year_wind_{i}" for i in range(5)])

    # ✅ Clear old records first (optional)
    #print("🗑️ Deleting old records...")
    #collection.delete(where={})

    # ✅ Chunk embeddings to avoid memory issues
    chunk_size = 500
    for text_chunk, id_chunk in zip(chunk_list(all_texts, chunk_size), chunk_list(all_ids, chunk_size)):
        embeddings = model.encode(text_chunk, show_progress_bar=True)
        collection.add(
            documents=text_chunk,
            embeddings=embeddings,
            ids=id_chunk
        )

    print("🎉 All embeddings inserted successfully!")
