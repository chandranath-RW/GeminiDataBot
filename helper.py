#import chromadb

# # Initialize Chroma client with the same path
# client = chromadb.PersistentClient(path="./chroma")
# collection = client.get_or_create_collection(name="delhi_weather")

# # ✅ Print total count
# print("✅ Number of embedded records:", collection.count())

# # ✅ Query a simple question to see example context
# question = "What was the weather like on 2018-05-10?"
# results = collection.query(query_texts=[question], n_results=3)

# # Print top 3 retrieved documents
# print("\nTop 3 documents retrieved:")
# for i, doc in enumerate(results["documents"][0], 1):
#     print(f"{i}. {doc}")
# client = chromadb.PersistentClient(path="./chroma")
# client.delete_collection(name="delhi_weather")
import chromadb

# # Connect to Chroma
# client = chromadb.PersistentClient(path="./chroma")
# collection = client.get_or_create_collection(name="delhi_weather")

# # Your question
# query = "What was the average temperature in 2018?"


# # Query Chroma
# results = collection.query(query_texts=[query], n_results=5)

# # Print top documents
# docs = results["documents"][0]
# for i, doc in enumerate(docs):
#     print(f"\n--- Document {i+1} ---\n{doc}")
# from chromadb import PersistentClient
from chromadb import PersistentClient

client = PersistentClient(path="./chroma")
collection = client.get_collection("delhi_weather")

# results = collection.get(
#     where={
#         "$and": [
#             {"type": {"$eq": "year_summary"}},
#             {"year": {"$eq": 2025}},
#             {"attribute": {"$eq": "temperature"}},
#             {"statistic": {"$eq": "min"}}
#         ]
#     }
# )
# results=collection.get(
# where={
#     "$and": [
#         {"type": {"$eq": "day_summary"}},
#         {"year": {"$eq": 2020}},
#         {"month": {"$eq": 4}},
#         {"day": {"$eq": 15}},
#         {"attribute": {"$eq": "wind_speed"}},
#         {"statistic": {"$eq": "median"}}
#     ]
# }
# )
# for doc in results["documents"]:
#     print(doc)
from dataanalysis import get_statistic,get_nth_highest_value,get_nth_lowest_value,get_top_n,get_yearly_trend,get_monthly_trend
# print(get_statistic("temperature", "mean", year=2018))

# print(get_nth_lowest_value(collection, 1, attribute='humidity', year=2018, month=8))
# top5 = get_top_n(collection, attribute="temperature", n=5, year=2019, month=5)
# for doc in top5:
#     print(doc)

trend = get_monthly_trend(collection, attribute="temperature", statistic="mean",month=5)
print(trend)













''''
⚙️ Function: get_statistic_with_time
✅ Purpose
Fetches a summary statistic (e.g., max, mean) for a given attribute and period, and also finds corresponding hourly times when this value was recorded.

✅ Parameters
Parameter	Type	Description	Possible Values
collection	ChromaDB collection object	Your collection object.	Your initialized collection.
attribute	string	Weather attribute.	"temperature", "humidity", "wind_speed"
statistic	string	Statistic type.	"mean", "median", "min", "max", "mode"
year	int (optional)	Year filter.	Any valid year.
month	int (optional)	Month filter.	1 to 12
day	int (optional)	Day filter.	1 to 31

✅ Returns
A dictionary with:
- "value": float value of the summary statistic.
- "hours": 
   • List of dicts, each with "year", "month", "day", "hour", "text" if matches found.
   • Or a string like "No exact hourly matches found (possible aggregation or missing data)."

✅ Example usage
result = get_statistic_with_time(collection, "temperature", "max", year=2019)
max_temp = result["value"]
if isinstance(result["hours"], list):
    times = [f"{rec['year']}-{rec['month']}-{rec['day']} at {rec['hour']}:00" for rec in result["hours"]]
    print(f"The maximum temperature in 2019 was {max_temp}°C, recorded at: {', '.join(times)}")
else:
    print(f"The maximum temperature in 2019 was {max_temp}°C. {result['hours']}")
'''






'''
⚙️ Function: compute_statistic_from_json
✅ Purpose
Reads a weather JSON file and computes a summary statistic (e.g., max, min, mean, median, mode) for a given attribute. Returns the value along with exact times (year, month, day, hour) when that value occurred.

✅ Parameters
Parameter	Type	Description	Possible Values
file_path	string	Path to the JSON file.	Example: "weather_delhi_2019.json"
attribute	string	Weather attribute to analyze.	"temperature", "humidity", "wind_speed"
statistic	string	Statistic to compute.	"max", "min", "mean", "median", "mode"
year	int (optional)	Filter data by year.	Any valid year, e.g., 2019
month	int (optional)	Filter by month.	1 to 12
day	int (optional)	Filter by day.	1 to 31

✅ Example usage
compute_statistic_from_json("weather_delhi_2019.json", "temperature", "max", year=2019)

Returns:
{
  "value": 44.0,
  "times": [
    {"year": 2019, "month": 5, "day": 30, "hour": 15},
    {"year": 2019, "month": 6, "day": 1, "hour": 14}
  ]
}

Notes:
- File must follow the pattern: "weather_delhi_{year}.json".
- If no year/month/day is given, uses all data from that file.
- Use this function when both the value and its exact times are needed.
'''