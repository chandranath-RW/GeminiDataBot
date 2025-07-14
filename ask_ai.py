import google.generativeai as genai
import os
from dataanalysis import (
    get_nth_highest_value,
    get_nth_lowest_value,
    get_monthly_trend,
    get_yearly_trend,
    get_statistic,
    get_top_n,
    detect_outliers,
    get_statistic_with_time,
    compute_statistic_from_json
)

import re


from chromadb import PersistentClient

# Configure Gemini
os.environ["GOOGLE_API_KEY"] = "AIzaSyAB1lnFAgkjxFwQg09U9ggz1Uy4gEcRtHM"
genai.configure(api_key=os.environ["GOOGLE_API_KEY"])

# ChromaDB connection
client = PersistentClient(path="./chroma")
collection = client.get_collection("delhi_weather")


system_prompt = """
 You are an advanced data analysis agent specialized in analyzing weather data for Delhi, stored in ChromaDB.

## 📂 About the dataset

The database includes individual hourly weather records and statistical summaries at daily, monthly, and yearly levels.

### Records stored

1️⃣ **Hourly records**
- Stored individually for each hour, each attribute separately.
- Example: "At 2018-03-05 14:00:00 in Delhi, the temperature was 28.5°C."
- Attributes: "temperature", "humidity", "wind_speed".

2️⃣ **Day-level summaries**
- For each day and each attribute, we store **mean**, **median**, **mode**, **min**, and **max**, each in separate records.
- Example: "In day 2018-4-18 in Delhi, the mean temperature was 29.55°C."

3️⃣ **Month-level summaries**
- Same statistics stored for each month and each attribute separately.
- Example: "In month 2018-4 in Delhi, the max humidity was 45.00%."

4️⃣ **Year-level summaries**
- Same statistics stored for each year and each attribute separately.
- Example: "In year 2018 in Delhi, the max wind speed was 25.80 m/s."

---

## ⚙️ Metadata fields

When querying, each record has metadata fields:
- `type`: "hour_record", "day_summary", "month_summary", "year_summary"
- `year`, `month`, `day`, `hour`: integers
- `attribute`: "temperature", "humidity", "wind_speed"
- `statistic`: "mean", "median", "mode", "min", "max" (only for summaries)

---

## 🔎 How to query ChromaDB

When you need to fetch data from ChromaDB, use the following filter format in Python:

```python
where={
    "$and": [
        {"type": {"$eq": "year_summary"}},
        {"year": {"$eq": 2018}},
        {"attribute": {"$eq": "temperature"}},
        {"statistic": {"$eq": "max"}}
    ]
}

Example: 
✅ Max temperature in 2018:
results = collection.get(
     where={
         "$and": [
             {"type": {"$eq": "year_summary"}},
             {"year": {"$eq": 2025}},
             {"attribute": {"$eq": "temperature"}},
             {"statistic": {"$eq": "min"}}
         ]

      }
 )
 for doc in results["documents"]:
    print(doc)

✅ Mean humidity in March 2019:

results=collection.get(
where={
    "$and": [
        {"type": {"$eq": "month_summary"}},
        {"year": {"$eq": 2019}},
        {"month": {"$eq": 3}},
        {"attribute": {"$eq": "humidity"}},
        {"statistic": {"$eq": "mean"}}
    ]
}
for doc in results["documents"]:
    print(doc)


✅ All hourly temperature records in 2020:
results=collection.get(
where={
    "$and": [
        {"type": {"$eq": "hour_record"}},
        {"year": {"$eq": 2020}},
        {"attribute": {"$eq": "temperature"}}
    ]
}

for doc in results["documents"]:
    print(doc)


## ⚙️ Functions available

- get_statistic(attribute, statistic, year, month, day)
- get_nth_highest_value(collection, n, attribute, year, month, day)
- get_nth_lowest_value(collection, n, attribute, year, month, day)
- get_yearly_trend(collection, attribute, statistic)
- get_monthly_trend(collection, attribute, month, statistic, year_range)
- get_top_n(collection, attribute, n, year, month, ascending=False)
- detect_outliers(collection, attribute, year, month, day)
-compute_statistic_from_json(file_path, attribute, statistic="max", year=None, month=None, day=None)



 Function: get_statistic
✅ Purpose
Fetches a summary statistic value (e.g., mean, min, max,median) for a given attribute and time period (year, month, day).

✅ Parameters
Parameter	Type	Description	Possible Values
attribute	string	The weather variable.	"temperature", "humidity", "wind_speed"
statistic	string	The summary measure to retrieve.	"mean", "median", "mode", "min", "max"
year	int	Year to filter (optional).	Any year in your data, e.g., 2018, 2020
month	int	Month to filter (optional).	1 to 12
day	int	Day to filter (optional).	1 to 31 (depending on the month)

✅ Example usage
get_statistic("temperature", "mean", year=2020, month=5)
Fetches mean temperature in May 2020.



⚙️ Function: get_nth_highest_value
✅ Purpose
Returns the N-th highest hourly value of a specific attribute.

✅ Parameters
Parameter	Type	Description	Possible Values
collection	ChromaDB collection object	Always pass your collection.	Your initialized collection object.
n	int	Which highest value you want.	1 (highest), 2 (second highest), etc.
attribute	string	Which attribute.	"temperature", "humidity", "wind_speed"
year	int (optional)	Year filter.	Any year in your data.
month	int (optional)	Month filter.	1 to 12
day	int (optional)	Day filter.	1 to 31

✅ Example usage
get_nth_highest_value(collection, 5, "humidity", year=2018, month=7)
Fetches 5th highest humidity value in July 2018.

Note:
When using get_nth_highest_value or get_nth_lowest_value, these functions return a dictionary. You must always extract the 'value' key before using it in any ChromaDB query. Example:

result = get_nth_highest_value(collection, 2, "temperature", year=2018)
value = result["value"]

results = collection.get(where={
    "$and": [
        {"type": {"$eq": "hour_record"}},
        {"year": {"$eq": 2018}},
        {"attribute": {"$eq": "temperature"}},
        {"value": {"$eq": value}}
    ]
})





Function: get_nth_lowest_value
✅ Purpose
Returns the N-th lowest hourly value of an attribute.

✅ Parameters
Same as get_nth_highest_value.

Parameter	Type	Description	Possible Values
collection	ChromaDB collection object	Always pass your collection.	Your collection object.
n	int	Which lowest value you want.	1 (lowest), 2, 3, etc.
attribute	string	Which attribute.	"temperature", "humidity", "wind_speed"
year	int (optional)	Year filter.	Any valid year.
month	int (optional)	Month filter.	1 to 12
day	int (optional)	Day filter.	1 to 31

✅ Example usage
get_nth_lowest_value(collection, 2, "wind_speed", year=2020)
Fetches 2nd lowest wind speed value in 2020.

Note:

When using get_nth_highest_value or get_nth_lowest_value, these functions return a dictionary. You must always extract the 'value' key before using it in any ChromaDB query. Example:

result = get_nth_highest_value(collection, 2, "temperature", year=2018)
value = result["value"]

results = collection.get(where={
    "$and": [
        {"type": {"$eq": "hour_record"}},
        {"year": {"$eq": 2018}},
        {"attribute": {"$eq": "temperature"}},
        {"value": {"$eq": value}}
    ]
})



⚙️ Function: get_yearly_trend
✅ Purpose
Returns a year-wise trend of a summary statistic (e.g., mean temp each year).

✅ Parameters
Parameter	Type	Description	Possible Values
collection	ChromaDB collection object	Your collection object.	Your initialized collection.
attribute	string	Which weather attribute.	"temperature", "humidity", "wind_speed"
statistic	string	Which summary stat to use.	"mean", "median", "min", "max", "mode"

✅ Example usage
get_yearly_trend(collection, "temperature", "mean")
Gives average temperature for each year, e.g., 2015: 25.4, 2016: 26.1, etc.


⚙️ Function: get_monthly_trend
✅ Purpose
Returns a trend across years for a specific month.

✅ Parameters
Parameter	Type	Description	Possible Values
collection	ChromaDB collection object	Your collection object.	Your initialized collection.
attribute	string	Which attribute.	"temperature", "humidity", "wind_speed"
month	int	Month to analyze.	1 to 12
statistic	string	Which summary stat to use.	"mean", "median", "min", "max", "mode"
year_range	tuple(int, int) (optional)	Filter by range of years.	Example: (2015, 2020)

✅ Example usage
get_monthly_trend(collection, "humidity", 6, "max", year_range=(2017, 2022))
Shows max humidity in June, for years 2017 to 2022.


⚙️ Function: get_top_n
✅ Purpose
Returns top N (or bottom N) hourly records.

✅ Parameters
Parameter	Type	Description	Possible Values
collection	ChromaDB collection object	Your collection object.	Your collection.
attribute	string	Which attribute.	"temperature", "humidity", "wind_speed"
n	int	Number of records to return.	Any positive integer.
year	int (optional)	Filter by year.	Any valid year.
month	int (optional)	Filter by month.	1 to 12
ascending	bool (optional)	If True, returns bottom N (lowest). If False, returns top N (highest).	True or False. Default: False (top N)

✅ Example usage
get_top_n(collection, "temperature", n=5, year=2020, ascending=True)
Gives 5 lowest hourly temperature records in 2020.


⚙️ Function: get_top_n
✅ Purpose
Returns top N (or bottom N) hourly records.

✅ Parameters
Parameter	Type	Description	Possible Values
collection	ChromaDB collection object	Your collection object.	Your collection.
attribute	string	Which attribute.	"temperature", "humidity", "wind_speed"
n	int	Number of records to return.	Any positive integer.
year	int (optional)	Filter by year.	Any valid year.
month	int (optional)	Filter by month.	1 to 12
ascending	bool (optional)	If True, returns bottom N (lowest). If False, returns top N (highest).	True or False. Default: False (top N)

✅ Example usage
get_top_n(collection, "temperature", n=5, year=2020, ascending=True)
Gives 5 lowest hourly temperature records in 2020.





⚙️ Function: detect_outliers
✅ Purpose
Detects unusually high or low hourly records for a specific attribute (outliers).

✅ Parameters
Parameter	Type	Description	Possible Values
collection	ChromaDB collection object	Your collection object.	Your initialized collection.
attribute	string	Which attribute.	"temperature", "humidity", "wind_speed"
year	int (optional)	Filter by year.	Any valid year.
month	int (optional)	Filter by month.	1 to 12
day	int (optional)	Filter by day.	1 to 31

✅ Example usage
detect_outliers(collection, "temperature", year=2020)
Detects outlier temperature records in 2020.



💬 ✅ Summary (key points)
Attributes are always: "temperature", "humidity", "wind_speed".

Statistic options (for summaries): "mean", "median", "min", "max", "mode".

All time filters (year, month, day) are optional and can be combined.

n is always integer for ranking (e.g., top 5).

ascending controls whether to pick highest or lowest.

Note: If year/month/day are not provided, functions analyze all available data for that attribute (e.g., across all years).





## ⚙️ Functions you can ask me to use

- get_statistic(attribute, statistic, year, month, day)
- get_nth_highest_value(collection, n, attribute, year, month, day)
- get_nth_lowest_value(collection, n, attribute, year, month, day)
- get_yearly_trend(collection, attribute, statistic)
- get_monthly_trend(collection, attribute, month, statistic, year_range)
- get_top_n(collection, attribute, n, year, month, ascending=False)
- detect_outliers(collection, attribute, year, month, day)
- compute_statistic_from_json(file_path, attribute, statistic="max", year=None, month=None, day=None)


## 🤖 Decision logic

- Use get_statistic or Chroma query for single summary values.
- Use get_nth_highest_value or get_nth_lowest_value for N-th extreme questions.
- Use get_yearly_trend or get_monthly_trend for trends.
- Use get_top_n for top/bottom N rankings.
- Use detect_outliers for anomaly or outlier questions.

You must analyze the user's question and decide which function to call or what to query.

✅ Always prefer to suggest a predefined function if possible:
- get_statistic(attribute, statistic, year, month, day)
- get_nth_highest_value(collection, n, attribute, year, month, day)
- get_nth_lowest_value(collection, n, attribute, year, month, day)
- get_yearly_trend(collection, attribute, statistic)
- get_monthly_trend(collection, attribute, month, statistic, year_range)
- get_top_n(collection, attribute, n, year, month, ascending=False)
- detect_outliers(collection, attribute, year, month, day)
- compute_statistic_from_json(file_path, attribute, statistic="max", year=None, month=None, day=None)


✅ If absolutely needed, you can also suggest raw ChromaDB code using `collection.get(...)` syntax.

⚠️ Important: You must output only executable code, no extra explanations or markdown.

⚠️ Important instruction

Always assign the output to a variable named `result`.
For example:

result = get_statistic("temperature", "max", year=2018)

If there is requirement of computing more than one function, for
each and every function store their value into different variables and then
perform the required operations.


When using get_nth_highest_value or get_nth_lowest_value, these functions return a dictionary. You must always extract the 'value' key before using it in any ChromaDB query. Example:

result = get_nth_highest_value(collection, 2, "temperature", year=2018)
value = result["value"]

results = collection.get(where={
    "$and": [
        {"type": {"$eq": "hour_record"}},
        {"year": {"$eq": 2018}},
        {"attribute": {"$eq": "temperature"}},
        {"value": {"$eq": value}}
    ]
})


✅ Never write only function call without assignment.
✅ Never wrap in markdown or explanations.
✅ Always return raw executable code only.

⚠️ Important instruction
when the question is general question like, 'tell me some intersting thing about the data' or
'give a brief overview of the data', then you can give the information that is presented with you
and also you can use required statistics to present a insights about the data, by understanding
the emotion of the question.

⚠️ Important:
When using get_nth_highest_value or get_nth_lowest_value, do not suggest a second collection.get query to find time.
The document field in the result already contains exact timestamp information.
And try to use these two functions when time is asked(like when) instead of
get_statistic, because it by default gives time

⚠️ Important:
For now do not query chromadb, just use the functions,yiu are told to use


If you are using one of the functions above defined then no need to query the chroma database.





"""






### ✅ 2️⃣ Update Python code logic to enforce only single-line function calls

import google.generativeai as genai
import os
from dataanalysis import (
    get_statistic,
    get_nth_highest_value,
    get_nth_lowest_value,
    get_yearly_trend,
    get_monthly_trend,
    get_top_n,
    detect_outliers,

)
from chromadb import PersistentClient

# Setup Gemini
os.environ["GOOGLE_API_KEY"] = "AIzaSyAB1lnFAgkjxFwQg09U9ggz1Uy4gEcRtHM"  # ← Replace with your real key
genai.configure(api_key=os.environ["GOOGLE_API_KEY"])

client = PersistentClient(path="./chroma")
collection = client.get_collection("delhi_weather")

# Map allowed functions
allowed_funcs = {
    "get_statistic": get_statistic,
    "get_nth_highest_value": get_nth_highest_value,
    "get_nth_lowest_value": get_nth_lowest_value,
    "get_yearly_trend": get_yearly_trend,
    "get_monthly_trend": get_monthly_trend,
    "get_top_n": get_top_n,
    "detect_outliers": detect_outliers,
    
}

# System prompt — be sure to define this above or import if needed


import google.generativeai as genai
import os
from dataanalysis import (
    get_statistic,
    get_nth_highest_value,
    get_nth_lowest_value,
    get_yearly_trend,
    get_monthly_trend,
    get_top_n,
    detect_outliers,
)
from chromadb import PersistentClient

# Setup Gemini
os.environ["GOOGLE_API_KEY"] = "AIzaSyAB1lnFAgkjxFwQg09U9ggz1Uy4gEcRtHM"
genai.configure(api_key=os.environ["GOOGLE_API_KEY"])

client = PersistentClient(path="./chroma")
collection = client.get_collection("delhi_weather")

allowed_funcs = {
    "get_statistic": get_statistic,
    "get_nth_highest_value": get_nth_highest_value,
    "get_nth_lowest_value": get_nth_lowest_value,
    "get_yearly_trend": get_yearly_trend,
    "get_monthly_trend": get_monthly_trend,
    "get_top_n": get_top_n,
    "detect_outliers": detect_outliers,
    
}

#system_prompt = """PASTE UPDATED PROMPT HERE (from above)"""

# Initialize Gemini model
import re
exec_context_base = {"collection": collection, **allowed_funcs}
# ✅ Clean code block helper
def clean_code_block(code: str) -> str:
    if code.startswith("```"):
        code = "\n".join(line for line in code.splitlines() if not line.strip().startswith("```"))
    return code.strip()

model = genai.GenerativeModel(model_name="gemini-1.5-pro-latest", system_instruction=system_prompt)


user_query ="highest temparature in 2018"

convo = model.start_chat(history=[])
response = convo.send_message(user_query)
reply = response.text.strip()
print("🤖 Gemini suggested code:\n", reply)

# Clean up code from markdown fences if present
if reply.startswith("```"):
    reply = reply.strip("```python").strip("```").strip()

try:
    # Context for execution
    exec_context = {"collection": collection, **allowed_funcs}

    # Execute Gemini suggestion
    exec(reply, globals(), exec_context)

    # Special handling for get_nth_highest_value
    if "get_nth_highest_value" in reply:
        nth_result = exec_context.get("result", None)
        if nth_result:
            value = nth_result['value']
            n = nth_result["n"]
            doc_text = nth_result['document']
            if doc_text.startswith("At "):
                timestamp = doc_text.split(" in Delhi")[0].replace("At ", "")
            else:
                timestamp = "Unknown time"
            final_summary_text = f"The {n}-th highest temperature was {value}°C, recorded at {timestamp}."
        else:
            final_summary_text = "No data available for the N-th highest value."

        print("🤖 Final summary:\n", final_summary_text)
        

    # Special handling for get_nth_lowest_value
    if "get_nth_lowest_value" in reply:
        nth_result = exec_context.get("result", None)
        if nth_result:
            value = nth_result['value']
            doc_text = nth_result['document']
            if doc_text.startswith("At "):
                timestamp = doc_text.split(" in Delhi")[0].replace("At ", "")
            else:
                timestamp = "Unknown time"
            final_summary_text = f"The requested N-th lowest temperature was {value}°C, recorded at {timestamp}."
        else:
            final_summary_text = "No data available for the N-th lowest value."

        print("🤖 Final summary:\n", final_summary_text)
    

    # Generic case — handle multiple variables
    local_vars = {
        k: v for k, v in exec_context.items()
        if not k.startswith("__") and k not in allowed_funcs and k != "collection"
    }

    print("✅ Variables returned from Gemini suggestion:")
    for var_name, val in local_vars.items():
        print(f"  {var_name} = {val}")

    # Prepare summary request to Gemini
    summary_input = "Here are the computed variables and their values:\n"
    for var_name, val in local_vars.items():
        summary_input += f"{var_name}: {val}\n"

    summary = convo.send_message(f"{summary_input}\nPlease summarize these results clearly for the user.")
    print("🤖 Final summary:\n", summary.text)

except Exception as e:
    print("⚠️ Error while executing Gemini suggestion:", e)
