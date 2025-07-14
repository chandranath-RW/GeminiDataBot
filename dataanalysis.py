from chromadb import PersistentClient
import pandas as pd

# ✅ Connect to Chroma collection
client = PersistentClient(path="./chroma")
collection = client.get_collection("delhi_weather")

def get_statistic(attribute, statistic, year=None, month=None, day=None):
    filters = []

    if year and not month:
        filters.append({"type": {"$eq": "year_summary"}})
    elif month and not day:
        filters.append({"type": {"$eq": "month_summary"}})
    elif day:
        filters.append({"type": {"$eq": "day_summary"}})
    else:
        filters.append({"type": {"$eq": "year_summary"}})

    filters.append({"attribute": {"$eq": attribute}})
    filters.append({"statistic": {"$eq": statistic}})

    if year:
        filters.append({"year": {"$eq": year}})
    if month:
        filters.append({"month": {"$eq": month}})
    if day:
        filters.append({"day": {"$eq": day}})

    where_filter = {"$and": filters}

    results = collection.get(where=where_filter)
    if results["documents"]:
        doc = results["documents"][0]
        try:
            value_str = doc.split("was")[1].split(".")[0]
            value = float(value_str.strip())
            return value
        except Exception as e:
            return f"⚠️ Could not parse numeric value: {e}"
    else:
        return "No data found."

def get_nth_highest_value(collection, n, attribute, year=None, month=None, day=None):
    """
    Returns a dictionary with 'value', 'document', and 'n'.
    """
    filters = [
        {"type": {"$eq": "hour_record"}},
        {"attribute": {"$eq": attribute}}
    ]

    if year is not None:
        filters.append({"year": {"$eq": year}})
    if month is not None:
        filters.append({"month": {"$eq": month}})
    if day is not None:
        filters.append({"day": {"$eq": day}})

    results = collection.get(where={"$and": filters}, limit=100000)

    # Extract numeric values and sort descending
    values = []
    for doc in results["documents"]:
        try:
            val_str = doc.split("was")[1].split("°")[0].strip()
            val = float(val_str)
            values.append((val, doc))
        except:
            continue

    if not values or len(values) < n:
        return {"value": None, "document": "No data available", "n": n}

    # Sort and pick n-th highest
    sorted_vals = sorted(values, key=lambda x: x[0], reverse=True)
    nth_value, nth_doc = sorted_vals[n - 1]

    return {"value": nth_value, "document": nth_doc, "n": n}



def get_nth_lowest_value(collection, n, attribute="temperature", year=None, month=None, day=None):
    """
    Get the n-th lowest value for a given attribute.

    Parameters:
        collection: ChromaDB collection object
        n: int, e.g., 5 for 5th lowest
        attribute: "temperature", "humidity", "wind_speed"
        year, month, day: optional filters

    Returns:
        Dictionary with value, document text, and metadata
    """
    filters = [
        {"type": {"$eq": "hour_record"}},
        {"attribute": {"$eq": attribute}}
    ]
    if year:
        filters.append({"year": {"$eq": year}})
    if month:
        filters.append({"month": {"$eq": month}})
    if day:
        filters.append({"day": {"$eq": day}})

    where_filter = {"$and": filters}

    results = collection.get(where=where_filter, limit=100000)

    records = []
    for doc, meta in zip(results["documents"], results["metadatas"]):
        try:
            value_str = doc.split(f"{attribute} was ")[1].split(" ")[0].replace("°C", "").replace("%", "").replace("m/s", "")
            value = float(value_str)
            records.append((value, doc, meta))
        except Exception:
            continue

    if len(records) < n:
        return None

    # Sort ascending for lowest
    records.sort(key=lambda x: x[0])

    nth_record = records[n - 1]
    return {
        "value": nth_record[0],
        "document": nth_record[1],
        "metadata": nth_record[2]
    }

def get_statistic_with_time(collection, attribute, statistic, year=None, month=None, day=None):
    """
    Returns summary statistic value and also matches it with hourly records to get the time.

    Parameters:
        collection: ChromaDB collection object
        attribute: "temperature", "humidity", "wind_speed"
        statistic: "mean", "median", "mode", "min", "max"
        year, month, day: optional filters

    Returns:
        dict with:
            - "value": float value of the statistic
            - "hours": list of dicts with year, month, day, hour, and text
                      or a string message if no exact match found
    """
    summary_value = get_statistic(attribute, statistic, year, month, day)
    if summary_value == "No data found.":
        return {"value": None, "hours": "No summary data found."}

    try:
        summary_value_float = float(summary_value)
    except ValueError:
        return {"value": None, "hours": f"Summary value '{summary_value}' could not be converted to float."}

    where_filter = [
        {"type": {"$eq": "hour_record"}},
        {"attribute": {"$eq": attribute}}
    ]
    if year:
        where_filter.append({"year": {"$eq": year}})
    if month:
        where_filter.append({"month": {"$eq": month}})
    if day:
        where_filter.append({"day": {"$eq": day}})

    results = collection.get(where={"$and": where_filter}, limit=100000)

    matching_records = []
    for doc, meta in zip(results["documents"], results["metadatas"]):
        try:
            val_str = doc.split(f"{attribute} was ")[1].split(" ")[0].replace("°C", "").replace("%", "")
            val = float(val_str)
            if abs(val - summary_value_float) < 0.01:
                matching_records.append({
                    "text": doc,
                    "year": meta.get("year"),
                    "month": meta.get("month"),
                    "day": meta.get("day"),
                    "hour": meta.get("hour")
                })
        except:
            continue

    if matching_records:
        return {"value": summary_value_float, "hours": matching_records}
    else:
        return {"value": summary_value_float, "hours": "No exact hourly matches found (possible aggregation or missing data)."}





    

def get_minimum(attribute, year=None, month=None, day=None):
    return get_statistic(attribute, "min", year, month, day)

def get_median(attribute, year=None, month=None, day=None):
    return get_statistic(attribute, "median", year, month, day)

def get_mode(attribute, year=None, month=None, day=None):
    return get_statistic(attribute, "mode", year, month, day)


def get_top_n(collection, attribute, n, year=None, month=None, ascending=False):
    """
    Get top N records for an attribute, optionally filtered by year and/or month.

    Parameters:
        collection: ChromaDB collection object
        attribute: "temperature", "humidity", "wind_speed"
        n: number of top records
        year, month: optional filters
        ascending: if True, return lowest; if False, highest

    Returns:
        List of document strings
    """
    filter_conditions = [
        {"type": {"$eq": "hour_record"}},
        {"attribute": {"$eq": attribute}}
    ]
    if year:
        filter_conditions.append({"year": {"$eq": year}})
    if month:
        filter_conditions.append({"month": {"$eq": month}})

    results = collection.get(
        where={"$and": filter_conditions},
        limit=100000
    )

    values = []
    for doc in results["documents"]:
        try:
            val_str = doc.split("was")[1].split()[0]
            val_str = val_str.replace("°C", "").replace("%", "").strip(".")
            val = float(val_str)
            values.append((val, doc))
        except Exception as e:
            print("Skipping doc due to parsing error:", doc)
            print("Error:", e)
            continue

    if not values:
        return []

    values.sort(reverse=not ascending, key=lambda x: x[0])
    return [doc for _, doc in values[:n]]



import re

import re

def get_yearly_trend(collection, attribute, statistic="mean", year_range=None):
    """
    Get yearly trend for an attribute and statistic (e.g., mean temperature).
    
    Parameters:
        collection: ChromaDB collection object.
        attribute (str): "temperature", "humidity", or "wind_speed".
        statistic (str): "mean", "median", "mode", "min", or "max". Default "mean".
        year_range (tuple): (start_year, end_year). Default None means no filtering.

    Returns:
        Dict {year: value rounded to two decimals}, sorted by year.
    """
    where_filter = {
        "$and": [
            {"type": {"$eq": "year_summary"}},
            {"attribute": {"$eq": attribute}},
            {"statistic": {"$eq": statistic}}
        ]
    }

    results = collection.get(where=where_filter, limit=1000)
    trends = {}

    for doc in results["documents"]:
        try:
            # Extract year
            year_str = doc.split("year ")[1].split(" ")[0]
            year = int(year_str)

            # Use regex to find first number after 'was'
            match = re.search(r"was\s+([-+]?\d*\.\d+|\d+)", doc)
            if match:
                val = float(match.group(1))
                val = round(val, 2)

                # Apply year_range filter if specified
                if year_range:
                    start_year, end_year = year_range
                    if not (start_year <= year <= end_year):
                        continue

                trends[year] = val
            else:
                print("Could not parse value from doc:", doc)

        except Exception as e:
            print("Skipping doc due to error:", doc)
            print("Error:", e)
            continue

    if trends:
        return dict(sorted(trends.items()))
    else:
        return "No data found for trend analysis."





import re

import re

def get_monthly_trend(collection, attribute, month, statistic="mean", year_range=None):
    """
    Returns the trend for a specific month across different years,
    optionally filtered by a range of years.

    Parameters:
        collection: ChromaDB collection object.
        attribute (str): "temperature", "humidity", or "wind_speed".
        month (int): 1–12.
        statistic (str): "mean", "median", "max", "min", "mode". Default: "mean".
        year_range (tuple): (start_year, end_year) to filter years. Default: None.

    Returns:
        Dictionary mapping year to value for the month, rounded to two decimals.
    """
    where_filter = {
        "$and": [
            {"type": {"$eq": "month_summary"}},
            {"attribute": {"$eq": attribute}},
            {"month": {"$eq": month}},
            {"statistic": {"$eq": statistic}}
        ]
    }

    results = collection.get(where=where_filter, limit=1000)
    trends = {}

    for doc in results["documents"]:
        try:
            # Extract year from text
            year_part = doc.split("month ")[1].split("-")[0]
            year_val = int(year_part)

            # Apply year range filter if specified
            if year_range:
                start_year, end_year = year_range
                if not (start_year <= year_val <= end_year):
                    continue

            # Use regex to extract value after "was"
            match = re.search(r"was\s+([-+]?\d*\.\d+|\d+)", doc)
            if match:
                val = float(match.group(1))
                val = round(val, 2)
                trends[year_val] = val
            else:
                print(f"Could not parse value from doc: {doc}")

        except Exception as e:
            print(f"Skipping doc due to error: {doc}")
            print("Error:", e)
            continue

    if trends:
        return dict(sorted(trends.items()))
    else:
        return "No data found for monthly trend analysis in the specified years."

    

def detect_outliers(collection, attribute="temperature", year=None, month=None, day=None):
    """
    Detect outliers for a given attribute using IQR method.

    Parameters:
        collection: ChromaDB collection object
        attribute: "temperature", "humidity", or "wind_speed"
        year, month, day: optional filters

    Returns:
        List of dictionaries with value, document, and metadata for each outlier.
    """
    filters = {"type": "hour_record", "attribute": attribute}
    if year:
        filters["year"] = year
    if month:
        filters["month"] = month
    if day:
        filters["day"] = day

    results = collection.get(where=filters, limit=100000)

    values = []
    docs = []

    for doc, meta in zip(results["documents"], results["metadatas"]):
        try:
            value_str = doc.split(f"{attribute} was ")[1].split(" ")[0].replace("°C", "").replace("%", "")
            value = float(value_str)
            values.append((value, doc, meta))
        except Exception:
            continue

    if not values:
        return []

    # Extract numerical values
    numerical_values = [v[0] for v in values]

    # Compute quartiles
    q1 = pd.Series(numerical_values).quantile(0.25)
    q3 = pd.Series(numerical_values).quantile(0.75)
    iqr = q3 - q1

    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    outliers = []
    for val, doc, meta in values:
        if val < lower_bound or val > upper_bound:
            outliers.append({
                "value": val,
                "document": doc,
                "metadata": meta
            })

    return outliers


import json
import statistics
from collections import Counter

def compute_statistic_from_json(file_path, attribute, statistic="max", year=None, month=None, day=None):
    with open(file_path, "r") as f:
        data = json.load(f)

    filtered = []
    for rec in data:
        if attribute not in rec:
            continue
        if year and rec.get("year") != year:
            continue
        if month and rec.get("month") != month:
            continue
        if day and rec.get("day") != day:
            continue
        filtered.append(rec)

    if not filtered:
        return {"value": None, "times": "No matching records found."}

    values = [rec[attribute] for rec in filtered if rec[attribute] is not None]

    if not values:
        return {"value": None, "times": "No valid attribute values found."}

    if statistic == "max":
        val = max(values)
    elif statistic == "min":
        val = min(values)
    elif statistic == "mean":
        val = round(statistics.mean(values), 2)
    elif statistic == "median":
        val = round(statistics.median(values), 2)
    elif statistic == "mode":
        try:
            val = statistics.mode(values)
        except statistics.StatisticsError:
            val = "No unique mode"
    else:
        return {"value": None, "times": f"Statistic '{statistic}' not supported."}

    times = []
    for rec in filtered:
        if rec[attribute] == val:
            times.append({
                "year": rec.get("year"),
                "month": rec.get("month"),
                "day": rec.get("day"),
                "hour": rec.get("hour")
            })

    if not times:
        times = "No exact time matches found."

    return {
        "value": val,
        "times": times
    }
