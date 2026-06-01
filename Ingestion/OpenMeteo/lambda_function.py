import io
import json
import os
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict

import boto3
import requests
import pyarrow as pa
import pyarrow.parquet as pq

# --- Configuration & Constants ---
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
UA = "AirQualityAnalysisProject/1.0"
REQUEST_TIMEOUT_S = 30
RETRIES = 3
SLEEP_BETWEEN_LOCATIONS_S = 1

BUCKET = os.environ.get("BUCKET", "air-quality-analysis-project-2026")

S3_PREFIX = os.environ.get("S3_PREFIX", "ingestion")
LOCATIONS_PREFIX = os.environ.get(
    "LOCATIONS_PREFIX",
    f"{S3_PREFIX}/openaq/locations/"
)

OUTPUT_PREFIX = os.environ.get(
    "OUTPUT_PREFIX",
    "ingestion/openmeteo/weather"
)

MAX_LOCATIONS = int(os.environ.get("MAX_LOCATIONS", "200"))

s3 = boto3.client("s3")

# --- S3 & Parquet Helpers ---
def find_latest_parquet_key(bucket: str, prefix: str) -> str:
    paginator = s3.get_paginator("list_objects_v2")
    latest_obj = None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                if latest_obj is None or obj["LastModified"] > latest_obj["LastModified"]:
                    latest_obj = obj

    if latest_obj is None:
        raise FileNotFoundError(f"No .parquet file found under s3://{bucket}/{prefix}")

    return latest_obj["Key"]

def read_parquet_from_s3(bucket: str, key: str) -> list[dict]:
    response = s3.get_object(Bucket=bucket, Key=key)
    parquet_bytes = response["Body"].read()

    table = pq.read_table(pa.BufferReader(parquet_bytes))
    return table.to_pylist()

def write_parquet_to_s3(bucket: str, key: str, rows: list[dict]) -> None:
    table = pa.Table.from_pylist(rows)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

def delete_old_parquet_files(bucket: str, prefix: str, keep_key: str) -> int:
    paginator = s3.get_paginator("list_objects_v2")
    keys_to_delete = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet") and key != keep_key:
                keys_to_delete.append({"Key": key})

    deleted_count = 0
    for i in range(0, len(keys_to_delete), 1000):
        chunk = keys_to_delete[i:i + 1000]
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": chunk}
        )
        deleted_count += len(chunk)

    return deleted_count

# --- Data Processing Helpers ---
def extract_input_locations(location_rows: list[dict], max_locations: int) -> list[dict]:
    rows = []
    seen = set()

    for loc in location_rows:
        location_id = loc.get("id")

        coordinates = loc.get("coordinates", {}) or {}
        country = loc.get("country", {}) or {}

        lat = coordinates.get("latitude")
        lon = coordinates.get("longitude")

        if location_id is None or lat is None or lon is None:
            continue

        country_name = country.get("name")
        city_or_locality = loc.get("locality")

        unique_key = (country_name, city_or_locality, float(lon), float(lat))

        if unique_key in seen:
            continue

        seen.add(unique_key)

        rows.append({
            "location_id": location_id,
            "country_name": country_name,
            "city_or_locality": city_or_locality,
            "longitude": float(lon),
            "latitude": float(lat),
        })

        if len(rows) >= max_locations:
            break

    return rows

# --- API Interaction ---
def fetch_historical_weather(lat: float, lon: float, start_date: str, end_date: str) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "weather_code,precipitation_sum,rain_sum,precipitation_hours,snowfall_sum,wind_speed_10m_max,wind_direction_10m_dominant"
    }
    
    headers = {"User-Agent": UA}
    last_error = None

    for attempt in range(1, RETRIES + 1):
        try:
            response = requests.get(
                OPEN_METEO_ARCHIVE_URL,
                params=params,
                headers=headers,
                timeout=REQUEST_TIMEOUT_S
            )

            if response.status_code == 429:
                time.sleep(2 * attempt)
                continue

            response.raise_for_status()
            return response.json()

        except requests.RequestException as e:
            last_error = e
            print(f"Open-Meteo error on attempt {attempt}: {e}")
            time.sleep(2 * attempt)

    raise RuntimeError(f"Open-Meteo API failed after {RETRIES} retries. Last error: {last_error}")

# --- Main Handler ---
def lambda_handler(event, context):
    now_utc = datetime.now(timezone.utc)
    api_call_timestamp = now_utc.isoformat()

    # Calculate yesterday's date dynamically
    yesterday = now_utc - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    
    # Numeric Run ID, e.g. 20260430101530
    run_id_numeric = int(now_utc.strftime("%Y%m%d%H%M%S"))

    locations_prefix = event.get("locations_prefix", LOCATIONS_PREFIX)
    max_locations = int(event.get("max_locations", MAX_LOCATIONS))
    
    # Use event payload if provided (useful for manual backfills), 
    # otherwise default to yesterday for standard automated runs.
    start_date = event.get("start_date", yesterday_str)
    end_date = event.get("end_date", yesterday_str)

    input_key = find_latest_parquet_key(BUCKET, locations_prefix)
    print(f"Reading input locations from s3://{BUCKET}/{input_key}")

    location_rows = read_parquet_from_s3(BUCKET, input_key)
    input_locations = extract_input_locations(location_rows, max_locations)
    print(f"Found {len(input_locations)} input locations (max={max_locations})")

    # Use a defaultdict to group rows by their record_date
    output_rows_by_date = defaultdict(list)

    for i, location in enumerate(input_locations, start=1):
        location_id = location["location_id"]
        lat = location["latitude"]
        lon = location["longitude"]

        print(f"Processing {i}/{len(input_locations)}: lat={lat}, lon={lon} | {start_date} to {end_date}")

        try:
            weather_data = fetch_historical_weather(lat, lon, start_date, end_date)
            daily_data = weather_data.get("daily", {})
            
            times = daily_data.get("time", [])
            weather_codes = daily_data.get("weather_code", [])
            precip_sums = daily_data.get("precipitation_sum", [])
            rain_sums = daily_data.get("rain_sum", [])
            precip_hours = daily_data.get("precipitation_hours", [])
            snowfall_sums = daily_data.get("snowfall_sum", [])
            wind_speeds = daily_data.get("wind_speed_10m_max", [])
            wind_dirs = daily_data.get("wind_direction_10m_dominant", [])

            for day_idx, record_date in enumerate(times):

                output_record = {
                    "location_id": location_id,
                    "api_call_timestamp": api_call_timestamp,
                    "country_name": location["country_name"],
                    "city_or_locality": location["city_or_locality"],
                    "lat": lat,
                    "lon": lon,
                    "record_date": record_date,
                    "weather_code": weather_codes[day_idx] if day_idx < len(weather_codes) else None,
                    "precipitation_sum": precip_sums[day_idx] if day_idx < len(precip_sums) else None,
                    "rain_sum": rain_sums[day_idx] if day_idx < len(rain_sums) else None,
                    "precipitation_hours": precip_hours[day_idx] if day_idx < len(precip_hours) else None,
                    "snowfall_sum": snowfall_sums[day_idx] if day_idx < len(snowfall_sums) else None,
                    "wind_speed_10m_max": wind_speeds[day_idx] if day_idx < len(wind_speeds) else None,
                    "wind_direction_10m_dominant": wind_dirs[day_idx] if day_idx < len(wind_dirs) else None,
                    "status": "success",
                    "error_message": None,
                }
                # Append to the list for this specific date
                output_rows_by_date[record_date].append(output_record)

        except Exception as e:
            # Since there is no data, use the start_date to group this error record
            error_date = start_date 
            
            error_record = {
                "location_id": location_id,
                "api_call_timestamp": api_call_timestamp,
                "country_name": location["country_name"],
                "city_or_locality": location["city_or_locality"],
                "lat": lat,
                "lon": lon,
                "record_date": error_date, 
                "weather_code": None,
                "precipitation_sum": None,
                "rain_sum": None,
                "precipitation_hours": None,
                "snowfall_sum": None,
                "wind_speed_10m_max": None,
                "wind_direction_10m_dominant": None,
                "status": "error",
                "error_message": str(e),
            }
            output_rows_by_date[error_date].append(error_record)

        # Respect API rate limits
        time.sleep(SLEEP_BETWEEN_LOCATIONS_S)

    # --- Write the Parquet files partitioned by year/month/day ---
    total_rows_written = 0
    written_files = []

    for date_str, rows in output_rows_by_date.items():
        # Parse the "YYYY-MM-DD" string to extract year and month
        try:
            dt_obj = datetime.strptime(date_str, "%Y-%m-%d")
            year = dt_obj.strftime("%Y")
            month = dt_obj.strftime("%m")
        except ValueError:
            # Fallback if an unexpected date format slips through
            year = "unknown_year"
            month = "unknown_month"

        # Construct path: ingestion/openmeteo/weather/2026/01/2026-01-29.parquet
        output_key = f"{OUTPUT_PREFIX}/year={year}/month={month}/{date_str}.parquet"

        print(f"Uploading {len(rows)} rows to s3://{BUCKET}/{output_key}")
        write_parquet_to_s3(BUCKET, output_key, rows)
        
        total_rows_written += len(rows)
        written_files.append(output_key)

    result = {
        "message": "Success",
        "input_s3_uri": f"s3://{BUCKET}/{input_key}",
        "output_prefix": f"s3://{BUCKET}/{OUTPUT_PREFIX}/",
        "api_call_timestamp": api_call_timestamp,
        "location_count": len(input_locations),
        "total_rows_written": total_rows_written,
        "files_written": len(written_files),
        "date_range": f"{start_date} to {end_date}",
    }

    print(result)
    return result