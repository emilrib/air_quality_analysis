import json
import os
import time
import random
from io import BytesIO
from datetime import datetime, timezone, timedelta

import boto3
import requests
import pyarrow as pa
import pyarrow.parquet as pq


s3 = boto3.client("s3")

REQUEST_DELAY = 0.3
PM25_PARAMETER_ID = 2


API_KEY = os.environ["API_KEY"]
BUCKET_NAME = os.environ["BUCKET_NAME"]

S3_PREFIX = os.environ.get("S3_PREFIX", "ingestion")
LOCATIONS_PREFIX = os.environ.get(
    "LOCATIONS_PREFIX",
    f"{S3_PREFIX}/locations/"
)

# true  = historical load using DATE_FROM and DATE_TO
# false = daily load for yesterday only
HISTORICAL_LOAD = (
    os.environ.get("HISTORICAL_LOAD", "false").lower() == "true"
)

DATE_FROM = os.environ.get("DATE_FROM")
DATE_TO = os.environ.get("DATE_TO")


def get_load_window():
    now = datetime.now(timezone.utc)

    if HISTORICAL_LOAD:
        if not DATE_FROM or not DATE_TO:
            raise ValueError("Historical load requires DATE_FROM and DATE_TO")

        return "historical", DATE_FROM, DATE_TO

    yesterday = now - timedelta(days=1)

    return (
        "daily",
        yesterday.strftime("%Y-%m-%d"),
        now.strftime("%Y-%m-%d"),
    )


def fetch_data(url, headers, params=None, max_retries=5):
    params = params or {}

    for attempt in range(max_retries):
        try:
            time.sleep(REQUEST_DELAY)

            api_call_timestamp = datetime.now(timezone.utc).isoformat()

            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=30,
            )

            print(f"REQUEST URL: {response.url}")
            print(f"STATUS CODE: {response.status_code}")

            if response.status_code == 401:
                raise Exception("Unauthorized: check API key")

            if response.status_code == 429:
                wait_time = (2 ** attempt) + random.uniform(0.5, 1.5)
                print(f"429 received. Sleeping {wait_time:.2f}s")
                time.sleep(wait_time)
                continue

            response.raise_for_status()

            results = response.json().get("results", [])

            for item in results:
                item["sys_source_from"] = response.url
                item["api_call_timestamp"] = api_call_timestamp

            return results

        except requests.exceptions.RequestException as e:
            wait_time = 2 ** attempt
            print(f"Request error: {e}. Retrying in {wait_time}s")
            time.sleep(wait_time)

    return []


def find_latest_locations_parquet(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    latest_obj = None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(".parquet"):
                continue

            if latest_obj is None or obj["LastModified"] > latest_obj["LastModified"]:
                latest_obj = obj

    if latest_obj is None:
        raise FileNotFoundError(
            f"No parquet files found under s3://{bucket}/{prefix}"
        )

    return latest_obj["Key"]


def read_locations_parquet(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    table = pq.read_table(BytesIO(obj["Body"].read()))
    return table.to_pylist()


def extract_pm25_sensors(locations):
    sensors = {}

    for location in locations:
        location_id = location.get("id")
        location_name = location.get("name")
        locality = location.get("locality")
        timezone_value = location.get("timezone")

        country = location.get("country") or {}
        coordinates = location.get("coordinates") or {}

        for sensor in location.get("sensors", []) or []:
            parameter = sensor.get("parameter") or {}

            try:
                parameter_id = int(parameter.get("id"))
            except Exception:
                continue

            if parameter_id != PM25_PARAMETER_ID:
                continue

            sensor_id = sensor.get("id")

            if sensor_id is None:
                continue

            sensors[sensor_id] = {
                "sensor_id": sensor_id,
                "sensor_name": sensor.get("name"),
                "parameter_id": parameter_id,
                "parameter_name": parameter.get("name"),
                "parameter_units": parameter.get("units"),
                "parameter_display_name": parameter.get("displayName"),
                "location_id": location_id,
                "location_name": location_name,
                "locality": locality,
                "timezone": timezone_value,
                "country_id": country.get("id"),
                "country_code": country.get("code"),
                "country_name": country.get("name"),
                "location_latitude": coordinates.get("latitude"),
                "location_longitude": coordinates.get("longitude"),
            }

    return list(sensors.values())


def fetch_daily_measurements_for_sensor(
    headers,
    sensor,
    date_from,
    date_to,
    load_type,
    limit=1000,
):
    all_results = []
    page = 1

    url = f"https://api.openaq.org/v3/sensors/{sensor['sensor_id']}/days"

    while True:
        params = {
            "date_from": date_from,
            "date_to": date_to,
            "limit": limit,
            "page": page,
        }

        results = fetch_data(url, headers, params)

        if not results:
            break

        for item in results:
            item["sensor_id"] = sensor["sensor_id"]
            item["sensor_name"] = sensor["sensor_name"]

            item["parameter_id"] = sensor["parameter_id"]
            item["parameter_name"] = sensor["parameter_name"]
            item["parameter_units"] = sensor["parameter_units"]
            item["parameter_display_name"] = sensor["parameter_display_name"]

            item["historical_load"] = HISTORICAL_LOAD
            item["daily_average_load"] = True
            item["load_date_from"] = date_from
            item["load_date_to"] = date_to

        all_results.extend(results)
        page += 1

    return all_results


def save_parquet(bucket, key, data):
    if not data:
        print("No data to save")
        return None

    table = pa.Table.from_pylist(data)

    buffer = BytesIO()
    pq.write_table(table, buffer, compression="snappy")

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    print(f"Saved: s3://{bucket}/{key}")

    return key


def lambda_handler(event, context):
    headers = {
        "X-API-Key": API_KEY,
        "Accept": "application/json",
    }

    now = datetime.now(timezone.utc)
    run_ts = now.strftime("%Y-%m-%d_%H-%M-%S")
    partition = now.strftime("year=%Y/month=%m/day=%d")

    load_type, date_from, date_to = get_load_window()

    locations_key = find_latest_locations_parquet(
        BUCKET_NAME,
        LOCATIONS_PREFIX,
    )

    print(f"Latest locations parquet: s3://{BUCKET_NAME}/{locations_key}")

    locations = read_locations_parquet(
        BUCKET_NAME,
        locations_key,
    )

    print(f"Locations loaded: {len(locations)}")

    pm25_sensors = extract_pm25_sensors(locations)

    print(f"PM2.5 sensors extracted: {len(pm25_sensors)}")
    print(f"Load type: {load_type}")
    print(f"Date from: {date_from}")
    print(f"Date to: {date_to}")

    all_measurements = []

    for sensor in pm25_sensors:
        print(f"Fetching daily measurements for sensor_id={sensor['sensor_id']}")

        measurements = fetch_daily_measurements_for_sensor(
            headers=headers,
            sensor=sensor,
            date_from=date_from,
            date_to=date_to,
            load_type=load_type,
            limit=1000,
        )

        print(
            f"sensor_id={sensor['sensor_id']}: "
            f"{len(measurements)} daily records"
        )

        all_measurements.extend(measurements)

    if not all_measurements:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "No daily PM2.5 measurements found",
                "historical_load": HISTORICAL_LOAD,
                "load_type": load_type,
                "source_locations_key": locations_key,
                "sensors_count": len(pm25_sensors),
                "date_from": date_from,
                "date_to": date_to,
            }),
        }

    output_key = (
        f"{S3_PREFIX}/measurements/"
        f"{partition}/"
        f"measurements_{load_type}_{run_ts}.parquet"
    )

    latest_pointer_key = f"{S3_PREFIX}/_latest/measurements.json"

    save_parquet(
        BUCKET_NAME,
        output_key,
        all_measurements,
    )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=latest_pointer_key,
        Body=json.dumps({
            "key": output_key,
            "source_locations_key": locations_key,
            "historical_load": HISTORICAL_LOAD,
            "load_type": load_type,
            "parameter_id": PM25_PARAMETER_ID,
            "sensors_count": len(pm25_sensors),
            "measurements_count": len(all_measurements),
            "date_from": date_from,
            "date_to": date_to,
            "created_at": now.isoformat(),
        }),
        ContentType="application/json",
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "measurements_count": len(all_measurements),
            "measurements_key": output_key,
            "source_locations_key": locations_key,
            "historical_load": HISTORICAL_LOAD,
            "load_type": load_type,
            "sensors_count": len(pm25_sensors),
            "date_from": date_from,
            "date_to": date_to,
        }),
    }