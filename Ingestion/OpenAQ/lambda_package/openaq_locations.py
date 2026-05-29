import json
import os
import requests
import boto3
import time
import random
from datetime import datetime, timezone, timedelta

s3 = boto3.client("s3")

REQUEST_DELAY = 0.3


# ---------------------------
# API CALL (retry + throttling)
# ---------------------------
def fetch_data(url, headers, params=None, max_retries=5):
    params = params or {}

    for attempt in range(max_retries):
        try:
            time.sleep(REQUEST_DELAY)
            response = requests.get(url, headers=headers, params=params, timeout=30)

            if response.status_code == 401:
                raise Exception("Unauthorized: check API key")

            if response.status_code == 429:
                wait_time = (2 ** attempt) + random.uniform(0.5, 1.5)
                print(f"429 → sleeping {wait_time:.2f}s")
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            payload = response.json()
            return payload.get("results", [])

        except requests.exceptions.RequestException as e:
            wait_time = 2 ** attempt
            print(f"Request error: {e}, retry {wait_time}s")
            time.sleep(wait_time)

    return []


# ---------------------------
# PAGINATION
# ---------------------------
def fetch_paginated_data(url, headers, params=None, limit=1000, max_records=None):
    all_results = []
    page = 1
    base_params = dict(params or {})

    while True:
        request_params = {**base_params, "limit": limit, "page": page}
        results = fetch_data(url, headers, request_params)

        if not results:
            break

        all_results.extend(results)

        if max_records is not None and len(all_results) >= max_records:
            return all_results[:max_records]

        page += 1

    return all_results


# ---------------------------
# S3 STATE (incremental)
# ---------------------------
def get_last_timestamp(bucket, prefix, sensor_id):
    key = f"{prefix}/state/sensor_{sensor_id}.json"
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read()).get("last_timestamp")
    except Exception:
        return None


def save_last_timestamp(bucket, prefix, sensor_id, timestamp):
    key = f"{prefix}/state/sensor_{sensor_id}.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps({"last_timestamp": timestamp}),
        ContentType="application/json"
    )


# ---------------------------
# S3 WRITE
# ---------------------------
def save_to_s3(bucket, prefix, dataset, data):
    if not data:
        return

    now = datetime.utcnow()
    file_ts = now.strftime("%Y-%m-%d_%H-%M-%S")
    partition = now.strftime("year=%Y/month=%m/day=%d")
    key = f"{prefix}/{dataset}/{partition}/{dataset}_{file_ts}.json"

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json"
    )

    print(f"Saved: {key}")


# ---------------------------
# TIMESTAMP HELPERS
# ---------------------------
def _get_nested_value(record, path):
    value = record
    for key in path:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return None
    return value


def extract_end_timestamp(record):
    candidate_paths = [
        ("date", "utc"),
        ("datetime", "utc"),
        ("period", "datetimeto", "utc"),
        ("coverage", "datetimeto", "utc"),
        ("period", "datetimeTo", "utc"),
        ("coverage", "datetimeTo", "utc"),
        ("datetimeto", "utc"),
        ("datetimeTo",),
    ]

    for path in candidate_paths:
        value = _get_nested_value(record, path)
        if value:
            return value

    return None


# ---------------------------
# DATE HELPERS
# ---------------------------
def normalize_iso_datetime(value, end_of_day=False):
    if not value:
        return None

    value = value.strip()

    if "T" in value:
        return value

    if end_of_day:
        return f"{value}T23:59:59+00:00"

    return f"{value}T00:00:00+00:00"


def parse_iso_datetime(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def start_of_month(dt):
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def add_one_month(dt):
    if dt.month == 12:
        return dt.replace(year=dt.year + 1, month=1, day=1)
    return dt.replace(month=dt.month + 1, day=1)


def build_month_windows(start_iso, end_iso):
    start_dt = parse_iso_datetime(start_iso)
    end_dt = parse_iso_datetime(end_iso)

    current = start_of_month(start_dt)
    windows = []

    while current <= end_dt:
        next_month = add_one_month(current)
        window_start = max(current, start_dt)
        window_end = min(next_month - timedelta(seconds=1), end_dt)

        windows.append((window_start.isoformat(), window_end.isoformat()))
        current = next_month

    return windows


# ---------------------------
# LAMBDA HANDLER
# ---------------------------
def lambda_handler(event, context):
    api_key = os.environ["API_KEY"]
    bucket = os.environ["BUCKET_NAME"]
    prefix = os.environ.get("S3_PREFIX", "ingestion")

    INITIAL_LOAD = os.environ.get("INITIAL_LOAD", "false").strip().lower() == "true"

    # Optional per-sensor cap
    # Leave unset or empty for no cap
    max_per_sensor_env = os.environ.get("MAX_PER_SENSOR", "").strip()
    MAX_PER_SENSOR = int(max_per_sensor_env) if max_per_sensor_env else None

    # Incremental fallback if no state exists
    increment_lookback_days = int(os.environ.get("INCREMENTAL_LOOKBACK_DAYS", "3"))

    country_env = os.environ.get("COUNTRY_IDS", "92,22,67,54,110")
    country_ids = [c.strip() for c in country_env.split(",") if c.strip()]

    historical_start_env = os.environ.get("HISTORICAL_START_DATE", "").strip()
    historical_end_env = os.environ.get("HISTORICAL_END_DATE", "").strip()

    # Sensor batching
    sensor_batch_start = int(os.environ.get("SENSOR_BATCH_START", "0"))
    sensor_batch_end_env = os.environ.get("SENSOR_BATCH_END", "").strip()
    sensor_batch_end = int(sensor_batch_end_env) if sensor_batch_end_env else None

    headers = {
        "X-API-Key": api_key,
        "Accept": "application/json"
    }

    now_utc = datetime.now(timezone.utc)
    default_historical_start = "2024-01-01T00:00:00+00:00"
    default_historical_end = now_utc.isoformat()

    historical_start = (
        normalize_iso_datetime(historical_start_env)
        if historical_start_env else default_historical_start
    )
    historical_end = (
        normalize_iso_datetime(historical_end_env, end_of_day=True)
        if historical_end_env else default_historical_end
    )

    incremental_fallback_start = (
        now_utc - timedelta(days=increment_lookback_days)
    ).isoformat()

    print(f"INITIAL_LOAD parsed: {INITIAL_LOAD}")
    print(f"MAX_PER_SENSOR: {MAX_PER_SENSOR}")
    print(f"INCREMENTAL_LOOKBACK_DAYS: {increment_lookback_days}")
    print(f"SENSOR_BATCH_START: {sensor_batch_start}")
    print(f"SENSOR_BATCH_END: {sensor_batch_end}")
    print(f"Historical start: {historical_start}")
    print(f"Historical end: {historical_end}")
    print(f"Countries: {country_ids}")
    print(f"Mode: {'INITIAL' if INITIAL_LOAD else 'INCREMENTAL'}")

    if INITIAL_LOAD:
        month_windows = build_month_windows(historical_start, historical_end)
        print(f"Historical monthly windows: {len(month_windows)}")
    else:
        month_windows = []

    # ---------------------------
    # LOCATIONS
    # ---------------------------
    all_locations = []

    for cid in country_ids:
        locations = fetch_data(
            "https://api.openaq.org/v3/locations",
            headers,
            {"countries_id": cid, "parameters_id": 2, "limit": 100}
        )

        if locations:
            all_locations.extend(locations)
            save_to_s3(bucket, prefix, "locations", locations)

    if not all_locations:
        return {"statusCode": 200, "body": "No locations found"}

    # ---------------------------
    # SENSORS
    # ---------------------------
    sensors = []

    for loc in all_locations:
        for s in loc.get("sensors", []):
            if s.get("parameter", {}).get("id") == 2:
                sensors.append({
                    "id": s["id"],
                    "name": s["name"],
                    "location_id": loc.get("id")
                })

    sensors = list({s["id"]: s for s in sensors}.values())

    save_to_s3(bucket, prefix, "sensors", sensors)
    print(f"Total sensors discovered: {len(sensors)}")

    sensors = sensors[sensor_batch_start:sensor_batch_end]
    print(f"Sensors in this batch: {len(sensors)}")

    # ---------------------------
    # MEASUREMENTS
    # ---------------------------
    total_records_saved = 0

    for sensor in sensors:
        sensor_id = sensor["id"]

        try:
            if INITIAL_LOAD:
                # Historical backfill uses raw measurements
                url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements"

                for window_start, window_end in month_windows:
                    print(
                        f"Sensor {sensor_id} historical window "
                        f"date_from={window_start}, date_to={window_end}"
                    )

                    params = {
                        "date_from": window_start,
                        "date_to": window_end,
                        "sort": "asc"
                    }

                    records = fetch_paginated_data(
                        url,
                        headers,
                        params,
                        limit=1000,
                        max_records=MAX_PER_SENSOR
                    )

                    if not records:
                        print(f"No historical data for sensor {sensor_id} in this window")
                        continue

                    ingestion_time = now_utc.isoformat()
                    enriched = [
                        {
                            "data": r,
                            "sys": {
                                "load_date": ingestion_time,
                                "source": "openaq_lambda",
                                "sensor_id": sensor_id,
                                "granularity": "measurement"
                            }
                        }
                        for r in records
                    ]

                    save_to_s3(bucket, prefix, "measurements", enriched)
                    total_records_saved += len(enriched)

                    valid_end_ts = [
                        extract_end_timestamp(r)
                        for r in records
                        if extract_end_timestamp(r)
                    ]

                    if valid_end_ts:
                        save_last_timestamp(bucket, prefix, sensor_id, max(valid_end_ts))
                    else:
                        print(f"No valid historical end timestamps for sensor {sensor_id}")

            else:
                # Incremental mode uses /days and still saves under measurements
                last_ts = get_last_timestamp(bucket, prefix, sensor_id)

                if last_ts:
                    try:
                        dt = parse_iso_datetime(last_ts)
                        date_from = max(
                            (dt + timedelta(seconds=1)).isoformat(),
                            incremental_fallback_start
                        )
                    except Exception:
                        date_from = incremental_fallback_start
                else:
                    date_from = incremental_fallback_start

                date_to = now_utc.isoformat()

                print(
                    f"Sensor {sensor_id} daily averages "
                    f"date_from={date_from}, date_to={date_to}"
                )

                url = f"https://api.openaq.org/v3/sensors/{sensor_id}/days"
                params = {
                    "date_from": date_from,
                    "date_to": date_to
                }

                records = fetch_paginated_data(
                    url,
                    headers,
                    params,
                    limit=1000,
                    max_records=MAX_PER_SENSOR
                )

                if not records:
                    print(f"No daily data for sensor {sensor_id}")
                    continue

                ingestion_time = now_utc.isoformat()
                enriched = [
                    {
                        "data": r,
                        "sys": {
                            "load_date": ingestion_time,
                            "source": "openaq_lambda",
                            "sensor_id": sensor_id,
                            "granularity": "day"
                        }
                    }
                    for r in records
                ]

                save_to_s3(bucket, prefix, "measurements", enriched)
                total_records_saved += len(enriched)

                valid_end_ts = [
                    extract_end_timestamp(r)
                    for r in records
                    if extract_end_timestamp(r)
                ]

                if valid_end_ts:
                    save_last_timestamp(bucket, prefix, sensor_id, max(valid_end_ts))
                else:
                    print(f"No valid daily end timestamps for sensor {sensor_id}; state not updated")

        except Exception as e:
            print(f"Sensor {sensor_id} error: {e}")

    return {
        "statusCode": 200,
        "body": f"Done. Total records saved: {total_records_saved}"
    }