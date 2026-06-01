import io
import json
import os
import time
from datetime import datetime, timezone

import boto3
import requests
import pyarrow as pa
import pyarrow.parquet as pq

OVERPASS_ENDPOINT = "https://overpass-api.de/api/interpreter"

UA = "AirQualityAnalysisProject/1.0"
REQUEST_TIMEOUT_S = 10
RETRIES_PER_ENDPOINT = 1
SLEEP_BETWEEN_LOCATIONS_S = 0.01

BUCKET = os.environ.get("BUCKET", "BUCKET_NAME") # use real name from the documentation 3.1 Data Ingestion

INPUT_PREFIX = os.environ.get(
    "INPUT_PREFIX",
    "ingestion/openaq/locations/"
)

OUTPUT_PREFIX = os.environ.get(
    "OUTPUT_PREFIX",
    "ingestion/osm/location_features"
)

RADIUS_M = int(os.environ.get("RADIUS_M", "300"))
MAX_LOCATIONS = int(os.environ.get("MAX_LOCATIONS", "200"))

s3 = boto3.client("s3")

EXCLUDED_BUILDING_VALUES = {
    "shed",
    "garage",
    "garages",
    "carport",
    "roof",
    "greenhouse",
    "greenhouse_horticulture",
    "hut",
    "cabin",
    "static_caravan",
    "houseboat",
    "container",
    "kiosk",
    "service",
    "transformer_tower",
    "construction",
    "ruins",
    "collapsed",
}


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
        raise FileNotFoundError(f"Keine .parquet-Datei gefunden unter s3://{bucket}/{prefix}")

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


def post_overpass(query: str) -> dict:
    headers = {"User-Agent": UA}
    payload = query.encode("utf-8")
    last_error = None

    for attempt in range(1, RETRIES_PER_ENDPOINT + 1):
        try:
            response = requests.post(
                OVERPASS_ENDPOINT,
                data=payload,
                headers=headers,
                timeout=REQUEST_TIMEOUT_S,
            )

            if response.status_code == 429:
                print(f"Rate limited on attempt {attempt}")
                time.sleep(1 * attempt)
                continue

            response.raise_for_status()
            return response.json()

        except requests.RequestException as e:
            last_error = e
            print(f"Overpass error on attempt {attempt}: {e}")

    raise RuntimeError(f"Overpass request failed. Last error: {last_error}")


def osm_feature_query(lat: float, lon: float, radius_m: int) -> str:
    return f"""
    [out:json][timeout:10];
    (
      way(around:{radius_m},{lat},{lon})["building"];
      relation(around:{radius_m},{lat},{lon})["building"];
      way(around:{radius_m},{lat},{lon})["highway"];
      way(around:{radius_m},{lat},{lon})["landuse"];
      relation(around:{radius_m},{lat},{lon})["landuse"];
    );
    out tags qt;
    """


def extract_input_locations(location_rows: list[dict], max_locations: int) -> list[dict]:
    rows = []
    seen = set()

    for loc in location_rows:
        coordinates = loc.get("coordinates", {}) or {}
        country = loc.get("country", {}) or {}

        lat = coordinates.get("latitude")
        lon = coordinates.get("longitude")
        location_id = loc.get("id")

        if lat is None or lon is None:
            continue

        country_name = country.get("name")
        city_or_locality = loc.get("locality")

        unique_key = (
            country_name,
            city_or_locality,
            float(lon),
            float(lat),
        )

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


def extract_osm_features(overpass_json: dict) -> dict:
    elements = overpass_json.get("elements", [])

    building_ids_all = set()
    main_building_ids = set()
    highway_ids = set()
    landuse_tags = []

    for element in elements:
        element_type = element.get("type")
        element_id = element.get("id")
        tags = element.get("tags", {}) or {}

        unique_id = f"{element_type}:{element_id}"

        if "building" in tags and element_type in {"way", "relation"}:
            building_value = str(tags["building"]).lower()
            building_ids_all.add(unique_id)

            if building_value not in EXCLUDED_BUILDING_VALUES:
                main_building_ids.add(unique_id)

        if "highway" in tags and element_type == "way":
            highway_ids.add(unique_id)

        if "landuse" in tags:
            landuse_tags.append(f"landuse={tags['landuse']}")

    if landuse_tags:
        area_tag = landuse_tags[0]
        area_tag_source = "landuse"
    else:
        area_tag = "unknown"
        area_tag_source = "fallback"

    return {
        "building_count_all": len(building_ids_all),
        "main_building_count": len(main_building_ids),
        "highway_count": len(highway_ids),
        "area_tag": area_tag,
        "area_tag_source": area_tag_source,
    }


def lambda_handler(event, context):
    now_utc = datetime.now(timezone.utc)
    api_call_timestamp = now_utc.isoformat()

    run_id_numeric = int(now_utc.strftime("%Y%m%d%H%M%S"))

    input_prefix = event.get("input_prefix", INPUT_PREFIX)
    radius_m = int(event.get("radius_m", RADIUS_M))
    max_locations = int(event.get("max_locations", MAX_LOCATIONS))

    input_key = find_latest_parquet_key(BUCKET, input_prefix)
    print(f"Reading input locations from s3://{BUCKET}/{input_key}")

    location_rows = read_parquet_from_s3(BUCKET, input_key)
    input_locations = extract_input_locations(location_rows, max_locations)
    print(f"Found {len(input_locations)} input locations (max={max_locations})")

    output_rows = []

    for i, location in enumerate(input_locations, start=1):
        remaining_ms = context.get_remaining_time_in_millis()
        if remaining_ms < 30000:
            print("Stopping early to avoid Lambda timeout.")
            break

        lat = location["latitude"]
        lon = location["longitude"]
        location_id = location["location_id"]

        sys_primary_key = int(f"{run_id_numeric}{i:03d}")

        print(
            f"Processing {i}/{len(input_locations)}: "
            f"location_id={location_id}, country={location['country_name']}, "
            f"city={location['city_or_locality']}, lat={lat}, lon={lon}"
        )

        try:
            overpass_json = post_overpass(osm_feature_query(lat, lon, radius_m))
            osm_features = extract_osm_features(overpass_json)

            output_record = {
                "sys_primary_key": sys_primary_key,
                "location_id": location_id,
                "api_call_timestamp": api_call_timestamp,
                "country_name": location["country_name"],
                "city_or_locality": location["city_or_locality"],
                "lat": lat,
                "lon": lon,
                "building_count_all": osm_features["building_count_all"],
                "main_building_count": osm_features["main_building_count"],
                "highway_count": osm_features["highway_count"],
                "area_tag": osm_features["area_tag"],
                "area_tag_source": osm_features["area_tag_source"],
                "radius_m": radius_m,
                "status": "success",
                "error_message": "",
            }

        except Exception as e:
            output_record = {
                "sys_primary_key": sys_primary_key,
                "location_id": location_id,
                "api_call_timestamp": api_call_timestamp,
                "country_name": location["country_name"],
                "city_or_locality": location["city_or_locality"],
                "lat": lat,
                "lon": lon,
                "building_count_all": None,
                "main_building_count": None,
                "highway_count": None,
                "area_tag": None,
                "area_tag_source": None,
                "radius_m": radius_m,
                "status": "error",
                "error_message": str(e),
            }

        output_rows.append(output_record)

        if SLEEP_BETWEEN_LOCATIONS_S > 0:
            time.sleep(SLEEP_BETWEEN_LOCATIONS_S)

    output_key = f"{OUTPUT_PREFIX}/osm_location_features_latest.parquet"

    print(f"Uploading result to s3://{BUCKET}/{output_key}")
    write_parquet_to_s3(BUCKET, output_key, output_rows)

    print(f"Deleting old parquet files under s3://{BUCKET}/{OUTPUT_PREFIX}/")
    deleted_count = delete_old_parquet_files(BUCKET, OUTPUT_PREFIX, output_key)

    result = {
        "message": "Success",
        "input_s3_uri": f"s3://{BUCKET}/{input_key}",
        "output_s3_uri": f"s3://{BUCKET}/{output_key}",
        "api_call_timestamp": api_call_timestamp,
        "location_count_requested": len(input_locations),
        "location_count_processed": len(output_rows),
        "max_locations": max_locations,
        "radius_m": radius_m,
        "deleted_old_parquet_files": deleted_count,
    }

    print(result)
    return result