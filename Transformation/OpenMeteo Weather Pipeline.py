import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Source: openmeteo openmeteo table from Glue Catalog
openmeteo_source = glueContext.create_dynamic_frame.from_catalog(
    database="air-quality-db",
    table_name="openmeteo",
    transformation_ctx="openmeteo_source"
)

# Transformation: keep latest load per location + record_date
sql_query = """
WITH ranked_openmeteo AS (
    SELECT
        concat_ws(
            '-',
            CAST(lat AS STRING),
            CAST(lon AS STRING),
            CAST(record_date AS STRING)
        ) AS dw_unique_key,

        CAST(location_id AS INT) AS sys_source_id,
        CAST(date_format(CAST(record_date AS DATE), 'yyyyMMdd') AS INT) AS date_id,
        CAST(lat AS DOUBLE) AS lat,
        CAST(lon AS DOUBLE) AS lon,
        CAST(record_date AS DATE) AS record_date,

        CAST(weather_code AS INT) AS weather_code,
        CAST(precipitation_sum AS DOUBLE) AS precipitation_sum,
        CAST(precipitation_hours AS DOUBLE) AS precipitation_hours,
        CAST(rain_sum AS DOUBLE) AS rain_sum,
        CAST(snowfall_sum AS DOUBLE) AS snowfall_sum,
        CAST(wind_speed_10m_max AS DOUBLE) AS wind_speed_10m_max,
        CAST(wind_direction_10m_dominant AS DOUBLE) AS wind_direction_10m_dominant,

        CAST(api_call_timestamp AS TIMESTAMP) AS sys_load_ts,

        ROW_NUMBER() OVER (
            PARTITION BY lat, lon, CAST(record_date AS DATE)
            ORDER BY CAST(api_call_timestamp AS TIMESTAMP) DESC
        ) AS rn

    FROM openmeteo
    WHERE lat IS NOT NULL 
      AND lon IS NOT NULL 
      AND record_date IS NOT NULL
      AND location_id IS NOT NULL
      AND status = 'success'
)

SELECT
    dw_unique_key,
    sys_source_id,
    date_id,
    weather_code,
    precipitation_sum,
    precipitation_hours,
    rain_sum,
    snowfall_sum,
    wind_speed_10m_max,
    wind_direction_10m_dominant,
    sys_load_ts
FROM ranked_openmeteo
WHERE rn = 1
"""

transformed_openmeteo = sparkSqlQuery(
    glueContext,
    query=sql_query,
    mapping={"openmeteo": openmeteo_source},
    transformation_ctx="transformed_openmeteo"
)

# Incremental logic
current_df = transformed_openmeteo.toDF()
current_df = current_df.dropDuplicates(["dw_unique_key"])

try:
    existing_curated = glueContext.create_dynamic_frame.from_catalog(
        database="air-quality-db",
        table_name="fact_weather",
        transformation_ctx="existing_fact_weather"
    )

    existing_keys_df = existing_curated.toDF() \
        .select("dw_unique_key") \
        .dropDuplicates(["dw_unique_key"])

    incremental_df = current_df.join(
        existing_keys_df,
        on="dw_unique_key",
        how="left_anti"
    )

except Exception:
    incremental_df = current_df

incremental_openmeteo = DynamicFrame.fromDF(
    incremental_df,
    glueContext,
    "incremental_openmeteo"
)

# Data quality check
EvaluateDataQuality().process_rows(
    frame=incremental_openmeteo,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "fact_weather_data_quality",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)

# Target 1: Write only new rows to S3 / Glue Catalog / Athena
s3_sink = glueContext.getSink(
    path="s3://air-quality-analysis-project-2026/Transformation/openmeteo/fact_weather/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["date_id"],
    enableUpdateCatalog=True,
    transformation_ctx="s3_fact_weather_sink"
)

s3_sink.setCatalogInfo(
    catalogDatabase="air-quality-db",
    catalogTableName="fact_weather"
)

s3_sink.setFormat(
    "glueparquet",
    compression="snappy"
)

s3_sink.writeFrame(incremental_openmeteo)

# Target 2: Write only new rows to RDS PostgreSQL
rds_df = incremental_openmeteo.toDF()

# 1. Extract the secure details from your Glue Connection
jdbc_conf = glueContext.extract_jdbc_conf("Jdbc connection")

jdbc_url = "jdbc:postgresql://air-quality-warehouse.cynamiwwmnme.us-east-1.rds.amazonaws.com:5432/air_qualiyt_dw"

# 2. Use the extracted details to write to the database
rds_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "fact_weather") \
    .option("user", "YOUR_USERNAME_HERE") \
    .option("password", "YOUR_PASSWORD_HERE") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

job.commit()