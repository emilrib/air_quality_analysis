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


measurements_source = glueContext.create_dynamic_frame.from_catalog(
    database="air-quality-db",
    table_name="measurements",
    transformation_ctx="measurements_source"
)

measurements_historical_source = glueContext.create_dynamic_frame.from_catalog(
    database="air-quality-db",
    table_name="measurements_historical",
    transformation_ctx="measurements_historical_source"
)


sql_query = """
WITH current_measurements AS (
    SELECT
        concat_ws('-', CAST(sensor_id AS STRING), CAST(load_date_from AS STRING)) AS sys_unique_key,
        CAST(sensor_id AS BIGINT) AS sensor_id,
        CAST(sensor_name AS STRING) AS sensor_name,
        CAST(parameter_display_name AS STRING) AS parameter_name,
        CAST(parameter_units AS STRING) AS parameter_units,
        CAST(value AS DOUBLE) AS parameter_value,
        CAST(load_date_from AS DATE) AS load_date,
        to_timestamp(api_call_timestamp, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX") AS sys_load_ts,
        CAST(sys_source_from AS STRING) AS sys_source_from
    FROM measurements
    WHERE sensor_id IS NOT NULL
      AND load_date_from IS NOT NULL
      AND parameter_display_name = 'pm25'
),

historical_raw AS (
    SELECT
        regexp_replace(CAST(sensors_id AS STRING), '^"|"$', '') AS sensor_id_clean,
        regexp_replace(parameter, '^"|"$', '') AS parameter_clean,
        regexp_replace(units, '^"|"$', '') AS units_clean,
        CAST(regexp_replace(value, '^"|"$', '') AS DOUBLE) AS value_clean,
        to_date(to_timestamp(regexp_replace(datetime, '^"|"$', ''), "yyyy-MM-dd'T'HH:mm:ssXXX")) AS load_date_clean,
        to_timestamp(regexp_replace(datetime, '^"|"$', ''), "yyyy-MM-dd'T'HH:mm:ssXXX") AS sys_load_ts_clean
    FROM measurements_historical
    WHERE regexp_replace(parameter, '^"|"$', '') = 'PM2.5'
      AND sensors_id IS NOT NULL
      AND datetime IS NOT NULL
),

historical_measurements AS (
    SELECT
        concat_ws('-', sensor_id_clean, CAST(load_date_clean AS STRING)) AS sys_unique_key,
        CAST(sensor_id_clean AS BIGINT) AS sensor_id,
        concat_ws(' ', parameter_clean, units_clean) AS sensor_name,
        parameter_clean AS parameter_name,
        units_clean AS parameter_units,
        AVG(value_clean) AS parameter_value,
        load_date_clean AS load_date,
        MIN(sys_load_ts_clean) AS sys_load_ts,
        CAST('historical_openaq' AS STRING) AS sys_source_from
    FROM historical_raw
    WHERE load_date_clean IS NOT NULL
      AND sensor_id_clean IS NOT NULL
      AND value_clean IS NOT NULL
    GROUP BY
        sensor_id_clean,
        parameter_clean,
        units_clean,
        load_date_clean
),

unioned_measurements AS (
    SELECT * FROM current_measurements
    UNION ALL
    SELECT * FROM historical_measurements
),

ranked_measurements AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY sensor_id, load_date
            ORDER BY sys_load_ts DESC
        ) AS rn
    FROM unioned_measurements
)

SELECT
    sys_unique_key,
    sensor_id,
    sensor_name,
    parameter_name,
    parameter_units,
    parameter_value,
    load_date,
    date_format(load_date, 'yyyyMMdd') AS date_id,
    sys_load_ts,
    sys_source_from
FROM ranked_measurements
WHERE rn = 1
"""


transformed_measurements = sparkSqlQuery(
    glueContext,
    query=sql_query,
    mapping={
        "measurements": measurements_source,
        "measurements_historical": measurements_historical_source
    },
    transformation_ctx="transformed_measurements"
)


current_df = transformed_measurements.toDF()
current_df = current_df.dropDuplicates(["sys_unique_key"])

print("current_df count:", current_df.count())


# Keep S3/Athena incremental
try:
    existing_curated = glueContext.create_dynamic_frame.from_catalog(
        database="air-quality-db",
        table_name="fact_sensor_measure",
        transformation_ctx="existing_fact_sensor_measure"
    )

    existing_keys_df = existing_curated.toDF() \
        .select("sys_unique_key") \
        .dropDuplicates(["sys_unique_key"])

    incremental_df = current_df.join(
        existing_keys_df,
        on="sys_unique_key",
        how="left_anti"
    )

except Exception as e:
    print("Could not read existing Athena/S3 table. Treating as first load.")
    print(str(e))
    incremental_df = current_df


print("incremental_df count:", incremental_df.count())


incremental_measurements = DynamicFrame.fromDF(
    incremental_df,
    glueContext,
    "incremental_measurements"
)


EvaluateDataQuality().process_rows(
    frame=DynamicFrame.fromDF(current_df, glueContext, "dq_current_measurements"),
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "fact_sensor_measure_data_quality",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)


# Write only new rows to S3/Athena
s3_sink = glueContext.getSink(
    path="s3://air-quality-analysis-project-2026/Transformation/openaq/fact_sensor_measure/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="s3_fact_sensor_measure_sink"
)

s3_sink.setCatalogInfo(
    catalogDatabase="air-quality-db",
    catalogTableName="fact_sensor_measure"
)

s3_sink.setFormat(
    "glueparquet",
    compression="snappy"
)

s3_sink.writeFrame(incremental_measurements)


# Fully refresh RDS from current_df, not incremental_df
rds_df = current_df

print("rds_df count:", rds_df.count())

jdbc_url = "jdbc:postgresql://air-quality-warehouse.cynamiwwmnme.us-east-1.rds.amazonaws.com:5432/air_qualiyt_dw"

rds_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "fact_sensor_measure") \
    .option("user", "cappuccino") \
    .option("password", "Airquality-Dashboard26*") \
    .option("driver", "org.postgresql.Driver") \
    .option("truncate", "true") \
    .mode("overwrite") \
    .save()


job.commit()