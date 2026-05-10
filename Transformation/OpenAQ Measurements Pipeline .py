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


# Source: locations table from Glue Catalog
locations_source = glueContext.create_dynamic_frame.from_catalog(
    database="air-quality-db",
    table_name="locations",
    transformation_ctx="locations_source"
)


# Transformation: flatten locations + sensors
sql_query = """
SELECT
    concat_ws(
        '_',
        CAST(id AS STRING),
        CAST(sensor.id AS STRING)
    ) AS sys_unique_key,

    CAST(id AS STRING) AS location_id,
    CAST(name AS STRING) AS location_name,
    CAST(timezone AS STRING) AS timezone,

    CAST(latitude AS DOUBLE) AS location_latitude,
    CAST(longitude AS DOUBLE) AS location_longitude,

    CAST(country.id AS BIGINT) AS country_id,
    CAST(country.name AS STRING) AS country_name,
    CAST(country.code AS STRING) AS country_code,

    CAST(sensor.id AS BIGINT) AS sensor_id,
    to_timestamp(
        api_call_timestamp,
        "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"
    ) AS sys_load_ts,

    CAST(sys_source_from AS STRING) AS sys_source_from

FROM locations
LATERAL VIEW explode(sensors) exploded_sensors AS sensor
WHERE sensor.parameter.id = 2
"""


transformed_locations = sparkSqlQuery(
    glueContext,
    query=sql_query,
    mapping={"locations": locations_source},
    transformation_ctx="transformed_locations"
)


# Incremental logic:
# - remove duplicates inside current run
# - compare against existing curated S3/Athena table
# - keep only new sys_unique_key records
current_df = transformed_locations.toDF()
current_df = current_df.dropDuplicates(["sys_unique_key"])

try:
    existing_curated = glueContext.create_dynamic_frame.from_catalog(
        database="air-quality-db",
        table_name="dim_sensor_location",
        transformation_ctx="existing_dim_sensor_location"
    )

    existing_keys_df = existing_curated.toDF() \
        .select("sys_unique_key") \
        .dropDuplicates(["sys_unique_key"])

    incremental_df = current_df.join(
        existing_keys_df,
        on="sys_unique_key",
        how="left_anti"
    )

except Exception:
    incremental_df = current_df


incremental_locations = DynamicFrame.fromDF(
    incremental_df,
    glueContext,
    "incremental_locations"
)


# Data quality check
EvaluateDataQuality().process_rows(
    frame=incremental_locations,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "dim_sensor_location_data_quality",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)


# Target 1: Write only new rows to S3 / Glue Catalog / Athena
s3_sink = glueContext.getSink(
    path="s3://air-quality-analysis-project-2026/Transformation/openaq/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="s3_dim_sensor_location_sink"
)

s3_sink.setCatalogInfo(
    catalogDatabase="air-quality-db",
    catalogTableName="dim_sensor_location"
)

s3_sink.setFormat(
    "glueparquet",
    compression="snappy"
)

s3_sink.writeFrame(incremental_locations)


# Target 2: Write only new rows to RDS PostgreSQL
rds_df = incremental_locations.toDF()

jdbc_url = "jdbc:postgresql://air-quality-warehouse.cynamiwwmnme.us-east-1.rds.amazonaws.com:5432/air_qualiyt_dw"

rds_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dim_sensor_location") \
    .option("user", "cappuccino") \
    .option("password", "Airquality-Dashboard26*") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()


job.commit()