import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    explode,
    sequence,
    to_date,
    date_format,
    year,
    quarter,
    month,
    dayofmonth,
    dayofweek,
    weekofyear,
    when,
    concat,
    lpad,
    col
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

jdbc_url = "jdbc:postgresql://air-quality-warehouse.cynamiwwmnme.us-east-1.rds.amazonaws.com:5432/air_qualiyt_dw"
db_user = "cappuccino"
db_password = "Airquality-Dashboard26*"

create_tables_sql = """
CREATE TABLE IF NOT EXISTS dim_sensor_location (
    sys_unique_key VARCHAR(255) PRIMARY KEY,
    location_id VARCHAR(100),
    location_name VARCHAR(255),
    timezone VARCHAR(100),
    location_latitude DOUBLE PRECISION,
    location_longitude DOUBLE PRECISION,
    country_id BIGINT,
    country_name VARCHAR(255),
    country_code VARCHAR(20),
    sensor_id BIGINT,
    sys_load_ts TIMESTAMP,
    sys_source_from VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS fact_sensor_measure (
    sys_unique_key VARCHAR(255) PRIMARY KEY,
    sensor_id BIGINT,
    sensor_name VARCHAR(255),
    parameter_name VARCHAR(255),
    parameter_units VARCHAR(100),
    parameter_value DOUBLE PRECISION,
    load_date DATE,
    sys_load_ts TIMESTAMP,
    sys_source_from VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    week_of_year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);
"""

# Trigger table creation
empty_df = spark.createDataFrame([], """
    sys_unique_key STRING,
    location_id STRING,
    location_name STRING,
    timezone STRING,
    location_latitude DOUBLE,
    location_longitude DOUBLE,
    country_id LONG,
    country_name STRING,
    country_code STRING,
    sensor_id LONG,
    sys_load_ts TIMESTAMP,
    sys_source_from STRING
""")

empty_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dim_sensor_location") \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", "org.postgresql.Driver") \
    .option("preactions", create_tables_sql) \
    .mode("append") \
    .save()


# Build date dimension
date_df = spark.sql("""
SELECT explode(
    sequence(
        to_date('2020-01-01'),
        to_date('2035-12-31'),
        interval 1 day
    )
) AS full_date
""")

dim_date_df = date_df.select(
    concat(
        year(col("full_date")),
        lpad(month(col("full_date")), 2, "0"),
        lpad(dayofmonth(col("full_date")), 2, "0")
    ).cast("int").alias("date_key"),

    col("full_date"),

    year(col("full_date")).cast("int").alias("year"),
    quarter(col("full_date")).cast("int").alias("quarter"),
    month(col("full_date")).cast("int").alias("month"),

    date_format(col("full_date"), "MMMM").alias("month_name"),

    dayofmonth(col("full_date")).cast("int").alias("day_of_month"),
    dayofweek(col("full_date")).cast("int").alias("day_of_week"),

    date_format(col("full_date"), "EEEE").alias("day_name"),

    weekofyear(col("full_date")).cast("int").alias("week_of_year"),

    when(
        dayofweek(col("full_date")).isin(1, 7),
        True
    ).otherwise(False).alias("is_weekend")
)


# Overwrite dim_date every run
dim_date_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dim_date") \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

job.commit()