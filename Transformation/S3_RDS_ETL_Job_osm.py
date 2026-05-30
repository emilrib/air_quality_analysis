from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, expr, round as spark_round

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# 1. Originales Parquet laden
df = spark.read.parquet(
    "s3://air-quality-analysis-project-2026/ingestion/osm/location_features/osm_location_features_latest.parquet"
)

df.createOrReplaceTempView("osm_location_features")

# 2. Athena-Transformation als Spark SQL ausführen
transformed_df = spark.sql("""
WITH input_data AS (
    SELECT *
    FROM osm_location_features
),
successful_rows AS (
    SELECT
        *,
        CAST(main_building_count AS DOUBLE) / (pi() * 300 * 300 / 10000.0) AS main_building_density_ha
    FROM input_data
    WHERE status = 'success'
      AND main_building_count IS NOT NULL
      AND highway_count IS NOT NULL
),
highway_ranked AS (
    SELECT
        *,
        ntile(5) OVER (ORDER BY highway_count) AS highway_score_1_5
    FROM successful_rows
),
building_scored AS (
    SELECT
        *,
        CASE
            WHEN main_building_density_ha < 6  THEN 1
            WHEN main_building_density_ha < 9  THEN 2
            WHEN main_building_density_ha < 12 THEN 3
            WHEN main_building_density_ha < 14 THEN 4
            ELSE 5
        END AS building_score_1_5
    FROM highway_ranked
),
final_scores AS (
    SELECT
        *,
        CAST(
            round(
                0.7 * building_score_1_5 +
                0.3 * highway_score_1_5
            ) AS INT
        ) AS urbanity_score_1_5
    FROM building_scored
),
scored_table AS (
    SELECT
        i.*,
        f.main_building_density_ha,
        f.building_score_1_5,
        f.highway_score_1_5,
        f.urbanity_score_1_5
    FROM input_data i
    LEFT JOIN final_scores f
        ON i.sys_primary_key = f.sys_primary_key
)
SELECT
    location_id,
    urbanity_score_1_5 AS urbanity_score,
    building_score_1_5 AS building_score,
    highway_score_1_5 AS highway_score
FROM scored_table
""")

# 3. Optional: nur gültige Zeilen für die Dimension laden
final_df = transformed_df.dropna(subset=[
    "location_id",
    "urbanity_score",
    "building_score",
    "highway_score"
])

# 4. In RDS schreiben
final_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://air-quality-warehouse.cynamiwwmnme.us-east-1.rds.amazonaws.com:5432/air_qualiyt_dw") \
    .option("dbtable", "dim_urbanity_score") \
    .option("user", "YOUR_PASSWORD_HERE") \
    .option("password", "YOUR_USERNAME_HERE") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()
