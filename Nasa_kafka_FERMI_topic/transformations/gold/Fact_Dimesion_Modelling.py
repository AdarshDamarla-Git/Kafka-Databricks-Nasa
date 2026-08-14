import dlt
from pyspark.sql.functions import col, hash, split, trim
import utilities.gold_schema_definitions as schema_def

# Source Silver Table Name
SILVER_TABLE = "kafka_nasa_fermi_topic.silver.FERMI_topic_cleaned_data_OBT"


# -----------------------------------------------------------------------------
# 1. Dimension: Event Timing
# -----------------------------------------------------------------------------
@dlt.table(
    name="kafka_nasa_fermi_topic.gold.dim_event_timing",
    comment="Dimension containing temporal aspects of the GCN trigger.",
    schema=schema_def.dim_event_timing_schema
)
def dim_event_timing():
    return (
        dlt.read_stream(SILVER_TABLE)
        .select(
            hash("GRB_DATE", "GRB_TIME").alias("timing_sk"),
            col("GRB_DATE").alias("grb_date_raw"),
            col("GRB_TIME").alias("grb_time_raw"),
            col("NOTICE_DATE").alias("notice_date_raw")
        )
        .dropDuplicates(["timing_sk"])
    )


# -----------------------------------------------------------------------------
# 2. Parent Dimension: Spatial Coordinates
# -----------------------------------------------------------------------------
@dlt.table(
    name="kafka_nasa_fermi_topic.gold.dim_spatial_coords",
    comment="Dimension containing GRB coordinates and linking to Solar/Lunar context.",
    schema=schema_def.dim_spatial_coords_schema
)
def dim_spatial_coords():
    return (
        dlt.read_stream(SILVER_TABLE)
        .select(
            hash("GRB_RA", "GRB_DEC", "GAL_COORDS").alias("spatial_sk"),
            hash("SUN_POSTN", "MOON_POSTN").alias("sun_moon_sk"),  # Foreign key to dim_sun_moon_coords
            col("GRB_RA").alias("right_ascension"),
            col("GRB_DEC").alias("declination"),
            col("GAL_COORDS").alias("galactic_coords"),
            col("ECL_COORDS").alias("ecliptic_coords")
        )
        .dropDuplicates(["spatial_sk"])
    )


# -----------------------------------------------------------------------------
# 3. Snowflake Child Dimension: Sun & Moon Coordinates
# -----------------------------------------------------------------------------
@dlt.table(
    name="kafka_nasa_fermi_topic.gold.dim_sun_moon_coords",
    comment="Snowflake dimension containing Solar and Lunar coordinates.",
    schema=schema_def.dim_sun_moon_coords_schema
)
def dim_sun_moon_coords():
    return (
        dlt.read_stream(SILVER_TABLE)
            .select(
                hash("SUN_POSTN", "MOON_POSTN").alias("sun_moon_sk"),
                col("SUN_POSTN").alias("sun_position"),
                col("MOON_POSTN").alias("moon_position"),
                col("MOON_ILLUM").alias("moon_illumination")
            )
            .dropDuplicates(["sun_moon_sk"])
    )


# -----------------------------------------------------------------------------
# 4. Fact Table: GCN Notices
# -----------------------------------------------------------------------------
@dlt.table(
    name="kafka_nasa_fermi_topic.gold.fact_gcn_notices",
    comment="Central fact table containing measurements and foreign keys to dimensions.",
    schema=schema_def.fact_gcn_notices_schema
)
def fact_gcn_notices():
    return (
        dlt.read_stream(SILVER_TABLE)
        .select(
            # Primary / Natural Keys
            col("TRIGGER_NUM").cast("bigint").alias("trigger_num"),
            col("RECORD_NUM").cast("int").alias("record_num"),
            
            # Kafka Metadata
            col("kafka_key"),
            col("kafka_topic"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_timestamp"),
            
            # Foreign Keys to Dimensions
            hash("GRB_DATE", "GRB_TIME").alias("timing_sk"),
            hash("GRB_RA", "GRB_DEC", "GAL_COORDS").alias("spatial_sk"),
            
            # Descriptive Attributes
            col("TITLE").alias("notice_title"),
            col("NOTICE_TYPE").alias("notice_type"),
            col("LOC_ALGORITHM").alias("loc_algorithm"),
            col("COMMENTS").alias("comments"),
            col("LC_URL").alias("lightcurve_url"),
            col("LOC_URL").alias("location_url"),
            
            # Facts / Measurements
            split(col("GRB_ERROR"), " ").getItem(0).cast("double").alias("grb_error_deg"),
            split(col("GRB_PHI"), " ").getItem(0).cast("double").alias("grb_phi_deg"),
            split(col("GRB_THETA"), " ").getItem(0).cast("double").alias("grb_theta_deg"),
            split(col("SUN_DIST"), " ").getItem(0).cast("double").alias("sun_dist_deg"),
            split(col("MOON_DIST"), " ").getItem(0).cast("double").alias("moon_dist_deg")
        )
    )