from pyspark import pipelines as dp
from pyspark.sql.functions import col, regexp_replace, expr, trim


@dp.table(
    name="kafka_nasa_fermi_topic.silver.FERMI_topic_cleaned_data_OBT",
    comment="Raw NASA GCN Fermi GBM Final Position events from Kafka"
)
def fermi_topic_cleaned_data_OBT():

    raw_df = spark.readStream.table("kafka_nasa_fermi_topic.bronze.FERMI_topic_raw")

    # 1. Clean raw_notice_text: normalize spaces and join multi-line entries
    cleaned_df = raw_df.withColumn(
        "cleaned_text",
        regexp_replace(col("kafka_value"), r"\u00A0", " ") # Remove non-breaking spaces
    ).withColumn(
        "single_line_text",
        regexp_replace(col("cleaned_text"), r"\n(?![A-Z_]+:)", " ") # Merge indented wrapped lines
    )

    # 2. Consolidate consecutive duplicate COMMENTS lines into a single COMMENTS key
    merged_comments_col = col("single_line_text")
    for _ in range(5):  # Safely merges up to 32 consecutive COMMENTS lines
        merged_comments_col = regexp_replace(
            merged_comments_col, 
            r"(\nCOMMENTS:[^\n]*)\nCOMMENTS:\s*", 
            "$1 "
        )

    cleaned_df = cleaned_df.withColumn("single_line_text", merged_comments_col)

    # 3. Convert single_line_text into a PySpark Map using str_to_map
    map_df = cleaned_df.withColumn(
        "data_map",
        expr("str_to_map(single_line_text, '\\n', ':')")
    )

    # 4. Target column list extracted from the GCN Notice body
    notice_columns = [
        "TITLE", "NOTICE_DATE", "NOTICE_TYPE", "RECORD_NUM", "TRIGGER_NUM",
        "GRB_RA", "GRB_DEC", "GRB_ERROR", "GRB_DATE", "GRB_TIME",
        "GRB_PHI", "GRB_THETA", "E_RANGE", "LOC_ALGORITHM",
        "SUN_POSTN", "SUN_DIST", "MOON_POSTN", "MOON_DIST", "MOON_ILLUM",
        "GAL_COORDS", "ECL_COORDS", "LC_URL", "LOC_URL", "COMMENTS"
    ]

    # 5. Extract map key-values into distinct columns and trim whitespace
    final_df = map_df
    for col_name in notice_columns:
        final_df = final_df.withColumn(
            col_name, 
            trim(col("data_map").getItem(col_name))
        )

    # 6. Select all Kafka metadata fields alongside the parsed body columns
    select_columns = [
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp"
    ] + notice_columns

    return final_df.select(select_columns)