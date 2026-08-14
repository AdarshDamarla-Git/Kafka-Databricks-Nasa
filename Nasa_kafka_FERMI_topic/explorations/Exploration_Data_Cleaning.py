# Databricks notebook source
from pyspark.sql.functions import col, regexp_replace, expr, trim

columns = [
        "TITLE", "NOTICE_DATE", "NOTICE_TYPE", "RECORD_NUM", "TRIGGER_NUM",
        "GRB_RA", "GRB_DEC", "GRB_ERROR", "GRB_DATE", "GRB_TIME",
        "GRB_PHI", "GRB_THETA", "E_RANGE", "LOC_ALGORITHM",
        "SUN_POSTN", "SUN_DIST", "MOON_POSTN", "MOON_DIST", "MOON_ILLUM",
        "GAL_COORDS", "ECL_COORDS", "LC_URL", "LOC_URL", "COMMENTS"
    ]

df = spark.readStream.table("kafka_nasa_fermi_topic.bronze.FERMI_topic_raw")
df = df.withColumn(
            "cleaned_text",
            regexp_replace(col("value"), r"\u00A0", " ")
        ).withColumn(
            "merged_continuation",
            regexp_replace(col("cleaned_text"), r"\n(?![A-Z_]+:)", " ") 
        ).withColumn(
            "single_line_text",
            regexp_replace(col("merged_continuation"), r"\nCOMMENTS:\s*", " ") 
        ).withColumn(
            "data_map",
            expr("str_to_map(single_line_text, '\\n', ':')")
        )
    
for col_name in columns:
    df = df.withColumn(col_name, trim(col("data_map").getItem(col_name)))

display(df.select(columns), checkpointLocation = "/Volumes/kafka_nasa_test/fermi_topic/dump_data_volume")