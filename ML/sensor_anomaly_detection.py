"""
=============================================================
  Sensor Anomaly Detection & Real-Time Alert System
  Built on: Apache Kafka + Spark Streaming + HDFS
=============================================================
  Architecture:
    Kafka (Ingestion) → Spark Streaming (Processing)
    → Rule Engine (AI/ML) → Alert + HDFS Storage
=============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max, min, lag, to_timestamp,
    current_timestamp, lit, when, udf, struct, to_json
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, TimestampType, BooleanType, IntegerType
)
from pyspark.sql.window import Window
import json
import logging

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SensorAnomalyDetection")

# ============================================================
# STEP 1: CONFIGURATION
# ============================================================

KAFKA_BROKER       = "localhost:9092"
KAFKA_TOPIC_INPUT  = "sensor-readings"
KAFKA_TOPIC_ALERTS = "sensor-alerts"

HDFS_BASE_PATH     = "hdfs://namenode:9000/sensor_project"
HDFS_EVENTS_PATH   = f"{HDFS_BASE_PATH}/events"
HDFS_ALERTS_PATH   = f"{HDFS_BASE_PATH}/alerts"
HDFS_CHECKPOINT    = f"{HDFS_BASE_PATH}/checkpoints/streaming"

# ── Anomaly Thresholds (Rule-Based AI) ───────────────────
THRESHOLDS = {
    "max_temperature":        150.0,   # °C — critical high
    "min_temperature":        10.0,    # °C — critical low
    "max_temp_change":        30.0,    # ΔT per reading — sudden spike
    "min_temp_change":       -30.0,    # ΔT per reading — sudden drop
    "max_moving_avg":         130.0,   # sustained high average
    "alert_severity_high":    120.0,   # triggers HIGH alert
    "alert_severity_medium":   90.0,   # triggers MEDIUM alert
}


# ============================================================
# STEP 2: CREATE SPARK SESSION
# ============================================================

def create_spark_session():
    """
    Initialize Spark Session with:
    - Kafka connector for streaming ingestion
    - HDFS write support
    """
    spark = (SparkSession.builder
             .appName("SensorAnomalyDetection")
             .master("local[*]")

             # Kafka connector
             .config("spark.jars.packages",
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")

             # Streaming tuning
             .config("spark.streaming.stopGracefullyOnShutdown", "true")
             .config("spark.sql.shuffle.partitions", "4")

             # HDFS / Hadoop integration
             .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")

             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")
    logger.info("✅ Spark Session created successfully.")
    return spark


# ============================================================
# STEP 3: DEFINE SCHEMA FOR INCOMING KAFKA MESSAGES
# ============================================================

SENSOR_SCHEMA = StructType([
    StructField("sensor_id",   StringType(),    nullable=False),
    StructField("temperature", DoubleType(),     nullable=False),
    StructField("timestamp",   StringType(),     nullable=True),
])


# ============================================================
# STEP 4: DATA INGESTION FROM KAFKA
# ============================================================

def read_from_kafka(spark):
    """
    Connect to Apache Kafka topic and read streaming sensor data.
    Each Kafka message value is expected to be a JSON string like:
      {"sensor_id": "SENSOR_1", "temperature": 93.21, "timestamp": "2026-05-04 17:56:43"}
    """
    raw_stream = (spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", KAFKA_BROKER)
                  .option("subscribe", KAFKA_TOPIC_INPUT)
                  .option("startingOffsets", "latest")
                  .option("failOnDataLoss", "false")
                  .load())

    # Parse JSON payload from Kafka value (binary → string → struct)
    from pyspark.sql.functions import from_json
    parsed = (raw_stream
              .selectExpr("CAST(value AS STRING) as json_str",
                          "timestamp as kafka_time")
              .select(from_json(col("json_str"), SENSOR_SCHEMA).alias("data"),
                      col("kafka_time"))
              .select("data.*", "kafka_time"))

    logger.info("✅ Kafka stream connected to topic: %s", KAFKA_TOPIC_INPUT)
    return parsed


# ============================================================
# STEP 5: DATA QUALITY FILTER
# ============================================================

def filter_invalid_data(df):
    """
    Remove readings that are:
      - null temperature
      - temperature <= 0  (sensor offline / error)
      - temperature >= 400 (physically impossible)
    """
    clean = df.filter(
        col("temperature").isNotNull() &
        (col("temperature") > 0) &
        (col("temperature") < 400)
    ).withColumn(
        "event_time",
        to_timestamp(col("timestamp"))
    )
    logger.info("✅ Data quality filter applied.")
    return clean


# ============================================================
# STEP 6: FEATURE ENGINEERING (Window Functions)
# ============================================================

def engineer_features(df):
    """
    Build ML-ready features using Spark Window functions.
    Matches the batch pipeline in spark_feature_engineering_pipeline.ipynb.

    Features produced:
      - prev_temp        : previous reading (lag-1)
      - temp_change      : ΔT = current − previous
      - moving_avg_temp  : 3-reading rolling average
      - max_recent_temp  : max of last 3 readings
      - min_recent_temp  : min of last 3 readings
    """
    window_spec   = Window.partitionBy("sensor_id").orderBy("event_time")
    recent_window = window_spec.rowsBetween(-2, 0)

    df = (df
          .withColumn("prev_temp",        lag("temperature", 1).over(window_spec))
          .withColumn("temp_change",       col("temperature") - col("prev_temp"))
          .withColumn("moving_avg_temp",   avg("temperature").over(recent_window))
          .withColumn("max_recent_temp",   max("temperature").over(recent_window))
          .withColumn("min_recent_temp",   min("temperature").over(recent_window)))

    logger.info("✅ Feature engineering completed.")
    return df


# ============================================================
# STEP 7: RULE-BASED ANOMALY DETECTION (AI / Alert Engine)
# ============================================================

def detect_anomalies(df):
    """
    Compare engineered features against defined thresholds.
    Produces:
      - is_anomaly     : boolean flag
      - alert_type     : description of the rule triggered
      - severity       : HIGH / MEDIUM / LOW
    """
    T = THRESHOLDS

    df = df.withColumn(
        "alert_type",
        when(col("temperature") >= T["max_temperature"],
             lit("CRITICAL_HIGH_TEMP"))
        .when(col("temperature") <= T["min_temperature"],
             lit("CRITICAL_LOW_TEMP"))
        .when(col("temp_change") >= T["max_temp_change"],
             lit("SUDDEN_SPIKE"))
        .when(col("temp_change") <= T["min_temp_change"],
             lit("SUDDEN_DROP"))
        .when(col("moving_avg_temp") >= T["max_moving_avg"],
             lit("SUSTAINED_HIGH_AVG"))
        .otherwise(lit("NORMAL"))
    ).withColumn(
        "is_anomaly",
        col("alert_type") != lit("NORMAL")
    ).withColumn(
        "severity",
        when(col("temperature") >= T["alert_severity_high"],
             lit("HIGH"))
        .when(col("temperature") >= T["alert_severity_medium"],
             lit("MEDIUM"))
        .when(col("is_anomaly"),
             lit("LOW"))
        .otherwise(lit("OK"))
    ).withColumn(
        "alert_timestamp",
        current_timestamp()
    )

    logger.info("✅ Anomaly detection rules applied.")
    return df


# ============================================================
# STEP 8: WRITE ALL EVENTS TO HDFS (Storage Layer)
# ============================================================

def write_events_to_hdfs(df, batch_id):
    """
    Persist every processed record (normal + anomaly) to HDFS
    in Parquet format, partitioned by date for efficient querying.
    """
    (df.withColumn("processing_date",
                   col("alert_timestamp").cast("date"))
       .write
       .mode("append")
       .partitionBy("processing_date")
       .parquet(HDFS_EVENTS_PATH))

    logger.info("[Batch %d] ✅ Events written to HDFS: %s",
                batch_id, HDFS_EVENTS_PATH)


# ============================================================
# STEP 9: WRITE ALERTS TO HDFS  (Alert Log)
# ============================================================

def write_alerts_to_hdfs(df, batch_id):
    """
    Filter and persist only anomaly records to a dedicated
    HDFS alerts directory for post-incident analysis.
    """
    alerts = df.filter(col("is_anomaly") == True)

    count = alerts.count()
    if count > 0:
        (alerts.write
               .mode("append")
               .partitionBy("severity")
               .parquet(HDFS_ALERTS_PATH))

        logger.warning("[Batch %d] 🚨 %d ALERT(S) written to HDFS: %s",
                       batch_id, count, HDFS_ALERTS_PATH)

        # Print alert details to console for immediate visibility
        alerts.select(
            "sensor_id", "temperature", "alert_type",
            "severity", "alert_timestamp"
        ).show(truncate=False)
    else:
        logger.info("[Batch %d] ✅ No anomalies detected.", batch_id)


# ============================================================
# STEP 10: FOREACH BATCH HANDLER
# ============================================================

def process_batch(batch_df, batch_id):
    """
    Called by Spark Streaming for each micro-batch.
    Applies feature engineering + anomaly detection + storage.
    """
    if batch_df.isEmpty():
        logger.info("[Batch %d] Empty batch, skipping.", batch_id)
        return

    logger.info("[Batch %d] Processing %d records ...",
                batch_id, batch_df.count())

    # Feature Engineering
    enriched = engineer_features(batch_df)

    # Anomaly Detection
    result = detect_anomalies(enriched)

    # Persist to HDFS
    write_events_to_hdfs(result, batch_id)
    write_alerts_to_hdfs(result, batch_id)


# ============================================================
# STEP 11: MAIN — WIRE EVERYTHING TOGETHER
# ============================================================

def main():
    logger.info("=" * 60)
    logger.info("  Sensor Anomaly Detection Pipeline — STARTING")
    logger.info("=" * 60)

    spark = create_spark_session()

    # Ingest from Kafka
    raw_stream = read_from_kafka(spark)

    # Apply data quality filter
    clean_stream = filter_invalid_data(raw_stream)

    # Stream processing with foreachBatch
    query = (clean_stream.writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", HDFS_CHECKPOINT)
             .trigger(processingTime="10 seconds")   # micro-batch every 10s
             .start())

    logger.info("✅ Streaming query started. Awaiting data from Kafka ...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
