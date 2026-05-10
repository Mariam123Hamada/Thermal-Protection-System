"""
=============================================================
  Offline ML Anomaly Detection — Isolation Forest
  Reads from CSV (the batch output of feature engineering)
  Trains Isolation Forest → predicts anomalies → saves to HDFS
=============================================================
  Use this when you want ML-based detection (not just rules).
  Run AFTER feature engineering pipeline has produced CSV output.
=============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp
import pandas as pd
import numpy as np
import json
import logging
import os

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("IsolationForestDetection")

# ── Config ─────────────────────────────────────────────────
INPUT_CSV_PATH   = "Spark_output"           # from feature engineering notebook
HDFS_ALERTS_PATH = "hdfs://namenode:9000/sensor_project/ml_alerts"
MODEL_OUTPUT_DIR = "isolation_forest_model"

# Isolation Forest hyperparameters
CONTAMINATION    = 0.05   # expected anomaly fraction (5%)
N_ESTIMATORS     = 100
RANDOM_STATE     = 42


# ============================================================
# STEP 1: SPARK SESSION
# ============================================================

def create_spark_session():
    spark = (SparkSession.builder
             .appName("IsolationForestAnomalyDetection")
             .master("local[*]")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================
# STEP 2: LOAD FEATURE-ENGINEERED DATA
# ============================================================

def load_features(spark, path: str):
    """
    Load the CSV output from the feature engineering pipeline.
    Expected columns (from notebook):
      sensor_id, temperature, timestamp, event_time,
      prev_temp, temp_change, moving_avg_temp,
      max_recent_temp, min_recent_temp
    """
    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .csv(path))

    logger.info("✅ Loaded %d records from %s", df.count(), path)
    df.printSchema()
    return df


# ============================================================
# STEP 3: PREPARE FEATURES FOR ML
# ============================================================

FEATURE_COLS = [
    "temperature",
    "temp_change",
    "moving_avg_temp",
    "max_recent_temp",
    "min_recent_temp",
]


def prepare_ml_features(df):
    """
    Keep only numeric ML feature columns.
    Drop rows with nulls (first reading of each sensor has null lag).
    """
    feature_df = df.select(["sensor_id", "event_time"] + FEATURE_COLS)
    feature_df = feature_df.dropna(subset=FEATURE_COLS)
    logger.info("✅ Feature matrix prepared: %d rows, %d features",
                feature_df.count(), len(FEATURE_COLS))
    return feature_df


# ============================================================
# STEP 4: TRAIN ISOLATION FOREST (via Pandas)
# ============================================================

def train_isolation_forest(feature_df):
    """
    Convert Spark DataFrame to Pandas, train Isolation Forest.
    Returns (model, pandas_df_with_predictions).
    """
    try:
        from sklearn.ensemble import IsolationForest
        from sklearn.preprocessing import StandardScaler
    except ImportError:
        raise SystemExit(
            "❌ Install scikit-learn: pip install scikit-learn"
        )

    logger.info("⚙️  Converting to Pandas for model training ...")
    pdf = feature_df.toPandas()

    X = pdf[FEATURE_COLS].values

    # Normalize features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    logger.info("⚙️  Training Isolation Forest (n_estimators=%d, contamination=%.2f) ...",
                N_ESTIMATORS, CONTAMINATION)

    model = IsolationForest(
        n_estimators=N_ESTIMATORS,
        contamination=CONTAMINATION,
        random_state=RANDOM_STATE,
        n_jobs=-1,                    # use all CPU cores
    )
    model.fit(X_scaled)

    # Predict: -1 = anomaly, 1 = normal
    predictions  = model.predict(X_scaled)
    anomaly_scores = model.decision_function(X_scaled)  # lower = more anomalous

    pdf["ml_prediction"]   = predictions
    pdf["anomaly_score"]   = anomaly_scores
    pdf["is_ml_anomaly"]   = pdf["ml_prediction"] == -1

    n_anomalies = pdf["is_ml_anomaly"].sum()
    logger.info("✅ Training complete. Anomalies detected: %d / %d (%.1f%%)",
                n_anomalies, len(pdf), 100 * n_anomalies / len(pdf))

    return model, scaler, pdf


# ============================================================
# STEP 5: SAVE MODEL METADATA
# ============================================================

def save_model_info(model, scaler, feature_cols: list):
    """
    Save model parameters to JSON for reproducibility and reloading.
    """
    os.makedirs(MODEL_OUTPUT_DIR, exist_ok=True)
    info = {
        "model_type":       "IsolationForest",
        "n_estimators":     model.n_estimators,
        "contamination":    CONTAMINATION,
        "random_state":     RANDOM_STATE,
        "features":         feature_cols,
        "scaler_mean":      scaler.mean_.tolist(),
        "scaler_scale":     scaler.scale_.tolist(),
    }
    path = os.path.join(MODEL_OUTPUT_DIR, "model_info.json")
    with open(path, "w") as f:
        json.dump(info, f, indent=2)
    logger.info("✅ Model info saved: %s", path)


# ============================================================
# STEP 6: CONVERT RESULTS BACK TO SPARK + SHOW ALERTS
# ============================================================

def results_to_spark(spark, pdf):
    """
    Convert Pandas results back to Spark DataFrame.
    Assign human-readable severity based on anomaly score.
    """
    result_df = spark.createDataFrame(pdf)

    result_df = result_df.withColumn(
        "severity",
        when(col("anomaly_score") < -0.2, lit("HIGH"))
        .when(col("anomaly_score") < -0.05, lit("MEDIUM"))
        .when(col("is_ml_anomaly"), lit("LOW"))
        .otherwise(lit("OK"))
    ).withColumn("detection_method", lit("IsolationForest")
    ).withColumn("alert_timestamp",  current_timestamp())

    return result_df


# ============================================================
# STEP 7: WRITE ALERTS TO HDFS
# ============================================================

def write_alerts_to_hdfs(result_df):
    """
    Save anomaly records to HDFS partitioned by severity.
    """
    alerts = result_df.filter(col("is_ml_anomaly") == True)
    count  = alerts.count()

    if count > 0:
        (alerts.write
               .mode("append")
               .partitionBy("severity")
               .parquet(HDFS_ALERTS_PATH))
        logger.warning("🚨 %d ML anomaly alert(s) written to HDFS: %s",
                       count, HDFS_ALERTS_PATH)
        alerts.select(
            "sensor_id", "temperature", "anomaly_score",
            "severity", "alert_timestamp"
        ).orderBy("anomaly_score").show(50, truncate=False)
    else:
        logger.info("✅ No anomalies detected by Isolation Forest.")


# ============================================================
# STEP 8: MAIN
# ============================================================

def main():
    logger.info("=" * 60)
    logger.info("  Isolation Forest Anomaly Detection — STARTING")
    logger.info("=" * 60)

    spark = create_spark_session()

    # Load feature-engineered data
    raw_df       = load_features(spark, INPUT_CSV_PATH)
    feature_df   = prepare_ml_features(raw_df)

    # Train model and predict
    model, scaler, pdf_results = train_isolation_forest(feature_df)

    # Persist model metadata
    save_model_info(model, scaler, FEATURE_COLS)

    # Back to Spark for HDFS write
    result_df = results_to_spark(spark, pdf_results)

    # Write anomalies to HDFS
    write_alerts_to_hdfs(result_df)

    logger.info("=" * 60)
    logger.info("  Pipeline complete.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
