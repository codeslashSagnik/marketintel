from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, abs as spark_abs, pandas_udf, dayofweek, expr
from pyspark.sql.types import DoubleType, StructType, StructField, BooleanType, DataType, DateType
import pandas as pd
import numpy as np
import mlflow

def apply_data_quality_rules(batch_df: DataFrame, stats_df: DataFrame) -> DataFrame:
    """
    Model 5 - Data Quality Guard
    Applies Hard Rules (reject) and Statistical Rules (flag) to incoming micro-batches.
    """
    joined = batch_df.join(
        stats_df.select(
            col("stat_product_id"), 
            col("stat_pincode"), 
            col("avg_price"), 
            col("stddev_price")
        ),
        on=[
            batch_df.product_id == col("stat_product_id"), 
            batch_df.pincode == col("stat_pincode")
        ],
        how="left"
    ).drop("stat_product_id", "stat_pincode")

    # Compute derived features for rules
    joined = joined.withColumn(
        "implied_discount",
        when((col("mrp").isNotNull()) & (col("mrp") > 0), 
             ((col("mrp") - col("selling_price")) / col("mrp")) * 100
        ).otherwise(lit(None).cast("double"))
    )

    # Apply Hard and Statistical Rules
    flagged = joined.withColumn(
        "quality_flag",
        when(col("selling_price").isNull() | (col("selling_price") <= 0), lit("rejected"))
        .when((col("mrp").isNotNull()) & (col("mrp") > 0) & (col("selling_price") > col("mrp") * 1.5), lit("rejected"))
        .when(col("price_change_pct").isNotNull() & (spark_abs(col("price_change_pct")) > 80.0), lit("rejected"))
        .when(col("avg_price").isNotNull() & col("stddev_price").isNotNull() & (col("stddev_price") > 0) & 
             ((spark_abs(col("selling_price") - col("avg_price")) / col("stddev_price")) > 4.0), lit("flagged"))
        .when(col("implied_discount").isNotNull() & col("discount_pct").isNotNull() & 
             (spark_abs(col("implied_discount") - col("discount_pct")) > 2.0), lit("flagged"))
        .otherwise(lit("clean"))
    )

    # Attach Rejection/Flag Reasons for Audit Logs
    flagged = flagged.withColumn(
        "rejection_reason",
        when(col("selling_price").isNull() | (col("selling_price") <= 0), lit("null_or_zero_price"))
        .when((col("mrp").isNotNull()) & (col("mrp") > 0) & (col("selling_price") > col("mrp") * 1.5), lit("above_mrp_markup"))
        .when(col("price_change_pct").isNotNull() & (spark_abs(col("price_change_pct")) > 80.0), lit("streaming_price_swing_>80%"))
        .when(col("avg_price").isNotNull() & col("stddev_price").isNotNull() & (col("stddev_price") > 0) & 
             ((spark_abs(col("selling_price") - col("avg_price")) / col("stddev_price")) > 4.0), lit("statistical_outlier_>4sigma"))
        .when(col("implied_discount").isNotNull() & col("discount_pct").isNotNull() & 
             (spark_abs(col("implied_discount") - col("discount_pct")) > 2.0), lit("discount_inconsistency"))
        .otherwise(lit(None).cast("string"))
    )

    return flagged.drop("avg_price", "stddev_price", "implied_discount")


# Setup MLflow UDF for parallel Spark Inference

# Setup MLflow UDF for parallel Spark Inference
@pandas_udf(DoubleType())
def isolation_forest_predict_udf(price_to_mrp_ratio: pd.Series, discount_pct: pd.Series, 
                                 price_change_pct: pd.Series, in_stock: pd.Series, 
                                 day_of_week: pd.Series) -> pd.Series:
    """
    Pandas UDF that loads the MLflow Isolation Forest Model to score anomalies. 
    Lower score = more anomalous.
    """
    mlflow.set_tracking_uri("sqlite:///mlflow.db")
    
    # Load the latest Isolation Forest model from the experiment
    try:
        # We find the latest run with the model
        client = mlflow.tracking.MlflowClient("sqlite:///mlflow.db")
        experiment = client.get_experiment_by_name("Anomaly_Detection")
        
        if not experiment:
            return pd.Series(np.zeros(len(price_to_mrp_ratio)))
            
        runs = client.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["start_time DESC"],
            max_results=1
        )
        
        if not runs:
             return pd.Series(np.zeros(len(price_to_mrp_ratio)))
             
        run_id = runs[0].info.run_id
        model_uri = f"runs:/{run_id}/isolation_forest_model"
        
        model = mlflow.sklearn.load_model(model_uri)
        
        # Build features dataframe matching training schema
        X_infer = pd.DataFrame({
            'price_to_mrp_ratio': price_to_mrp_ratio.fillna(1.0),
            'discount_pct': discount_pct.fillna(0.0),
            'price_change_pct': price_change_pct.fillna(0.0),
            'in_stock': in_stock.fillna(True).astype(int),
            'day_of_week': day_of_week.fillna(0).astype(int)
        })
        
        # return anomaly score: standard is higher = normal, lower = anomalous
        # We invert it so higher = anomalous (0.0 to 1.0 proxy)
        scores = model.decision_function(X_infer)
        inverted_scores = -1 * scores # Now positive is weird
        
        return pd.Series(inverted_scores)

    except Exception as e:
        # Fallback if model fails to load
        return pd.Series(np.zeros(len(price_to_mrp_ratio)))


def detect_streaming_anomalies(df: DataFrame, stats_df: DataFrame) -> DataFrame:
    """
    Model 2 - Streaming Anomaly Detection
    Executes the trained Isolation Forest via Pandas UDF on the streaming data.
    """
    
    anom = df.withColumn(
        "price_to_mrp_ratio",
        when((col("mrp").isNotNull()) & (col("mrp") > 0), col("selling_price") / col("mrp")).otherwise(lit(1.0))
    ).withColumn(
        "day_of_week", dayofweek(col("scraped_at"))
    )

    # 1. Point Anomaly via MLflow UDF
    anom = anom.withColumn(
        "raw_if_score",
        isolation_forest_predict_udf(
            col("price_to_mrp_ratio"),
            col("discount_pct"),
            col("price_change_pct"),
            col("in_stock"),
            col("day_of_week")
        )
    )

    # Normalize Point Anomaly Score (Sigmoid proxy or threshold clamp)
    anom = anom.withColumn(
        "point_anomaly_score",
        when(col("raw_if_score") > 0.1, lit(1.0)) # Deep anomaly
        .when(col("raw_if_score") < -0.1, lit(0.0)) # Very normal
        .otherwise(spark_abs(col("raw_if_score")) * 10) # Transitional 
    )

    # 2. CUSUM Trend Anomaly proxy score (Cumulative Sum of deviations)
    # We calculate the naive z-score (distance from mean) and use it as the CUSUM step.
    # A true stateful CUSUM requires mapGroupsWithState. For this micro-batch proxy,
    # we use the raw deviation scaled.
    anom = anom.withColumn(
        "z_score_deviation",
        when(col("avg_price").isNotNull() & col("stddev_price").isNotNull() & (col("stddev_price") > 0),
             (col("selling_price") - col("avg_price")) / col("stddev_price")
        ).otherwise(lit(0.0).cast("double"))
    )
    
    anom = anom.withColumn(
        "trend_anomaly_score",
        when(spark_abs(col("z_score_deviation")) > 3.0, lit(1.0))
        .otherwise(spark_abs(col("z_score_deviation")) / 3.0)
    )

    # Determine explicit anomaly boolean (Flags if Isolation Forest caught it OR CUSUM trend is extreme)
    anom = anom.withColumn(
        "is_anomaly",
        when(col("raw_if_score") > 0, lit(True))
        .when(col("trend_anomaly_score") >= 0.9, lit(True))
        .otherwise(lit(False))
    )

    anom = anom.withColumn(
        "anomaly_type",
        when(col("is_anomaly"), lit("isolation_forest_point")).otherwise(lit(None).cast("string"))
    ).withColumn(
        "model_type",
        lit("isolation_forest_v1")
    )

    return anom.drop("price_to_mrp_ratio", "day_of_week", "raw_if_score")
