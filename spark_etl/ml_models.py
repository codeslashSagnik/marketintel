from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, abs as spark_abs

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

    # Apply Hard and Statistical Rules
    flagged = joined.withColumn(
        "quality_flag",
        when(col("selling_price") <= 0, lit("rejected"))
        .when((col("mrp").isNotNull()) & (col("mrp") > 0) & (col("selling_price") > col("mrp") * 1.5), lit("rejected"))
        .when(col("price_change_pct").isNotNull() & (spark_abs(col("price_change_pct")) > 80.0), lit("rejected"))
        .when(col("avg_price").isNotNull() & col("stddev_price").isNotNull() & (col("stddev_price") > 0) & 
             ((spark_abs(col("selling_price") - col("avg_price")) / col("stddev_price")) > 4.0), lit("flagged"))
        .otherwise(lit("clean"))
    )

    # Attach Rejection/Flag Reasons for Audit Logs
    flagged = flagged.withColumn(
        "rejection_reason",
        when(col("selling_price") <= 0, lit("Price <= 0"))
        .when((col("mrp").isNotNull()) & (col("mrp") > 0) & (col("selling_price") > col("mrp") * 1.5), lit("Price > 1.5x MRP"))
        .when(col("price_change_pct").isNotNull() & (spark_abs(col("price_change_pct")) > 80.0), lit("Price swing > 80%"))
        .when(col("quality_flag") == "flagged", lit(">4 StdDev from 30d mean"))
        .otherwise(lit(None).cast("string"))
    )

    return flagged.drop("avg_price", "stddev_price")


def detect_streaming_anomalies(df: DataFrame, stats_df: DataFrame) -> DataFrame:
    """
    Model 2 - Streaming Anomaly Detection
    Computes point anomalies using a Z-score distribution proxy for the stream.
    """
    joined = df.join(
        stats_df.select(
            col("stat_product_id"), 
            col("stat_pincode"), 
            col("avg_price"), 
            col("stddev_price")
        ),
        on=[
            df.product_id == col("stat_product_id"), 
            df.pincode == col("stat_pincode")
        ],
        how="left"
    ).drop("stat_product_id", "stat_pincode")

    # Compute Streaming Z-Score
    anom = joined.withColumn(
        "z_score",
        when(col("avg_price").isNotNull() & col("stddev_price").isNotNull() & (col("stddev_price") > 0),
             spark_abs(col("selling_price") - col("avg_price")) / col("stddev_price")
        ).otherwise(lit(0.0).cast("double"))
    )

    # Normalize Point Anomaly Score (Capping at 5 StdDevs)
    anom = anom.withColumn(
        "point_anomaly_score",
        when(col("z_score") > 5.0, lit(1.0))
        .otherwise(col("z_score") / 5.0)
    )
    
    # Placeholder for rolling multi-day streaming CUSUM
    anom = anom.withColumn("trend_anomaly_score", lit(0.0).cast("double"))

    # Determine explicit anomaly boolean (Score >= 0.8 represents ~4+ StdDev event threshold)
    anom = anom.withColumn(
        "is_anomaly",
        when(col("point_anomaly_score") >= 0.8, lit(True)).otherwise(lit(False))
    )

    anom = anom.withColumn(
        "anomaly_type",
        when(col("is_anomaly"), lit("point")).otherwise(lit(None).cast("string"))
    )

    return anom.drop("avg_price", "stddev_price", "z_score")
