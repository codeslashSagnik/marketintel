"""
Model 5: Data Quality Gatekeeper
================================

This module implements the first line of defense in the ML pipeline.
It applies hard physical rules and statistical checks to flag or reject bad scraped data
before it enters the predictive models. This script acts as a standalone spark transformer
or a batch validator.
"""

import sys
import logging
from typing import Dict, Any, List

import pandas as pd
import numpy as np

# MLflow for tracking DQ model metrics
import mlflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
logger = logging.getLogger("ml.data_quality")

class DataQualityGatekeeper:
    """
    Applies Data Quality rules to incoming price records.
    """
    def __init__(self, tracking_uri: str = "sqlite:///mlflow.db"):
        self.tracking_uri = tracking_uri
        mlflow.set_tracking_uri(self.tracking_uri)
        mlflow.set_experiment("Data_Quality_Gatekeeper")
        
        # Thresholds (Configurable)
        self.thresholds = {
            "max_mrp_markup": 1.5,      # Selling price cannot exceed MRP * 1.5
            "max_daily_change_pct": {
                "packaged_goods": 0.15,
                "vegetables": 0.50,
                "dairy": 0.20
            },
            "sigma_outlier": 4.0        # Flag if 4 std deviations from the 30-day mean
        }

    def _check_hard_rules(self, row: pd.Series) -> Dict[str, Any]:
        """
        Hard rules result in IMMEDIATE REJECTION.
        Checks: negative price, null stock, price > 1.5x MRP.
        """
        selling_price = row.get("selling_price")
        mrp = row.get("mrp")
        in_stock = row.get("in_stock")

        # 1. Null criticals
        if pd.isna(selling_price):
            return {"status": "rejected", "reason": "null_selling_price"}
        
        # 2. Impossible values
        if selling_price <= 0:
            return {"status": "rejected", "reason": "zero_or_negative_price"}
        
        # 3. Legal bounds
        if mrp and not pd.isna(mrp) and selling_price > (mrp * self.thresholds["max_mrp_markup"]):
            return {"status": "rejected", "reason": "above_mrp_markup"}

        return {"status": "clean"}

    def _check_statistical_flags(self, row: pd.Series, rolling_stats: Dict[str, float]) -> Dict[str, Any]:
        """
        Statistical rules result in FLAGGING (downweighting), but not rejection.
        """
        selling_price = row["selling_price"]
        mean_30d = rolling_stats.get("mean_30d")
        std_30d = rolling_stats.get("std_30d")

        if mean_30d is not None and std_30d is not None and std_30d > 0:
            z_score = abs(selling_price - mean_30d) / std_30d
            if z_score > self.thresholds["sigma_outlier"]:
                return {
                    "status": "flagged", 
                    "reason": f"statistical_outlier (z={z_score:.2f})",
                    "flagged_values": {"selling_price": selling_price, "mean_30d": mean_30d}
                }

        # Check discount inconsistency
        mrp = row.get("mrp")
        reported_discount = row.get("discount_pct")
        
        if mrp and not pd.isna(mrp) and mrp > 0 and reported_discount and not pd.isna(reported_discount):
            implied_discount = ((mrp - selling_price) / mrp) * 100
            if abs(implied_discount - reported_discount) > 2.0:  # 2% tolerance
                return {
                    "status": "flagged",
                    "reason": "discount_inconsistency",
                    "flagged_values": {"implied": implied_discount, "reported": reported_discount}
                }

        return {"status": "clean"}

    def evaluate_batch(self, df: pd.DataFrame, historical_stats_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Evaluate a batch of records. Returns the dataframe with `data_quality_flag` 
        and `rejection_reason` appended.
        """
        logger.info(f"Evaluating {len(df)} records through DQ Gatekeeper...")
        results = []
        
        stats_lookup = {}
        if historical_stats_df is not None and not historical_stats_df.empty:
            historical_stats_df["lookup_key"] = historical_stats_df["product_id"] + "_" + historical_stats_df["pincode"]
            stats_lookup = historical_stats_df.set_index("lookup_key").to_dict("index")

        rejected_count = 0
        flagged_count = 0

        for idx, row in df.iterrows():
            # 1. Hard Rules
            hard_check = self._check_hard_rules(row)
            if hard_check["status"] == "rejected":
                results.append({"data_quality_flag": "rejected", "rejection_reason": hard_check["reason"]})
                rejected_count += 1
                continue

            # 2. Statistical Rules
            stats = {}
            lookup_key = f"{row.get('product_id', '')}_{row.get('pincode', '')}"
            if lookup_key in stats_lookup:
                stats = stats_lookup[lookup_key]

            stat_check = self._check_statistical_flags(row, stats)
            if stat_check["status"] == "flagged":
                results.append({"data_quality_flag": "flagged", "rejection_reason": stat_check["reason"]})
                flagged_count += 1
                continue
            
            # 3. Clean
            results.append({"data_quality_flag": "clean", "rejection_reason": None})

        # Attach results
        result_df = pd.DataFrame(results)
        df["data_quality_flag"] = result_df["data_quality_flag"].values
        df["data_quality_rejection_reason"] = result_df["rejection_reason"].values

        clean_count = len(df) - rejected_count - flagged_count
        
        # Log to MLflow
        try:
            with mlflow.start_run(run_name="daily_dq_check"):
                mlflow.log_metric("total_rows_processed", len(df))
                mlflow.log_metric("clean_rate", clean_count / len(df) if len(df) > 0 else 0)
                mlflow.log_metric("rejected_rate", rejected_count / len(df) if len(df) > 0 else 0)
                mlflow.log_metric("flagged_rate", flagged_count / len(df) if len(df) > 0 else 0)
        except Exception as e:
            logger.warning(f"Failed to log metrics to MLflow: {e}")

        logger.info(f"DQ Complete: {clean_count} Clean | {flagged_count} Flagged | {rejected_count} Rejected")
        return df

if __name__ == "__main__":
    # Simple test case for validation
    gatekeeper = DataQualityGatekeeper()
    
    test_data = pd.DataFrame([
        {"product_id": "p1", "pincode": "700001", "selling_price": 50.0, "mrp": 60.0, "in_stock": True, "discount_pct": 16.66}, # Clean
        {"product_id": "p2", "pincode": "700001", "selling_price": 0.0, "mrp": 100.0, "in_stock": True, "discount_pct": 0},    # Rejected (0)
        {"product_id": "p3", "pincode": "700001", "selling_price": 200.0, "mrp": 50.0, "in_stock": True, "discount_pct": 0},   # Rejected (> MRP)
        {"product_id": "p4", "pincode": "700001", "selling_price": 50.0, "mrp": 100.0, "in_stock": True, "discount_pct": 10.0} # Flagged (discount inconsistency)
    ])
    
    historical_stats = pd.DataFrame([
        {"product_id": "p1", "pincode": "700001", "mean_30d": 52.0, "std_30d": 5.0} # Normal
    ])

    out_df = gatekeeper.evaluate_batch(test_data, historical_stats)
    print(out_df[["product_id", "selling_price", "data_quality_flag", "data_quality_rejection_reason"]])
