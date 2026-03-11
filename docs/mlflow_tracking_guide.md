# MLflow Tracking Guide for Real-Time Market Intelligence

This document outlines how to integrate **MLflow** into the Market Intelligence Machine Learning lifecycle (Phase 33 onwards) for reliable model versioning, experiment tracking, and artifact management.

## 1. Core Concepts
*   **Tracking Server:** The central repository for all logged parameters, metrics, and models. You can run this locally or host it.
*   **Experiment:** A named group of related MLflow runs (e.g., `Price Forecasting - Onion`).
*   **Run:** A single execution of a training script. It logs metadata, metrics, and the model artifact itself.
*   **Model Registry:** The central store where you register trained models to manage their lifecycle stages (e.g., `Staging` → `Production`).

## 2. Infrastructure Setup
### Local Execution
1.  Install MLflow:
    ```bash
    pip install mlflow
    ```
2.  Start the MLflow tracking UI:
    ```bash
    mlflow ui --port 5000
    ```
    (The UI will be accessible at `http://localhost:5000`)
3.  Set the tracking URI in your scripts if needed (default is locally to `mlruns/` directory):
    ```python
    import mlflow
    mlflow.set_tracking_uri("http://localhost:5000")
    ```

## 3. How to Track Model Retraining
As per the architecture, the offline *batch retraining job* uses MLflow to log each new model.

```python
import mlflow
import mlflow.prophet  # or mlflow.sklearn, etc.
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error
from datetime import datetime

def train_prophet_model(df, product_id, pincode):
    # 1. Set the experiment name
    mlflow.set_experiment(f"Forecasting_{product_id}_{pincode}")

    # 2. Start an MLflow Run
    with mlflow.start_run(run_name=f"Retrain_{datetime.now().strftime('%Y%m%d')}"):
        
        # Log hyper-parameters
        changepoint_prior_scale = 0.05
        mlflow.log_param("changepoint_prior_scale", changepoint_prior_scale)
        mlflow.log_param("data_rows", len(df))
        
        # Train model
        m = Prophet(changepoint_prior_scale=changepoint_prior_scale)
        m.fit(df)
        
        # Validate model on a hold-out set (e.g. last 7 days)
        # (Assume validation_df is created prior)
        forecast = m.predict(validation_df)
        
        # Calculate Validation Metrics
        mae = mean_absolute_error(validation_df['y'], forecast['yhat'])
        mape = mean_absolute_percentage_error(validation_df['y'], forecast['yhat'])
        
        # Log Metrics
        mlflow.log_metric("validation_mae", mae)
        mlflow.log_metric("validation_mape", mape)
        
        # Log the actual Model Artifact
        # This serializes the Prophet model and saves it to the tracking server
        mlflow.prophet.log_model(m, "prophet_model")

        print(f"Model logged with MAE: {mae:.2f}")
```

## 4. The Promotion Rule (Staging vs. Production)
When the retraining job finishes, you don't immediately start using it for inference. You compare it against the *currently active production model*.

```python
from mlflow.tracking import MlflowClient

client = MlflowClient()
model_name = "Prophet_Onion"

# Fetch the current production model's latest MAE
prod_versions = client.get_latest_versions(model_name, stages=["Production"])
if prod_versions:
    prod_version = prod_versions[0]
    prod_run_id = prod_version.run_id
    prod_run = client.get_run(prod_run_id)
    prod_mae = prod_run.data.metrics.get("validation_mae", float('inf'))
else:
    prod_mae = float('inf')

# Compare with the new model we just trained
new_mae = mae # From the training step above

if new_mae < prod_mae:
    print("New model improves MAE. Promoting to Production.")
    # Register the model
    model_version_info = mlflow.register_model(
        model_uri=f"runs:/{mlflow.active_run().info.run_id}/prophet_model", 
        name=model_name
    )
    # Transition stage to Production
    client.transition_model_version_stage(
        name=model_name,
        version=model_version_info.version,
        stage="Production",
        archive_existing_versions=True
    )
else:
    print("New model underperforms. Discarding.")
```

## 5. Continuous Live Inference
The *Inference Pipeline* (which runs right after Spark inserts `fact_pricing_snapshots`) dynamically loads the absolute latest `Production` model from the registry.

```python
import mlflow.prophet

# 1. Provide the URI specifying we want the Production version
model_name = "Prophet_Onion"
model_uri = f"models:/{model_name}/Production"

# 2. Load the model from MLflow
loaded_model = mlflow.prophet.load_model(model_uri)

# 3. Create a future dataframe and predict
# (For live inference, this takes the new fact_pricing_snapshots rows)
future_df = ... 
forecast = loaded_model.predict(future_df)

# 4. Extract the 7-day array and insert into `ml_predictions`
predicted_price_array = forecast['yhat'].tail(7).tolist()
# Execute INSERT statement to PostgreSQL Star Schema
```

## 6. Daily Operations & Auditing
With MLflow integrated:
*   **Auditability:** If the LLM generates a strange alert, you can query PostgreSQL's `ml_predictions.model_version`, then open the MLflow UI and view the exact parameters, data split, and training metrics for that version.
*   **Rollbacks:** If a production model deteriorates gracefully, you can use the MLflow UI or SDK to transition a previous version back to `Production`.
*   **Extensibility:** This exact same pattern applies to `Isolation Forest` (Anomaly Detection) logging `Precision/Recall`, and linear regression for `Elasticity` logging `$R^2$`.
