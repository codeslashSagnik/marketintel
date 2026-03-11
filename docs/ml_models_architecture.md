# The 7-Model Market Intelligence Architecture

Before picking algorithms or generating text, the platform must solve 8 distinct mathematical and logical problems. This document defines the inputs, algorithms, outputs, and cadence for each of the 7 core Machine Learning models.

---

## 🛡️ Model 5: Data Quality Detection (The Gatekeeper)
**Goal:** Is this scrape reliable or did something go wrong?
**Why:** Garbage data corrupts forecasting baselines and triggers false anomalies. This must run *before* anything else.
*   **Approach:** Hybrid Rule-based + Statistical.
    *   *Hard Rules (Reject):* Price <= 0, Price > 1.5x MRP, >80% swing for stable category.
    *   *Statistical Rules (Flag for review):* >4 std deviations from 30-day mean, zero price movement for 14+ days.
*   **Output:** `data_quality_flag` (`clean`, `flagged`, `rejected`) appended to `fact_pricing_snapshots`.
*   **Cadence:** Streaming (07:30 AM).

---

## 🚨 Model 2: Anomaly Detection
**Goal:** Is something weird happening to this price right now?
**Why:** Detects sudden supply shocks or pricing errors. Needs point (single day) and trend (multi-day) detection.
*   **Approach:** 
    *   *Point Anomaly:* `Isolation Forest` (Unsupervised, isolates extreme variations).
    *   *Trend Anomaly:* `CUSUM` (Accumulates minor daily deviations until a threshold is breached).
*   **Output:** `is_anomaly`, `point_anomaly_score`, `trend_anomaly_score` written to `ml_predictions`.
*   **Cadence:** Streaming (07:35 AM on `clean` rows). Triggers Kafka `alerts.price_drops` if `>10%`.

---

## 📈 Model 1: Price Forecasting
**Goal:** What will this product cost tomorrow / next week?
**Why:** Enables buy/wait recommendations. Prices have weekly seasonality and holiday spikes.
*   **Features:** `selling_price`, `discount_pct`, `in_stock`, calendar features, weather (lagged).
*   **Approach:** 
    *   `Prophet`: For seasonal items with 90+ days history (e.g., Vegetables).
    *   `LightGBM`: For stable packaged goods with step-changes.
*   **Output:** 7-day predicted price array & confidence intervals written to `ml_predictions`.
*   **Cadence:** 
    *   *Inference:* Daily batch (07:45 AM).
    *   *Retraining:* Weekly (Sun 02:00 AM) with MLflow Registry tracking MAE/MAPE.

---

## 🌦️ Model 3: Weather–Price Correlation
**Goal:** Does weather actually move this product's price?
**Why:** Explanatory model. Gives the LLM causal evidence for anomalies.
*   **Approach:** Lagged cross-correlation (Pearson `r`). Tests lags of 0, 1, 2, 3 days to find the highest correlation between `temp_max_c`/`precipitation_mm` and `selling_price`.
*   **Output:** `correlation_r`, `optimal_lag_days` written to `weather_correlation_coefficients`.
*   **Cadence:** Monthly batch (structural relationships change slowly).

---

## 📉 Model 4: Price Elasticity Estimation
**Goal:** If price drops X%, does demand actually respond?
**Why:** Determines if a discount is actually moving product.
*   **Approach:** Proxy elasticity. Since we lack transaction data, we measure how fast a product goes `out_of_stock` following a steep `discount_pct` drop. Starts with Bayesian priors from academic literature.
*   **Output:** `elasticity_index` (-3 highly elastic to 0 inelastic).
*   **Cadence:** Monthly batch.

---

## 👤 Model 6: User Interest Profiling
**Goal:** What does this user care about?
**Why:** Personalization without collaborative filtering.
*   **Approach:** Non-ML continuous aggregation from `user_search_log`. Uses exponential time decay ($\lambda = 0.1$) to weight recent searches higher. Applies intent weighting (`buy_signal_request` > `trend_analysis`).
*   **Output:** `category_weights` & `top_products` written to `user_profiles`.
*   **Cadence:** Daily batch (08:15 AM).

---

## 🥇 Model 7: Personalised Value Ranking
**Goal:** Which products deserve the user's attention today?
**Why:** Curates a daily "watchlist" based on deterministic value, not opaque neural nets.
*   **Approach:** Explicit ranking function.
    *   $Value\ Score = (0.30 \times Discount) + (0.25 \times Forecast\ Trend) + (0.25 \times Historic\ Value) + (0.10 \times In\ Stock) + (0.10 \times Stability)$.
    *   $Final\ Rank = (User\ Category\ Weight \times 0.40) + (Value\ Score \times 0.35) + (Forecast\ Urgency \times 0.15) + (Top\ Product\ Bonus \times 0.10)$.
    *   *Diversity:* Max 4 per category, min 2 categories.
*   **Output:** Ranked list written to `user_daily_suggestions` with `reason_flags`.
*   **Cadence:** Daily batch (08:20 AM).

---

## ⏱️ The Full Pipeline Schedule
1.  **07:00** Scrapers & Weather APIs run → Kafka.
2.  **07:15** Spark processes raw JSON into Star Schema.
3.  **07:30** `Model 5 (Quality)` cleans and flags data.
4.  **07:35** `Model 2 (Anomaly)` runs live inference on clean data.
5.  **07:45** `Model 1 (Forecast)` runs daily inference.
6.  **08:00** Value Scores are computed globally.
7.  **08:15** `Model 6 (Profiling)` updates user weights.
8.  **08:20** `Model 7 (Ranking)` generates daily suggestions.
9.  **08:30** Caches prime. LLM is ready to narrate the results.
