# Advanced Design: ML, LLM & Personalization Architecture

This document expands on the foundational ML & LLM architecture with a production-grade blueprint for model lifecycles, complex SQL generation, stateful conversational memory, and deterministic user-value scoring.

---

## 🟢 1. The Machine Learning Lifecycle
The ML layer must strictly separate continuous inference from scheduled retraining to prevent overfitting and ensure computational efficiency.

### Inference (Continuous)
*   **Flow:** Scraper → Kafka → Spark → `fact_pricing_snapshots` → **Live Inference** → `ml_predictions`.
*   **Action:** Every new scraped row is immediately passed through the currently active model to generate a forecast, anomaly score, and elasticity index.

### Retraining (Scheduled Batch)
*   **Flow:** Runs offline (e.g., weekly, or daily for perishables).
*   **Action:** Trains on all accumulated historical data.
*   **Model Registry:** Uses tools like MLflow to track:
    *   Training timestamp & data volume.
    *   Metrics (MAE for forecasting, Precision/F1 for anomalies, $R^2$ for correlations).
    *   Versions (`v1`, `v2`).
*   **Promotion Rule:** New versions are only promoted to live inference if validation metrics improve over the active version. Ensures full auditability.

---

## 🟣 2. LLM Interaction paradigms

The LLM never directly queries raw fact tables for insights. It relies entirely on structured data provided by the application.

### A. Pull Mechanism (Dashboard Queries)
1.  Application runs simple SQL on `ml_predictions` to get the latest structured row.
2.  Application assembles a context payload (e.g., predicted array, anomaly flags).
3.  LLM is prompted to translate that payload into a plain-English recommendation.

### B. Push Mechanism (Real-Time Alerts)
1.  Spark publishes `>10%` change to `alerts.price_drops` (Kafka).
2.  Consumer fetches `ml_predictions` and `fact_daily_weather` context.
3.  LLM generates an explanation narrative, which is pushed to the user.

---

## 🧠 3. Advanced Text-to-SQL (Chain of Thought)

For complex user questions (e.g., *"Which anomalous products had high weather correlation and big price swings?"*), a vanilla LLM prompt will fail.

### The Pipeline:
1.  **Schema Injection:** Every prompt includes explicit Star Schema DDL definitions and relationships.
2.  **Decomposition (CoT):** The LLM must first output a reasoning block breaking down the filtering, calculations, and joins required.
3.  **SQL Generation:** Based on the decomposed logic (often using CTEs).
4.  **Application Validation:**
    *   Parse AST to ensure read-only (`SELECT` only).
    *   Enforce a `LIMIT` cap.
    *   If execution fails, feed the error back to the LLM for self-correction.
5.  **Agentic Execution:** For multi-step analysis, the LLM coordinates sequential queries, passing results from Query 1 as parameters to Query 2.

---

## 🔄 4. Stateful Conversational Memory

LLMs are stateless. To support multi-turn data exploration, the backend must maintain conversational context.

### The Conversation State Object (Redis)
*   `conversation_id`, `user_id`.
*   `message_history`: Array of past user/assistant messages.
*   `active_context`:
    *   `last_product_set` (Array of SKUs).
    *   `last_pincode_filter`.
    *   `last_sql_run`.

### Handling Context Windows
*   **Sliding Window:** Keep the last 10 turns intact. Summarize older turns into a single compressed block to save tokens.
*   **Incremental SQL:** Follow-up questions do not generate SQL from scratch. The LLM receives `last_sql_run` and modifies it (e.g., appending a `WHERE` clause based on the new user filter).

---

## 📊 5. User Interest & Daily Value Scoring

Since there is no "purchase" data, we cannot build a traditional collaborative filtering recommender. Instead, we build a **curated daily watchlist**.

### Step 1: User Interest Profile
*   **Parsing:** Extract intents from search queries (`user_search_log`).
*   **Profile:** Maintain category frequency weights for each user (e.g., `vegetables: 0.45`, `dairy: 0.28`). Rebuilt nightly.

### Step 2: Deterministic Value Scoring
Calculated daily for every product:
$$Value\ Score = (0.30 \times Discount\ Depth) + (0.25 \times Forecast\ Trend) + (0.25 \times Historic\ Value) + (0.10 \times In\ Stock) + (0.10 \times Stability\ Score)$$
*(Stability = $1 - anomaly\_score$)*

### Step 3: Personalized Watchlist Generation
*   Join `daily_product_scores` + `user_profiles` on category weights.
*   Filter by availability and threshold.
*   Rank by `(user_category_weight * value_score)`.

### Step 4: LLM Narration
The LLM does **not** pick the products. It receives the top 3 deterministic outputs and creates a personalized human-readable daily briefing explaining *why* they are good deals today.

---

## ⏱️ 6. The Orchestration Cascade

Data integrity relies on strict sequential execution of downstream dependencies:
*   `07:00` → Scrapers & Weather APIs run.
*   `07:15` → Spark processes Kafka → `fact_pricing_snapshots`.
*   `07:30` → Live ML Inference scores new rows → `ml_predictions`.
*   `08:00` → `daily_product_scores` batch job computes Value Scores.
*   `08:15` → `user_profiles` job rebuilds interest weights.
*   `08:30` → Personalized Watchlist caching complete. Ready for LLM Narration.
