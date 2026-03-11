# Manual Operation Guide (Non-Docker / Windows)

This guide provides the sequence of commands to run the Market Intelligence pipeline manually on Windows.

## 1. Prerequisites (External Binaries)
Since you are not using Docker, ensure you have the following installed and in your `PATH`:
- **Java JDK 11+** (Required for Kafka and Spark)
- **Apache Kafka** (Download binary from [kafka.apache.org](https://kafka.apache.org/downloads))
- **Apache Spark 3.5.0** (Download from [spark.apache.org](https://spark.apache.org/downloads))
- **Redis for Windows** (Required for Celery) - [github.com/microsoftarchive/redis](https://github.com/microsoftarchive/redis/releases) or use Memurai.
- **PostgreSQL 16** (Already running locally)

---

## 2. Infrastructure (Start in Order)

### A. Start Zookeeper
Open a terminal and run:
```powershell
C:\k\bin\windows\zookeeper-server-start.bat C:\k\config\zookeeper.properties
```

### B. Start Kafka Broker
Open a NEW terminal:
```powershell
C:\k\bin\windows\kafka-server-start.bat C:\k\config\server.properties
```

### C. Start Redis (Celery Broker)
Open a NEW terminal and run your Redis server executable (e.g., `redis-server.exe`).

### D. Create Kafka Topics
You can use the helper script or run these commands:
```powershell
# Create raw topics
C:\k\bin\windows\kafka-topics.bat --create --topic raw.jiomart --bootstrap-server localhost:9092
C:\k\bin\windows\kafka-topics.bat --create --topic raw.bigbasket --bootstrap-server localhost:9092
C:\k\bin\windows\kafka-topics.bat --create --topic context.weather --bootstrap-server localhost:9092
# Create alert topic
C:\k\bin\windows\kafka-topics.bat --create --topic alerts.price_drops --bootstrap-server localhost:9092
```

---

## 3. Application (Start in Order)

### A. Start Celery Worker (Scrapers)
Scrapers are triggered via Celery. Open a terminal, activate your venv, and run:
```powershell
venv\Scripts\activate
celery -A celery_app worker --loglevel=info -P solo
```
*(Note: `-P solo` is the most stable way to run Celery on Windows)*

### B. Start Spark ETL Job
This consumes from Kafka and writes to Postgres. Ensure `SPARK_HOME` is set.
```powershell
venv\Scripts\activate
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 spark_etl/etl_job.py
```

### C. Dispatch the Scrapers
Now that the infrastructure is up, trigger the actual scraping:
```powershell
venv\Scripts\activate

# Run everything (JioMart, BigBasket, Weather)
python run_distributed.py --all

# OR run individually
python run_distributed.py --source jiomart
python run_distributed.py --source bigbasket
```

---

## 4. Verification
1. **Kafka**: Use `kafka-console-consumer.bat` to verify data is hitting `raw.*` topics.
2. **Postgres**: Check `fact_pricing_snapshots` to see if records are landing.
3. **MLflow**: Once data exists, you can run the batch scripts in `ml_batch/` as shown previously.
