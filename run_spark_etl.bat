@echo off
REM ═══════════════════════════════════════════════════════════
REM  Market Intelligence — Spark ETL Launcher (Windows)
REM  Sets SPARK_HOME, JAVA_HOME, HADOOP_HOME, PYSPARK_PYTHON
REM  and runs the streaming ETL job using PySpark pip package.
REM ═══════════════════════════════════════════════════════════

REM ── Project Root (strip trailing backslash) ───────────────
set "PROJECT_ROOT=E:\cv projects\real_time-market-intelligence"

REM ── Java ──────────────────────────────────────────────────
set "JAVA_HOME=C:\Program Files\Java\jdk-20"

REM ── Hadoop (winutils for Windows) ─────────────────────────
set "HADOOP_HOME=C:\hadoop"

REM ── PySpark as SPARK_HOME ─────────────────────────────────
set "SPARK_HOME=%PROJECT_ROOT%\venv\Lib\site-packages\pyspark"

REM ── Python — use the venv python ──────────────────────────
set "PYSPARK_PYTHON=%PROJECT_ROOT%\venv\Scripts\python.exe"
set "PYSPARK_DRIVER_PYTHON=%PROJECT_ROOT%\venv\Scripts\python.exe"

REM ── PYTHONPATH — so ETL imports work ──────────────────────
set "PYTHONPATH=%PROJECT_ROOT%\market_intelligence;%PROJECT_ROOT%;%PYTHONPATH%"

REM ── PATH ──────────────────────────────────────────────────
set "PATH=%JAVA_HOME%\bin;%SPARK_HOME%\bin;%HADOOP_HOME%\bin;%PATH%"

echo ══════════════════════════════════════════════════════════
echo  SPARK_HOME  = %SPARK_HOME%
echo  JAVA_HOME   = %JAVA_HOME%
echo  HADOOP_HOME = %HADOOP_HOME%
echo  PYSPARK_PYTHON = %PYSPARK_PYTHON%
echo ══════════════════════════════════════════════════════════
echo  Starting Spark ETL Job...
echo ══════════════════════════════════════════════════════════

REM ── Launch spark-submit ───────────────────────────────────
call "%SPARK_HOME%\bin\spark-submit.cmd" ^
  --master "local[*]" ^
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1" ^
  "%PROJECT_ROOT%\spark_etl\etl_job.py"

pause
