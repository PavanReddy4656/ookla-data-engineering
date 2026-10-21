FROM apache/airflow:3.3.2-python3.12

# DuckDB filters each batch. PyArrow reads and writes the Parquet batches.
# Pinning apache-airflow to the image version stops pip from replacing Airflow.
# This install happens in the image, not in the Windows virtual environment.
USER airflow
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "duckdb==1.5.6" "pyarrow==25.0.1"
