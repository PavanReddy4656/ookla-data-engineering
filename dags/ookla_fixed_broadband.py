"""Manually triggered Ookla fixed-broadband pipeline.

Task order:
1. Confirm the raw Parquet file is mounted in the container.
2. Run src/etl_pipeline.py.
3. Run src/run_sql_analytics.py only after the ETL task succeeds.

The scripts resolve their own project root from their file path, so this file
only needs src/, sql/, and data/ mounted together at /opt/ookla.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pendulum
from airflow.sdk import dag, task


PROJECT_DIR = Path("/opt/ookla")
RAW_DATASET = (
    PROJECT_DIR
    / "data"
    / "raw"
    / "2024-10-01_performance_fixed_tiles.parquet"
)
ETL_SCRIPT = PROJECT_DIR / "src" / "etl_pipeline.py"
ANALYTICS_SCRIPT = PROJECT_DIR / "src" / "run_sql_analytics.py"


def run_project_script(script_path: Path) -> None:
    """Run one project script with the container Python.

    A missing script or a non-zero exit code fails the Airflow task.
    """
    if not script_path.is_file():
        raise FileNotFoundError(f"Script is missing in the container: {script_path}")
    subprocess.run([sys.executable, str(script_path)], check=True)


@dag(
    dag_id="ookla_fixed_broadband",
    description="Check the Ookla raw Parquet file, run ETL, then run SQL analytics.",
    schedule=None,
    start_date=pendulum.datetime(2024, 10, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["ookla"],
)
def ookla_fixed_broadband():
    """Check the raw file, then run ETL, then run SQL analytics."""

    @task(task_id="check_raw_parquet")
    def check_raw_parquet() -> str:
        """Fail this task when the mounted raw Parquet file is missing."""
        if not RAW_DATASET.is_file():
            raise FileNotFoundError(f"Raw Parquet file is missing: {RAW_DATASET}")
        return str(RAW_DATASET)

    @task(task_id="run_etl")
    def run_etl(raw_dataset: str) -> str:
        """Run the existing ETL script. The script checks and writes its own paths."""
        if not Path(raw_dataset).is_file():
            raise FileNotFoundError(f"Raw Parquet file is missing: {raw_dataset}")
        run_project_script(ETL_SCRIPT)
        return raw_dataset

    @task(task_id="run_sql_analytics")
    def run_sql_analytics(raw_dataset: str) -> None:
        """Run the SQL analytics script after ETL has succeeded.

        Airflow's default trigger rule is all_success, so this task does not
        start when the ETL task fails or is skipped.
        """
        if not raw_dataset:
            raise FileNotFoundError("ETL did not receive the raw Parquet path.")
        run_project_script(ANALYTICS_SCRIPT)

    checked = check_raw_parquet()
    etl_done = run_etl(checked)
    run_sql_analytics(etl_done)


ookla_fixed_broadband()
