# Network Performance Data Engineering Pipeline

A beginner portfolio project that turns public Ookla fixed-broadband measurements into a clean local table.

The pipeline reads the raw Parquet file, checks each row, and writes the valid rows to `data/processed`. It does not change the raw file.

## Objective

Build a small, local data engineering pipeline that:

1. Stores raw network performance files without changing them.
2. Cleans and organizes that data with Python.
3. Loads it into DuckDB so it can be queried with SQL.
4. Saves analysis-ready outputs.
5. Checks the important steps with automated tests.

The processed file keeps the columns from the raw file. A missing downstream or upstream latency value is allowed and does not remove the row.

## Planned local stack

| Tool | Role |
| --- | --- |
| Python | Scripts that read, clean, and save data |
| pandas | Working with tables in Python |
| PyArrow | Efficient columnar data, including Parquet files |
| DuckDB | A local analytical database you can query with SQL |
| SQL | The transformation queries, kept in the `sql` folder |
| pytest | Tests that confirm the pipeline behaves as expected |

These tools run on your machine. AWS and other cloud services are out of scope for now.

## Planned workflow

Each later step will add one piece. The intended order is:

1. **Land raw data** in `data/raw`. Original files stay untouched.
2. **Inspect the file** and record the real column names and types in `docs`.
3. **Write Python** in `src` to read the raw file with pandas and PyArrow.
4. **Load a local DuckDB database** and explore the data with SQL from the `sql` folder.
5. **Transform** the data into a cleaner table and write the result to `data/processed`.
6. **Test** the important checks with pytest in `tests`.
7. **Document** what each step does, in plain language, in `docs` and this README.

## Folder guide

| Folder | Purpose |
| --- | --- |
| `data/raw` | Original downloaded files. Git ignores their contents. |
| `data/processed` | Cleaned files the pipeline creates. Git ignores their contents. |
| `src` | Python code for the pipeline. |
| `sql` | SQL queries used with DuckDB. |
| `tests` | pytest tests. |
| `docs` | Notes about the data, decisions, and how to run the project. |

Empty folders contain a `.gitkeep` file so Git can track the folder even when it has no real files yet. Generated data is ignored; `.gitkeep` is not.

## Windows and PowerShell

Commands in this project use PowerShell. From the project folder:

```powershell
.\.venv\Scripts\Activate.ps1
.\.venv\Scripts\python.exe src\etl_pipeline.py
.\.venv\Scripts\python.exe -m pytest tests
```

If PowerShell blocks the activate script, that is the execution policy, not a broken project.

## Data-quality summary

A successful pipeline run logs this summary and prints the same text at the end. The numbers below are an example from a tiny file, not a copy of the full dataset. A real run prints the full Windows path of the output file:

```text
Data-quality summary
---------------------
Input rows: 4
Valid rows: 3
Rejected rows: 1
Rejection percentage: 25.00%
Null avg_lat_down_ms: 2
Null avg_lat_up_ms: 1
Valid output file: data/processed/2024-10-01_performance_fixed_tiles.parquet
Duration: 1.250 seconds
```

| Metric | Meaning |
| --- | --- |
| Input rows | Rows read from the raw Parquet file. |
| Valid rows | Rows that passed every validation rule and were written to the processed file. |
| Rejected rows | Rows that failed a rule. They are written to `rejected_records.parquet` when this count is above zero. |
| Rejection percentage | Rejected rows divided by input rows, shown as a percent. An empty input has no rejected rows, so this is `0.00%`. |
| Null avg_lat_down_ms | Input rows missing downstream latency. These rows are kept when the other rules pass. |
| Null avg_lat_up_ms | Input rows missing upstream latency. These rows are kept when the other rules pass. |
| Valid output file | Path of the processed Parquet file written by this run. |
| Duration | Seconds from the start of the run until the outputs are in place. |

High download speed, upload speed, or latency does not reject a row. The pipeline does not invent a cutoff for outliers.

## SQL analytics

The `sql` folder holds reusable read-only queries. Each row in the processed file is one geographic tile. `avg_d_kbps` and `avg_u_kbps` are already averages for that tile, stored in kilobits per second. `tests` and `devices` are counts stored on the tile, not individual speed tests or unique users.

Run the queries from the project folder:

```powershell
.\.venv\Scripts\python.exe src\run_sql_analytics.py
```

The script opens `data/processed/2024-10-01_performance_fixed_tiles.parquet` in DuckDB and runs the SQL files. It does not change the raw or processed files.

| SQL file | Purpose |
| --- | --- |
| `sql/speed_summary.sql` | Average and median download and upload speed across tiles. Speeds are shown in Mbps, using 1 Mbps = 1,000 kbps. |
| `sql/speed_by_test_count_tercile.sql` | Average download speed and latency for three groups created with `NTILE(3)`. Group 1 has the lowest tile test counts and group 3 has the highest. These groups are not official Ookla categories. |
| `sql/top_tiles_by_download_speed.sql` | The 100 tiles with the highest tile-level download speed, with each tile's test count and latency. Equal speeds are ordered by test count, then by quadkey. |
| `sql/data_quality_checks.sql` | Counts of missing downstream and upstream latency, plus tiles that fail the core validation rules. Missing directional latency is reported and does not by itself make a tile invalid. The query does not delete rows. |
