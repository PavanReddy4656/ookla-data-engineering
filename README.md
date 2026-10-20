# Network Performance Data Engineering Pipeline

A beginner portfolio project that will turn public network performance data into clean, queryable tables on your own computer.

Nothing is installed yet, no dataset has been downloaded, and no cloud resources have been created. This repository currently holds the folder layout and the plan for the work ahead.

## Objective

Build a small, local data engineering pipeline that:

1. Stores raw network performance files without changing them.
2. Cleans and organizes that data with Python.
3. Loads it into DuckDB so it can be queried with SQL.
4. Saves analysis-ready outputs.
5. Checks the important steps with automated tests.

The exact columns will be written down only after we inspect a real data file. This README does not guess the schema.

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

Commands in this project use PowerShell. When it is time to install packages, the commands will look like this:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Do not run those commands yet. Packages stay uninstalled until a later step.

If PowerShell blocks the activate script, that is the execution policy, not a broken project. We will handle it when we create the virtual environment.

## What this step does not include

- No `pip install`
- No dataset download
- No AWS resources
- No pipeline code
- No invented column list
