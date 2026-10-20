"""Profile the local Ookla fixed-broadband Parquet file.

This script only reads the file in data/raw. It prints the schema, file size,
row count, null counts, and five sample rows. The raw file stays unchanged.

From the project folder in PowerShell:

    .\.venv\Scripts\python.exe src\profile_dataset.py

PyArrow reads the file footer, which holds the schema and row count.
DuckDB then runs two read-only SQL queries. The full table is not loaded
into pandas.
"""

from pathlib import Path

import duckdb
import pyarrow.parquet as pq


# __file__ is this script. parent is src/, and parent.parent is the project folder.
# Building the path here means the script works no matter which folder the
# terminal is in.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATASET_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "2024-10-01_performance_fixed_tiles.parquet"
)
SAMPLE_ROW_COUNT = 5


def quote_identifier(column_name: str) -> str:
    """Wrap a column name so SQL treats it as a name, not as code.

    The name comes from the Parquet schema. Quoting keeps names with spaces
    or symbols readable to DuckDB. A quote inside a name is doubled.
    """
    escaped_name = column_name.replace('"', '""')
    return f'"{escaped_name}"'


def ensure_dataset_exists(path: Path) -> None:
    """Stop with a clear message when the expected file is absent."""
    if path.is_file():
        return

    if path.exists():
        raise FileNotFoundError(
            "The dataset path exists, but it is not a file:\n"
            f"  {path}"
        )

    raise FileNotFoundError(
        "The raw dataset is missing. Looked for it here:\n"
        f"  {path}\n"
        "Place the downloaded Parquet file at that path, then run this script again."
    )


def print_schema_profile(path: Path) -> list[str]:
    """Print schema, row count, column count, and file size.

    ParquetFile reads the footer metadata. It does not load every row into
    memory, and it does not write to the file. Column names are taken from
    the schema that PyArrow reports.
    """
    size_mb = path.stat().st_size / (1024 * 1024)
    print(f"File: {path}")
    print(f"File size: {size_mb:.1f} MB")

    try:
        parquet_file = pq.ParquetFile(path)
    except Exception as error:
        raise RuntimeError(
            "PyArrow could not read this file as Parquet:\n"
            f"  {path}\n"
            f"Reason: {error}"
        ) from error

    schema = parquet_file.schema_arrow
    column_names = [field.name for field in schema]

    print(f"Row count: {parquet_file.metadata.num_rows}")
    print(f"Column count: {len(column_names)}")
    print()
    print("Columns:")
    for field in schema:
        print(f"  {field.name}: {field.type}")

    return column_names


def print_sql_profile(path: Path, column_names: list[str]) -> None:
    """Run read-only DuckDB queries for null counts and a five-row sample.

    The database lives in memory, so DuckDB does not create a database file
    next to the data. Both statements are SELECT queries against read_parquet.
    """
    if not column_names:
        print()
        print("The schema has no columns. Null counts and the sample were skipped.")
        return

    # One aggregate scans the file once. Python only receives a single result row.
    null_expressions = []
    for column_name in column_names:
        quoted_name = quote_identifier(column_name)
        null_expressions.append(
            f"sum(CASE WHEN {quoted_name} IS NULL THEN 1 ELSE 0 END) "
            f"AS {quoted_name}"
        )
    null_count_sql = (
        "SELECT " + ", ".join(null_expressions) + " FROM read_parquet(?)"
    )
    sample_sql = f"SELECT * FROM read_parquet(?) LIMIT {SAMPLE_ROW_COUNT}"

    # The path is passed separately from the SQL so Windows backslashes stay
    # part of the path instead of being read as SQL escape characters.
    path_parameter = [str(path)]

    try:
        connection = duckdb.connect(database=":memory:")
        try:
            null_row = connection.execute(null_count_sql, path_parameter).fetchone()
            sample_result = connection.execute(sample_sql, path_parameter)
            sample_columns = [item[0] for item in sample_result.description]
            sample_rows = sample_result.fetchall()
        finally:
            connection.close()
    except duckdb.Error as error:
        raise RuntimeError(
            "DuckDB could not run the read-only profile queries on:\n"
            f"  {path}\n"
            f"Reason: {error}"
        ) from error

    print()
    print("Null counts:")
    if null_row is None:
        print("  The null-count query returned no result.")
    else:
        for column_name, null_count in zip(column_names, null_row):
            print(f"  {column_name}: {null_count}")

    print()
    print(f"Sample of {len(sample_rows)} row(s):")
    if not sample_rows:
        print("  The file has no rows to show.")
        return

    for row_number, row in enumerate(sample_rows, start=1):
        print(f"Row {row_number}:")
        for column_name, value in zip(sample_columns, row):
            print(f"  {column_name}: {value}")


def main() -> None:
    """Profile the local dataset and print a message when a read fails."""
    try:
        ensure_dataset_exists(DATASET_PATH)
        column_names = print_schema_profile(DATASET_PATH)
        print_sql_profile(DATASET_PATH, column_names)
    except (FileNotFoundError, RuntimeError) as error:
        print()
        print("Could not profile the dataset.")
        print(error)
        raise SystemExit(1) from error

    print()
    print("Profile finished. The raw dataset was only read.")


# This guard runs main() only when you execute this file directly.
# Importing the file from another script does not profile the dataset.
if __name__ == "__main__":
    main()
