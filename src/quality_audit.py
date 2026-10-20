"""Run a read-only data-quality audit on the local Ookla Parquet file.

DuckDB reads the file in data/raw and prints quality checks. The raw file
stays unchanged, and the rows are not loaded into pandas.

From the project folder in PowerShell:

    .\.venv\Scripts\python.exe src\quality_audit.py

The script scans the full file, so the run can take a few minutes.
"""

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import duckdb


# __file__ is this script. parent is src/, and parent.parent is the project folder.
# The dataset path is built from that root, so the terminal's current folder
# does not matter.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATASET_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "2024-10-01_performance_fixed_tiles.parquet"
)

# These names come from the profiled schema. The audit uses them only when
# DESCRIBE shows that the file actually contains them.
PERFORMANCE_AND_COUNT_COLUMNS = [
    "avg_d_kbps",
    "avg_u_kbps",
    "avg_lat_ms",
    "avg_lat_down_ms",
    "avg_lat_up_ms",
    "tests",
    "devices",
]
ZERO_CHECK_COLUMNS = ["tests", "devices"]
LATENCY_DIRECTION_COLUMNS = ["avg_lat_down_ms", "avg_lat_up_ms"]


@dataclass
class AuditData:
    """Results collected from DuckDB before anything is printed."""

    columns: list[tuple[str, str]]
    summary: dict[str, object]
    duplicates: dict[str, int] | None


def quote_identifier(column_name: str) -> str:
    """Wrap a column name so SQL treats it as a name, not as code."""
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


def print_section(title: str) -> None:
    """Print a heading that separates one audit check from the next."""
    print()
    print(title)
    print("-" * len(title))


def print_table(headers: list[str], rows: list[list[str]]) -> None:
    """Print a text table. Numeric-looking columns stay right-aligned."""
    widths = [len(header) for header in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    def format_row(cells: list[str]) -> str:
        parts = []
        for index, cell in enumerate(cells):
            if index == 0:
                parts.append(cell.ljust(widths[index]))
            else:
                parts.append(cell.rjust(widths[index]))
        return "  ".join(parts)

    print(format_row(headers))
    print(format_row(["-" * width for width in widths]))
    for row in rows:
        print(format_row(row))


def format_value(value: object, decimal_places: int = 2) -> str:
    """Format one SQL result for display without changing the query result."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, (float, Decimal)):
        return f"{float(value):,.{decimal_places}f}"
    return str(value)


def format_percent(part: int, total: int) -> str:
    """Turn a count into a percentage of all rows."""
    if total == 0:
        return "n/a"
    return f"{(part / total) * 100:.4f}%"


def format_count_and_percent(part: int, total: int) -> str:
    return f"{part:,} ({format_percent(part, total)})"


def as_int(value: object) -> int:
    """Convert a SQL count to int. A SQL NULL count is reported as zero."""
    if value is None:
        return 0
    return int(value)


def describe_columns(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
) -> list[tuple[str, str]]:
    """Read column names and types from the Parquet file through DuckDB."""
    result = connection.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)",
        [str(path)],
    )
    headers = [item[0] for item in result.description]
    rows = result.fetchall()
    try:
        name_index = headers.index("column_name")
        type_index = headers.index("column_type")
    except ValueError as error:
        raise RuntimeError(
            "DuckDB DESCRIBE did not return column_name and column_type.\n"
            f"Headers returned: {headers}"
        ) from error

    return [(str(row[name_index]), str(row[type_index])) for row in rows]


def build_summary_sql(column_names: list[str]) -> str:
    """Build one SELECT that scans the Parquet file a single time.

    Null checks use every column DESCRIBE returned. Min, max, average,
    negative, zero, coordinate, and latency checks are added only for the
    columns this audit knows about and the file actually contains.
    """
    available = set(column_names)
    select_parts = ["count(*) AS total_rows"]

    for index, column_name in enumerate(column_names):
        quoted_name = quote_identifier(column_name)
        select_parts.append(
            f"sum(CASE WHEN {quoted_name} IS NULL THEN 1 ELSE 0 END) "
            f"AS null_{index}"
        )

    for column_name in PERFORMANCE_AND_COUNT_COLUMNS:
        if column_name not in available:
            continue
        quoted_name = quote_identifier(column_name)
        select_parts.append(f"min({quoted_name}) AS min_{column_name}")
        select_parts.append(f"max({quoted_name}) AS max_{column_name}")
        select_parts.append(f"avg({quoted_name}) AS avg_{column_name}")
        select_parts.append(
            f"sum(CASE WHEN {quoted_name} < 0 THEN 1 ELSE 0 END) "
            f"AS negative_{column_name}"
        )

    for column_name in ZERO_CHECK_COLUMNS:
        if column_name not in available:
            continue
        quoted_name = quote_identifier(column_name)
        select_parts.append(
            f"sum(CASE WHEN {quoted_name} = 0 THEN 1 ELSE 0 END) "
            f"AS zero_{column_name}"
        )

    if "tile_x" in available:
        quoted_name = quote_identifier("tile_x")
        select_parts.append(f"min({quoted_name}) AS min_tile_x")
        select_parts.append(f"max({quoted_name}) AS max_tile_x")
        select_parts.append(
            f"sum(CASE WHEN {quoted_name} >= -180 AND {quoted_name} <= 180 "
            f"THEN 1 ELSE 0 END) AS inside_tile_x"
        )
        select_parts.append(
            f"sum(CASE WHEN {quoted_name} < -180 OR {quoted_name} > 180 "
            f"THEN 1 ELSE 0 END) AS outside_tile_x"
        )

    if "tile_y" in available:
        quoted_name = quote_identifier("tile_y")
        select_parts.append(f"min({quoted_name}) AS min_tile_y")
        select_parts.append(f"max({quoted_name}) AS max_tile_y")
        select_parts.append(
            f"sum(CASE WHEN {quoted_name} >= -90 AND {quoted_name} <= 90 "
            f"THEN 1 ELSE 0 END) AS inside_tile_y"
        )
        select_parts.append(
            f"sum(CASE WHEN {quoted_name} < -90 OR {quoted_name} > 90 "
            f"THEN 1 ELSE 0 END) AS outside_tile_y"
        )

    if "tile_x" in available and "tile_y" in available:
        tile_x = quote_identifier("tile_x")
        tile_y = quote_identifier("tile_y")
        select_parts.append(
            "sum(CASE WHEN "
            f"{tile_x} < -180 OR {tile_x} > 180 OR "
            f"{tile_y} < -90 OR {tile_y} > 90 "
            "THEN 1 ELSE 0 END) AS outside_either_coordinate"
        )

    if all(column_name in available for column_name in LATENCY_DIRECTION_COLUMNS):
        down_latency = quote_identifier("avg_lat_down_ms")
        up_latency = quote_identifier("avg_lat_up_ms")
        # OR counts a row once when one value is missing or both are missing.
        select_parts.append(
            f"sum(CASE WHEN {down_latency} IS NULL OR {up_latency} IS NULL "
            f"THEN 1 ELSE 0 END) AS missing_either_latency_direction"
        )
        select_parts.append(
            f"sum(CASE WHEN {down_latency} IS NULL AND {up_latency} IS NULL "
            f"THEN 1 ELSE 0 END) AS missing_both_latency_directions"
        )

    select_list = ",\n    ".join(select_parts)
    return f"SELECT\n    {select_list}\nFROM read_parquet(?)"


def fetch_row(result: duckdb.DuckDBPyConnection) -> dict[str, object]:
    """Turn one SQL result row into a dictionary keyed by column alias."""
    headers = [item[0] for item in result.description]
    row = result.fetchone()
    if row is None:
        raise RuntimeError("A query finished without returning a result row.")
    return dict(zip(headers, row))


def fetch_duplicates(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
) -> dict[str, int]:
    """Count repeated non-null quadkey values.

    Null quadkeys are excluded here and reported with the other null counts.
    GROUP BY puts each distinct quadkey in one group, then the outer query
    keeps the groups whose row count is greater than one.
    """
    duplicate_sql = """
        SELECT
            count(*) AS distinct_values,
            count(*) FILTER (WHERE occurrences > 1) AS values_repeated,
            coalesce(
                sum(occurrences) FILTER (WHERE occurrences > 1),
                0
            ) AS rows_in_repeated_values,
            coalesce(
                sum(occurrences - 1) FILTER (WHERE occurrences > 1),
                0
            ) AS extra_rows
        FROM (
            SELECT quadkey, count(*) AS occurrences
            FROM read_parquet(?)
            WHERE quadkey IS NOT NULL
            GROUP BY quadkey
        )
    """
    summary = fetch_row(connection.execute(duplicate_sql, [str(path)]))
    return {key: as_int(value) for key, value in summary.items()}


def collect_audit(path: Path) -> AuditData:
    """Run the read-only queries. The database connection is in memory."""
    print("Running read-only DuckDB queries on the Parquet file.", flush=True)
    print("A full scan can take a few minutes.", flush=True)

    connection = duckdb.connect(database=":memory:")
    try:
        columns = describe_columns(connection, path)
        column_names = [column_name for column_name, _column_type in columns]
        available = set(column_names)

        print("Query 1 of 2: counts, nulls, ranges, and numeric summary.", flush=True)
        summary = fetch_row(
            connection.execute(build_summary_sql(column_names), [str(path)])
        )

        duplicates = None
        if "quadkey" in available:
            print("Query 2 of 2: quadkey duplicate check.", flush=True)
            duplicates = fetch_duplicates(connection, path)
        else:
            print("Query 2 of 2 skipped: the file has no quadkey column.", flush=True)
    except duckdb.Error as error:
        raise RuntimeError(
            "DuckDB could not finish the read-only quality audit on:\n"
            f"  {path}\n"
            f"Reason: {error}"
        ) from error
    finally:
        connection.close()

    return AuditData(columns=columns, summary=summary, duplicates=duplicates)


def null_count(summary: dict[str, object], index: int) -> int:
    return as_int(summary[f"null_{index}"])


def print_overview(path: Path, audit: AuditData, total_rows: int) -> None:
    print("Ookla fixed broadband data-quality audit")
    print(f"File: {path}")
    print_section("Dataset size")
    print(f"Rows: {total_rows:,}")
    print(f"Columns: {len(audit.columns):,}")

    print_section("Columns")
    print_table(
        ["Column", "Type"],
        [[column_name, column_type] for column_name, column_type in audit.columns],
    )


def print_nulls(audit: AuditData, total_rows: int) -> None:
    print_section("Null counts")
    rows = []
    for index, (column_name, _column_type) in enumerate(audit.columns):
        missing = null_count(audit.summary, index)
        rows.append(
            [
                column_name,
                f"{missing:,}",
                format_percent(missing, total_rows),
            ]
        )
    print_table(["Column", "Nulls", "Percent of rows"], rows)


def print_numeric_summary(audit: AuditData) -> None:
    print_section("Numeric summary")
    print("Minimum, maximum, and average for the performance and count columns.")
    print("Average is calculated from non-null values.")

    available = {column_name for column_name, _column_type in audit.columns}
    rows = []
    missing_columns = []
    for column_name in PERFORMANCE_AND_COUNT_COLUMNS:
        if column_name not in available:
            missing_columns.append(column_name)
            continue
        rows.append(
            [
                column_name,
                format_value(audit.summary[f"min_{column_name}"]),
                format_value(audit.summary[f"max_{column_name}"]),
                format_value(audit.summary[f"avg_{column_name}"]),
            ]
        )

    if rows:
        print()
        print_table(["Column", "Minimum", "Maximum", "Average"], rows)
    if missing_columns:
        print()
        print("Skipped because these columns are not in the file:")
        for column_name in missing_columns:
            print(f"  {column_name}")


def print_negative_values(audit: AuditData, total_rows: int) -> None:
    print_section("Negative values")
    print("Speed, latency, tests, and devices. The count includes values below zero.")

    available = {column_name for column_name, _column_type in audit.columns}
    rows = []
    missing_columns = []
    for column_name in PERFORMANCE_AND_COUNT_COLUMNS:
        if column_name not in available:
            missing_columns.append(column_name)
            continue
        negatives = as_int(audit.summary[f"negative_{column_name}"])
        rows.append(
            [
                column_name,
                f"{negatives:,}",
                format_percent(negatives, total_rows),
            ]
        )

    if rows:
        print()
        print_table(["Column", "Negative rows", "Percent of rows"], rows)
    if missing_columns:
        print()
        print("Skipped because these columns are not in the file:")
        for column_name in missing_columns:
            print(f"  {column_name}")


def print_zero_values(audit: AuditData, total_rows: int) -> None:
    print_section("Zero values")
    print("Zero counts for tests and devices.")

    available = {column_name for column_name, _column_type in audit.columns}
    rows = []
    missing_columns = []
    for column_name in ZERO_CHECK_COLUMNS:
        if column_name not in available:
            missing_columns.append(column_name)
            continue
        zeros = as_int(audit.summary[f"zero_{column_name}"])
        rows.append(
            [
                column_name,
                f"{zeros:,}",
                format_percent(zeros, total_rows),
            ]
        )

    if rows:
        print()
        print_table(["Column", "Zero rows", "Percent of rows"], rows)
    if missing_columns:
        print()
        print("Skipped because these columns are not in the file:")
        for column_name in missing_columns:
            print(f"  {column_name}")


def print_coordinates(audit: AuditData, total_rows: int) -> None:
    print_section("Coordinate ranges")
    available = {column_name for column_name, _column_type in audit.columns}
    column_names = [column_name for column_name, _column_type in audit.columns]

    if "tile_x" in available:
        inside = as_int(audit.summary["inside_tile_x"])
        outside = as_int(audit.summary["outside_tile_x"])
        missing = null_count(audit.summary, column_names.index("tile_x"))
        print("tile_x expected range: -180 to 180")
        print(f"  Inside range: {format_count_and_percent(inside, total_rows)}")
        print(f"  Outside range: {format_count_and_percent(outside, total_rows)}")
        print(f"  Null: {format_count_and_percent(missing, total_rows)}")
        print(f"  Minimum: {format_value(audit.summary['min_tile_x'], decimal_places=6)}")
        print(f"  Maximum: {format_value(audit.summary['max_tile_x'], decimal_places=6)}")
    else:
        print("tile_x range check skipped because tile_x is not in the file.")

    if "tile_y" in available:
        inside = as_int(audit.summary["inside_tile_y"])
        outside = as_int(audit.summary["outside_tile_y"])
        missing = null_count(audit.summary, column_names.index("tile_y"))
        print()
        print("tile_y expected range: -90 to 90")
        print(f"  Inside range: {format_count_and_percent(inside, total_rows)}")
        print(f"  Outside range: {format_count_and_percent(outside, total_rows)}")
        print(f"  Null: {format_count_and_percent(missing, total_rows)}")
        print(f"  Minimum: {format_value(audit.summary['min_tile_y'], decimal_places=6)}")
        print(f"  Maximum: {format_value(audit.summary['max_tile_y'], decimal_places=6)}")
    else:
        print()
        print("tile_y range check skipped because tile_y is not in the file.")

    if "tile_x" in available and "tile_y" in available:
        outside_either = as_int(audit.summary["outside_either_coordinate"])
        print()
        print(
            "Rows with tile_x or tile_y outside its range: "
            f"{format_count_and_percent(outside_either, total_rows)}"
        )


def print_latency_gaps(audit: AuditData, total_rows: int) -> None:
    print_section("Latency direction gaps")
    print("Latency-direction columns: avg_lat_down_ms and avg_lat_up_ms.")

    available = {column_name for column_name, _column_type in audit.columns}
    column_names = [column_name for column_name, _column_type in audit.columns]
    if not all(column_name in available for column_name in LATENCY_DIRECTION_COLUMNS):
        print("This check needs both columns, and at least one is missing from the file.")
        return

    for column_name in LATENCY_DIRECTION_COLUMNS:
        missing = null_count(audit.summary, column_names.index(column_name))
        print(
            f"{column_name} nulls: {format_count_and_percent(missing, total_rows)}"
        )

    missing_either = as_int(audit.summary["missing_either_latency_direction"])
    missing_both = as_int(audit.summary["missing_both_latency_directions"])
    print(
        "Rows missing at least one of the two values: "
        f"{format_count_and_percent(missing_either, total_rows)}"
    )
    print(
        "Rows missing both values: "
        f"{format_count_and_percent(missing_both, total_rows)}"
    )


def print_duplicates(audit: AuditData, total_rows: int) -> None:
    print_section("Quadkey duplicates")
    print("Repeated quadkey values are counted below.")
    print("This report does not label a repeat as valid or invalid.")

    column_names = [column_name for column_name, _column_type in audit.columns]
    if audit.duplicates is None or "quadkey" not in column_names:
        print()
        print("Duplicate check skipped because quadkey is not in the file.")
        return

    null_quadkeys = null_count(audit.summary, column_names.index("quadkey"))
    rows_with_quadkey = total_rows - null_quadkeys
    distinct_values = audit.duplicates["distinct_values"]
    values_repeated = audit.duplicates["values_repeated"]
    rows_in_repeated_values = audit.duplicates["rows_in_repeated_values"]
    extra_rows = audit.duplicates["extra_rows"]

    print()
    print(f"Rows with a non-null quadkey: {rows_with_quadkey:,}")
    print(f"Distinct non-null quadkey values: {distinct_values:,}")
    print(f"Quadkey values appearing more than once: {values_repeated:,}")
    print(f"Rows that use one of those repeated values: {rows_in_repeated_values:,}")
    print(f"Extra rows beyond a single occurrence: {extra_rows:,}")
    print(f"Null quadkey rows: {null_quadkeys:,}")
    print()
    if values_repeated == 0:
        print("Each non-null quadkey value appears on one row.")
    else:
        print("Some non-null quadkey values appear on more than one row.")


def print_report(path: Path, audit: AuditData) -> None:
    """Print each quality check in its own section."""
    total_rows = as_int(audit.summary["total_rows"])
    print()
    print_overview(path, audit, total_rows)
    print_nulls(audit, total_rows)
    print_numeric_summary(audit)
    print_negative_values(audit, total_rows)
    print_zero_values(audit, total_rows)
    print_coordinates(audit, total_rows)
    print_latency_gaps(audit, total_rows)
    print_duplicates(audit, total_rows)
    print()
    print("Audit finished. The raw dataset was only read.")


def main() -> None:
    """Audit the local dataset and print a message when a read fails."""
    try:
        ensure_dataset_exists(DATASET_PATH)
        audit = collect_audit(DATASET_PATH)
    except (FileNotFoundError, RuntimeError) as error:
        print()
        print("Could not audit the dataset.")
        print(error)
        raise SystemExit(1) from error

    print_report(DATASET_PATH, audit)


# This guard runs main() only when you execute this file directly.
# Importing the file from another script does not audit the dataset.
if __name__ == "__main__":
    main()
