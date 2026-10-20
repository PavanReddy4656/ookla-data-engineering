r"""Run the reusable SQL analytics queries on the processed Ookla tiles.

DuckDB reads the Parquet file through an in-memory view named performance_tiles.
The SQL files live in sql/. They do not load pandas, and they do not write
to the raw or processed files.

From the project folder in PowerShell:

    .\.venv\Scripts\python.exe src\run_sql_analytics.py

The queries scan the full file, so the run can take a few minutes.
"""

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import duckdb


# __file__ is this script. parent is src/, and parent.parent is the project folder.
# Paths built from that root work no matter which folder the terminal is in.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = PROJECT_ROOT / "sql"
PROCESSED_DATASET_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "2024-10-01_performance_fixed_tiles.parquet"
)
VIEW_NAME = "performance_tiles"


@dataclass(frozen=True)
class QuerySpec:
    """One SQL file and the heading printed before its result."""

    filename: str
    title: str
    purpose: str


@dataclass(frozen=True)
class QueryResult:
    """Rows returned by one analytics query."""

    spec: QuerySpec
    columns: list[str]
    rows: list[dict[str, object]]


# The order here is the order the report prints. It does not depend on
# how the operating system lists the sql folder.
QUERY_CATALOG = [
    QuerySpec(
        filename="speed_summary.sql",
        title="Overall download and upload speed",
        purpose=(
            "Average and median download and upload speed across tiles. "
            "Each row is one tile. Speeds are converted from kbps to Mbps "
            "using 1 Mbps = 1,000 kbps."
        ),
    ),
    QuerySpec(
        filename="speed_by_test_count_tercile.sql",
        title="Download speed and latency by test-count tercile",
        purpose=(
            "NTILE(3) groups tiles by the test count stored on each tile. "
            "Group 1 is the lowest third and group 3 is the highest third. "
            "These groups are not official Ookla categories."
        ),
    ),
    QuerySpec(
        filename="top_tiles_by_download_speed.sql",
        title="Top 100 tiles by average download speed",
        purpose=(
            "The 100 tiles with the highest tile-level download speed, "
            "with each tile's test count and latency. Ties use the test "
            "count, then quadkey."
        ),
    ),
    QuerySpec(
        filename="data_quality_checks.sql",
        title="Data-quality checks",
        purpose=(
            "Counts missing downstream and upstream latency, and counts "
            "tiles that fail the core validation rules. This query does "
            "not remove rows. Missing directional latency is allowed."
        ),
    ),
]


def ensure_processed_dataset_exists(path: Path) -> None:
    """Stop with a clear message when the processed file is absent."""
    if path.is_file():
        return

    if path.exists():
        raise FileNotFoundError(
            "The processed dataset path exists, but it is not a file:\n"
            f"  {path}"
        )

    raise FileNotFoundError(
        "The processed dataset is missing. Looked for it here:\n"
        f"  {path}\n"
        "Run src/etl_pipeline.py to create it, then run this script again."
    )


def dataset_signature(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return (stat.st_size, stat.st_mtime_ns)


def print_section(title: str) -> None:
    """Print a heading that separates one query from the next."""
    print()
    print(title)
    print("-" * len(title))


def print_table(headers: list[str], rows: list[list[str]]) -> None:
    """Print a text table. The first column is left-aligned."""
    if not headers:
        print("The query returned no columns.")
        return

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


def first_sql_keyword(sql: str) -> str:
    """Return the first SQL keyword after comments and blank lines."""
    text = sql.strip()
    while text:
        if text.startswith("--"):
            newline = text.find("\n")
            text = "" if newline == -1 else text[newline + 1 :].strip()
            continue
        if text.startswith("/*"):
            end = text.find("*/")
            if end == -1:
                return ""
            text = text[end + 2 :].strip()
            continue
        break
    if not text:
        return ""
    return text.split(None, 1)[0].upper().rstrip(";")


def assert_read_only_query(sql: str, source_name: str) -> None:
    """Allow a query only when it starts with SELECT or WITH."""
    keyword = first_sql_keyword(sql)
    if keyword in {"SELECT", "WITH"}:
        return
    shown = keyword or "nothing"
    raise RuntimeError(
        f"{source_name} must be a read-only SELECT query.\n"
        f"The first SQL keyword is {shown}."
    )


def read_sql_file(path: Path) -> str:
    """Read one SQL file from the project sql folder."""
    if path.is_file():
        return path.read_text(encoding="utf-8")

    raise FileNotFoundError(
        "The SQL file is missing. Looked for it here:\n"
        f"  {path}"
    )


def format_cell(column: str, value: object) -> str:
    """Format one result value using the unit encoded in the column name."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, Decimal):
        value = float(value)

    if column.endswith("_mbps"):
        return f"{float(value):,.3f} Mbps"
    if column.endswith("_ms"):
        return f"{float(value):,.2f} ms"
    if column.endswith("_pct"):
        return f"{float(value):.2f}%"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float) and value.is_integer() and not column.endswith("_mbps"):
        return f"{int(value):,}"
    if isinstance(value, float):
        return f"{value:,.3f}"
    return str(value)


def sql_string(path: Path) -> str:
    """Turn a Windows path into a SQL string literal DuckDB can read."""
    return path.resolve().as_posix().replace("'", "''")


def connect_to_dataset(path: Path) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB view over the Parquet file."""
    ensure_processed_dataset_exists(path)
    connection = duckdb.connect(database=":memory:")
    try:
        # CREATE VIEW cannot use a prepared ? parameter, so the path is quoted.
        connection.execute(
            f"CREATE VIEW {VIEW_NAME} AS "
            f"SELECT * FROM read_parquet('{sql_string(path)}')"
        )
    except duckdb.Error as error:
        connection.close()
        raise RuntimeError(
            "DuckDB could not open the processed dataset:\n"
            f"  {path}\n"
            f"Reason: {error}"
        ) from error
    return connection


def execute_query(
    connection: duckdb.DuckDBPyConnection,
    spec: QuerySpec,
) -> QueryResult:
    """Run one SQL file and return its rows."""
    sql_path = SQL_DIR / spec.filename
    sql = read_sql_file(sql_path)
    assert_read_only_query(sql, spec.filename)
    try:
        result = connection.execute(sql)
    except duckdb.Error as error:
        raise RuntimeError(
            f"DuckDB could not run {spec.filename}.\n"
            f"Reason: {error}"
        ) from error
    columns = [item[0] for item in result.description]
    rows = [dict(zip(columns, row)) for row in result.fetchall()]
    return QueryResult(spec=spec, columns=columns, rows=rows)


def print_query_result(query_result: QueryResult) -> None:
    """Print one query heading, its purpose, and a readable table."""
    print_section(query_result.spec.title)
    print(query_result.spec.purpose)
    print(f"SQL file: sql/{query_result.spec.filename}")
    print(f"Result rows: {len(query_result.rows):,}")
    print()
    if not query_result.rows:
        print("The query returned no rows.")
        return
    print_table(
        query_result.columns,
        [
            [format_cell(column, row[column]) for column in query_result.columns]
            for row in query_result.rows
        ],
    )


def run_analytics(path: Path) -> list[QueryResult]:
    """Run every catalog query. The Parquet file is only read."""
    ensure_processed_dataset_exists(path)
    before = dataset_signature(path)
    print(f"Processed input: {path}", flush=True)
    print(
        "Running read-only SQL files from the sql folder. "
        "This can take a few minutes.",
        flush=True,
    )

    connection = connect_to_dataset(path)
    results: list[QueryResult] = []
    try:
        for spec in QUERY_CATALOG:
            print(f"Running {spec.filename}", flush=True)
            query_result = execute_query(connection, spec)
            results.append(query_result)
            print_query_result(query_result)
    finally:
        connection.close()

    if dataset_signature(path) != before:
        raise RuntimeError(
            "The processed dataset changed while the SQL analytics were running.\n"
            f"  {path}"
        )

    print()
    print("SQL analytics finished. The processed dataset was only read.")
    return results


def main() -> None:
    """Run the SQL analytics and print a message when a query fails."""
    try:
        run_analytics(PROCESSED_DATASET_PATH)
    except (FileNotFoundError, RuntimeError, OSError) as error:
        print()
        print("Could not run the SQL analytics.")
        print(error)
        raise SystemExit(1) from error


# This guard runs main() only when you execute this file directly.
# Importing the file from another script does not query the dataset.
if __name__ == "__main__":
    main()
