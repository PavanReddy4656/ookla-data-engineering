r"""Time three read-only SQL queries on the processed Ookla Parquet file.

DuckDB reads the file directly. The rows are not loaded into pandas, and
neither the raw nor the processed file is modified.

From the project folder in PowerShell:

    .\.venv\Scripts\python.exe src\benchmark_queries.py

Each query runs three times. The first time is reported on its own because
operating-system and DuckDB caches may make a later repeat different.
A shorter later time is not a performance improvement.
"""

from dataclasses import dataclass
from pathlib import Path
import time

import duckdb


# __file__ is this script. parent is src/, and parent.parent is the project folder.
# Paths built from that root work no matter which folder the terminal is in.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DATASET_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "2024-10-01_performance_fixed_tiles.parquet"
)

# Run 1 is the first measurement. Runs 2 and 3 are repeats of the same SQL.
TOTAL_RUNS = 3


@dataclass(frozen=True)
class QueryCase:
    """One analytical query and a short description of what it returns."""

    name: str
    explanation: str
    sql: str


@dataclass(frozen=True)
class QueryRun:
    """Timing for one execution after its result rows have been read."""

    run_number: int
    elapsed_seconds: float
    row_count: int


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
    """Print a heading that separates one part of the report from the next."""
    print()
    print(title)
    print("-" * len(title))


def query_cases() -> list[QueryCase]:
    """Return the three queries, using the same rules as the analysis script."""
    average_speed = QueryCase(
        name="Average and median download speed",
        explanation=(
            "Calculates the average and median of avg_d_kbps across every tile. "
            "This returns one row."
        ),
        sql="""
            SELECT
                avg(avg_d_kbps) AS avg_download_kbps,
                median(avg_d_kbps) AS median_download_kbps
            FROM read_parquet(?)
        """,
    )
    grouped_speed = QueryCase(
        name="Average download speed and latency by test-count group",
        explanation=(
            "Splits tiles with NTILE(3), ordered by tests and then by quadkey. "
            "Group 1 has the lowest test counts and group 3 has the highest. "
            "These groups are only for this query. They are not Ookla categories. "
            "The query returns the average download speed and average latency "
            "for each group."
        ),
        sql="""
            WITH grouped AS (
                SELECT
                    ntile(3) OVER (
                        ORDER BY tests ASC, quadkey ASC
                    ) AS test_count_group,
                    avg_d_kbps,
                    avg_lat_ms
                FROM read_parquet(?)
            )
            SELECT
                test_count_group,
                avg(avg_d_kbps) AS avg_download_kbps,
                avg(avg_lat_ms) AS avg_latency_ms
            FROM grouped
            GROUP BY test_count_group
            ORDER BY test_count_group
        """,
    )
    top_tiles = QueryCase(
        name="Top 100 tiles by download speed",
        explanation=(
            "Returns 100 tiles. Download speed is the sort key. "
            "When speeds tie, the tile with more tests comes first. "
            "quadkey is the final tie-breaker, so the order stays the same "
            "on every run."
        ),
        sql="""
            SELECT
                quadkey,
                avg_d_kbps,
                avg_u_kbps,
                avg_lat_ms,
                tests,
                devices
            FROM read_parquet(?)
            ORDER BY avg_d_kbps DESC, tests DESC, quadkey ASC
            LIMIT 100
        """,
    )
    return [average_speed, grouped_speed, top_tiles]


def measure_query(
    connection: duckdb.DuckDBPyConnection,
    sql: str,
    path: Path,
    run_number: int,
) -> QueryRun:
    """Time SQL execution and the work of reading every result row."""
    start = time.perf_counter()
    result = connection.execute(sql, [str(path)])
    row_count = 0
    for _row in result.fetchall():
        row_count += 1
    elapsed_seconds = time.perf_counter() - start
    return QueryRun(
        run_number=run_number,
        elapsed_seconds=elapsed_seconds,
        row_count=row_count,
    )


def format_seconds(elapsed_seconds: float) -> str:
    return f"{elapsed_seconds:.3f} seconds"


def print_query_runs(query: QueryCase, runs: list[QueryRun]) -> None:
    """Print the first run apart from the repeats."""
    print_section(query.name)
    print(query.explanation)
    print()

    first_run = runs[0]
    later_runs = runs[1:]
    print(
        f"Run {first_run.run_number} (first run): "
        f"{format_seconds(first_run.elapsed_seconds)}, "
        f"{first_run.row_count:,} result rows"
    )
    for run in later_runs:
        print(
            f"Run {run.run_number} (later run): "
            f"{format_seconds(run.elapsed_seconds)}, "
            f"{run.row_count:,} result rows"
        )

    if later_runs:
        later_average = sum(run.elapsed_seconds for run in later_runs) / len(later_runs)
        print(f"Average of later runs: {format_seconds(later_average)}")

    row_counts = {run.row_count for run in runs}
    if len(row_counts) == 1:
        print("Each run returned the same number of result rows.")
    else:
        print("The runs did not return the same number of result rows.")


def print_summary(results: list[tuple[QueryCase, list[QueryRun]]]) -> None:
    """Print one line per query with the first run and the later-run average."""
    print_section("Benchmark summary")
    print("Times are elapsed seconds measured with time.perf_counter().")
    print("The clock starts before the query and stops after every result row is read.")
    print()
    print(f"{'Query':<56} {'First run':>12} {'Later average':>16} {'Rows':>8}")
    print(f"{'-' * 56} {'-' * 12} {'-' * 16} {'-' * 8}")
    for query, runs in results:
        later_runs = runs[1:]
        later_average = sum(run.elapsed_seconds for run in later_runs) / len(later_runs)
        print(
            f"{query.name:<56} "
            f"{runs[0].elapsed_seconds:12.3f} "
            f"{later_average:16.3f} "
            f"{runs[0].row_count:8,}"
        )
    print()
    print("These queries do different amounts of work, so their times are not a ranking.")
    print("A shorter later run can come from data already held in a cache.")
    print("This benchmark does not report that difference as a performance improvement.")


def print_query_plan(
    connection: duckdb.DuckDBPyConnection,
    query: QueryCase,
    path: Path,
) -> None:
    """Print DuckDB's plan for one query. This is not a timed run."""
    print_section("Query plan")
    print(f"EXPLAIN for: {query.name}")
    print("The plan describes how DuckDB intends to run the SQL.")
    print("It is printed after the timings so it does not warm the cache first.")
    print()
    result = connection.execute(f"EXPLAIN {query.sql}", [str(path)])
    rows = result.fetchall()
    if not rows:
        print("DuckDB returned an empty query plan.")
        return
    for row in rows:
        for value in row:
            print(value)


def run_benchmark(path: Path) -> None:
    """Time each query three times, then print one query plan."""
    before = dataset_signature(path)
    cases = query_cases()
    print(f"Processed input: {path}", flush=True)
    print(
        "This reads the full file several times, so the benchmark can take "
        "several minutes.",
        flush=True,
    )
    print(
        "Each query runs "
        f"{TOTAL_RUNS} times on one in-memory DuckDB connection.",
        flush=True,
    )
    print(
        "The operating system and DuckDB may cache file data after the first read. "
        "That can change the time of a later run.",
        flush=True,
    )

    connection = duckdb.connect(database=":memory:")
    results: list[tuple[QueryCase, list[QueryRun]]] = []
    try:
        for case_number, query in enumerate(cases, start=1):
            print(
                f"Timing query {case_number} of {len(cases)}: {query.name}",
                flush=True,
            )
            runs = [
                measure_query(connection, query.sql, path, run_number)
                for run_number in range(1, TOTAL_RUNS + 1)
            ]
            results.append((query, runs))
        print_query_plan(connection, cases[1], path)
    except duckdb.Error as error:
        raise RuntimeError(
            "DuckDB could not finish the benchmark for:\n"
            f"  {path}\n"
            f"Reason: {error}"
        ) from error
    finally:
        connection.close()

    if dataset_signature(path) != before:
        raise RuntimeError(
            "The processed dataset changed during the benchmark.\n"
            f"  {path}"
        )

    for query, runs in results:
        print_query_runs(query, runs)
    print_summary(results)
    print()
    print("Benchmark finished. The processed dataset was only read.")


def main() -> None:
    """Benchmark the processed dataset and print a message when a read fails."""
    try:
        ensure_processed_dataset_exists(PROCESSED_DATASET_PATH)
        run_benchmark(PROCESSED_DATASET_PATH)
    except (FileNotFoundError, RuntimeError) as error:
        print()
        print("Could not benchmark the processed dataset.")
        print(error)
        raise SystemExit(1) from error


# This guard runs main() only when you execute this file directly.
# Importing the file from another script does not run the benchmark.
if __name__ == "__main__":
    main()
