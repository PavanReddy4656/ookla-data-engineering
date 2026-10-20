r"""Analyze the processed Ookla fixed-broadband dataset.

DuckDB runs read-only SQL on the Parquet file in data/processed.
The file is not loaded into pandas, and it is not modified.

From the project folder in PowerShell:

    .\.venv\Scripts\python.exe src\analyze_performance.py

The queries scan the full file, so the run can take a few minutes.
"""

from decimal import Decimal
from pathlib import Path

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

# The Parquet file stores speed in kilobits per second.
# 1 megabit per second = 1,000 kilobits per second.
KBPS_PER_MBPS = 1000


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


def print_section(title: str) -> None:
    """Print a heading that separates one analysis from the next."""
    print()
    print(title)
    print("-" * len(title))


def print_table(headers: list[str], rows: list[list[str]]) -> None:
    """Print a text table. The first column is left-aligned."""
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


def as_int(value: object) -> int:
    if value is None:
        return 0
    return int(value)


def as_float(value: object) -> float:
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def format_count(value: object) -> str:
    return f"{as_int(value):,}"


def format_mbps(kbps_value: object) -> str:
    """Convert a kbps result to Mbps for display."""
    if kbps_value is None:
        return "NULL"
    mbps = as_float(kbps_value) / KBPS_PER_MBPS
    return f"{mbps:,.3f} Mbps"


def format_ms(value: object) -> str:
    """Format a latency result. The stored unit is milliseconds."""
    if value is None:
        return "NULL"
    if isinstance(value, int):
        return f"{value:,} ms"
    return f"{as_float(value):,.2f} ms"


def format_percent(part: int, total: int) -> str:
    if total == 0:
        return "n/a"
    return f"{(part / total) * 100:.4f}%"


def fetch_rows(
    connection: duckdb.DuckDBPyConnection,
    sql: str,
    path: Path,
) -> list[dict[str, object]]:
    """Run one read-only query and return each result row as a dictionary."""
    result = connection.execute(sql, [str(path)])
    headers = [item[0] for item in result.description]
    return [dict(zip(headers, row)) for row in result.fetchall()]


def dataset_signature(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return (stat.st_size, stat.st_mtime_ns)


def print_dataset_size(summary: dict[str, object]) -> None:
    """Record count and distinct quadkey count."""
    print_section("Dataset size")
    print(f"Records: {format_count(summary['record_count'])}")
    print(f"Distinct quadkeys: {format_count(summary['distinct_quadkeys'])}")


def print_download_speed(summary: dict[str, object]) -> None:
    """Average, median, minimum, and maximum download speed."""
    print_section("Download speed")
    print("Stored in the file as kbps. Displayed here as Mbps.")
    print(f"Average: {format_mbps(summary['avg_download_kbps'])}")
    print(f"Median: {format_mbps(summary['median_download_kbps'])}")
    print(f"Minimum: {format_mbps(summary['min_download_kbps'])}")
    print(f"Maximum: {format_mbps(summary['max_download_kbps'])}")


def print_upload_speed(summary: dict[str, object]) -> None:
    """Average and median upload speed."""
    print_section("Upload speed")
    print("Stored in the file as kbps. Displayed here as Mbps.")
    print(f"Average: {format_mbps(summary['avg_upload_kbps'])}")
    print(f"Median: {format_mbps(summary['median_upload_kbps'])}")


def print_overall_latency(summary: dict[str, object]) -> None:
    """Average and median overall latency."""
    print_section("Overall latency")
    print("avg_lat_ms is the overall latency column. The unit is milliseconds.")
    print(f"Average: {format_ms(summary['avg_latency_ms'])}")
    print(f"Median: {format_ms(summary['median_latency_ms'])}")


def print_tile_count_sums(summary: dict[str, object]) -> None:
    """Sums of the tests and devices values already stored on each tile."""
    print_section("Tile-level test and device counts")
    print(
        "Each row is one map tile. tests and devices are counts already "
        "aggregated for that tile."
    )
    print(f"Sum of tile-level test counts: {format_count(summary['sum_tests'])}")
    print("This is not a count of individual test events.")
    print(f"Sum of tile-level device counts: {format_count(summary['sum_devices'])}")
    print("This is not a count of unique users or unique devices.")


def print_missing_latency(summary: dict[str, object]) -> None:
    """Counts and percentages for missing directional latency values."""
    print_section("Missing directional latency")
    print("Downstream latency is avg_lat_down_ms. Upstream latency is avg_lat_up_ms.")
    total_rows = as_int(summary["record_count"])
    missing_down = as_int(summary["missing_down_latency"])
    missing_up = as_int(summary["missing_up_latency"])
    print(
        "Rows missing downstream latency: "
        f"{format_count(missing_down)} ({format_percent(missing_down, total_rows)})"
    )
    print(
        "Rows missing upstream latency: "
        f"{format_count(missing_up)} ({format_percent(missing_up, total_rows)})"
    )


def print_top_download_speeds(rows: list[dict[str, object]]) -> None:
    """Ten tiles with the highest download speed."""
    print_section("10 tiles with the highest download speed")
    print(
        "Ordered by download speed, then by test count, then by quadkey. "
        "The quadkey tie-breaker keeps the same order on every run."
    )
    print()
    print_table(
        ["Quadkey", "Download", "Upload", "Latency", "Tests", "Devices"],
        [
            [
                str(row["quadkey"]),
                format_mbps(row["avg_d_kbps"]),
                format_mbps(row["avg_u_kbps"]),
                format_ms(row["avg_lat_ms"]),
                format_count(row["tests"]),
                format_count(row["devices"]),
            ]
            for row in rows
        ],
    )


def print_top_test_counts(rows: list[dict[str, object]]) -> None:
    """Ten tiles with the highest stored test count."""
    print_section("10 tiles with the highest test counts")
    print(
        "Ordered by the tile test count, then by download speed, then by quadkey."
    )
    print()
    print_table(
        ["Quadkey", "Tests", "Devices", "Download", "Upload", "Latency"],
        [
            [
                str(row["quadkey"]),
                format_count(row["tests"]),
                format_count(row["devices"]),
                format_mbps(row["avg_d_kbps"]),
                format_mbps(row["avg_u_kbps"]),
                format_ms(row["avg_lat_ms"]),
            ]
            for row in rows
        ],
    )


def print_test_count_groups(rows: list[dict[str, object]]) -> None:
    """Compare download speed and latency across three test-count groups."""
    print_section("Download speed and latency by test-count group")
    print("Tiles are split with NTILE(3), ordered by tests and then by quadkey.")
    print("Group 1 contains the tiles with the lowest test counts.")
    print("Group 3 contains the tiles with the highest test counts.")
    print("These names are only for this comparison. They are not Ookla categories.")
    print()

    group_names = {1: "low", 2: "medium", 3: "high"}
    if not rows:
        print("The file has no rows, so no groups were created.")
        return

    print_table(
        [
            "Group",
            "Tiles",
            "Min tests",
            "Max tests",
            "Avg download",
            "Median download",
            "Avg latency",
            "Median latency",
        ],
        [
            [
                f"{as_int(row['test_count_group'])} ({group_names.get(as_int(row['test_count_group']), 'unlabeled')})",
                format_count(row["tile_count"]),
                format_count(row["min_tests"]),
                format_count(row["max_tests"]),
                format_mbps(row["avg_download_kbps"]),
                format_mbps(row["median_download_kbps"]),
                format_ms(row["avg_latency_ms"]),
                format_ms(row["median_latency_ms"]),
            ]
            for row in rows
        ],
    )


def run_analysis(path: Path) -> None:
    """Run each read-only analysis and print it."""
    before = dataset_signature(path)
    print(f"Processed input: {path}", flush=True)
    print("Running read-only DuckDB queries. This can take a few minutes.", flush=True)

    # One summary query scans the file once for the overall metrics.
    summary_sql = """
        SELECT
            count(*) AS record_count,
            count(DISTINCT quadkey) AS distinct_quadkeys,
            avg(avg_d_kbps) AS avg_download_kbps,
            median(avg_d_kbps) AS median_download_kbps,
            min(avg_d_kbps) AS min_download_kbps,
            max(avg_d_kbps) AS max_download_kbps,
            avg(avg_u_kbps) AS avg_upload_kbps,
            median(avg_u_kbps) AS median_upload_kbps,
            avg(avg_lat_ms) AS avg_latency_ms,
            median(avg_lat_ms) AS median_latency_ms,
            sum(tests) AS sum_tests,
            sum(devices) AS sum_devices,
            count(*) FILTER (WHERE avg_lat_down_ms IS NULL) AS missing_down_latency,
            count(*) FILTER (WHERE avg_lat_up_ms IS NULL) AS missing_up_latency
        FROM read_parquet(?)
    """
    # If download speed ties, more tests ranks higher. quadkey breaks any remaining tie.
    top_download_sql = """
        SELECT
            quadkey,
            avg_d_kbps,
            avg_u_kbps,
            avg_lat_ms,
            tests,
            devices
        FROM read_parquet(?)
        ORDER BY avg_d_kbps DESC, tests DESC, quadkey ASC
        LIMIT 10
    """
    top_tests_sql = """
        SELECT
            quadkey,
            tests,
            devices,
            avg_d_kbps,
            avg_u_kbps,
            avg_lat_ms
        FROM read_parquet(?)
        ORDER BY tests DESC, avg_d_kbps DESC, quadkey ASC
        LIMIT 10
    """
    # NTILE(3) gives three groups of nearly equal size.
    # Ordering by tests, then quadkey, makes the boundary between groups stable.
    group_sql = """
        WITH grouped AS (
            SELECT
                ntile(3) OVER (ORDER BY tests ASC, quadkey ASC) AS test_count_group,
                tests,
                avg_d_kbps,
                avg_lat_ms
            FROM read_parquet(?)
        )
        SELECT
            test_count_group,
            count(*) AS tile_count,
            min(tests) AS min_tests,
            max(tests) AS max_tests,
            avg(avg_d_kbps) AS avg_download_kbps,
            median(avg_d_kbps) AS median_download_kbps,
            avg(avg_lat_ms) AS avg_latency_ms,
            median(avg_lat_ms) AS median_latency_ms
        FROM grouped
        GROUP BY test_count_group
        ORDER BY test_count_group
    """

    connection = duckdb.connect(database=":memory:")
    try:
        print("Query 1 of 4: overall metrics.", flush=True)
        summary_rows = fetch_rows(connection, summary_sql, path)
        if not summary_rows:
            raise RuntimeError("The summary query returned no row.")
        summary = summary_rows[0]

        print("Query 2 of 4: highest download speeds.", flush=True)
        top_download_rows = fetch_rows(connection, top_download_sql, path)

        print("Query 3 of 4: highest test counts.", flush=True)
        top_test_rows = fetch_rows(connection, top_tests_sql, path)

        print("Query 4 of 4: test-count groups.", flush=True)
        group_rows = fetch_rows(connection, group_sql, path)
    except duckdb.Error as error:
        raise RuntimeError(
            "DuckDB could not finish the analysis of:\n"
            f"  {path}\n"
            f"Reason: {error}"
        ) from error
    finally:
        connection.close()

    if dataset_signature(path) != before:
        raise RuntimeError(
            "The processed dataset changed during the analysis.\n"
            f"  {path}"
        )

    print_dataset_size(summary)
    print_download_speed(summary)
    print_upload_speed(summary)
    print_overall_latency(summary)
    print_tile_count_sums(summary)
    print_missing_latency(summary)
    print_top_download_speeds(top_download_rows)
    print_top_test_counts(top_test_rows)
    print_test_count_groups(group_rows)
    print()
    print("Analysis finished. The processed dataset was only read.")


def main() -> None:
    """Analyze the processed dataset and print a message when a read fails."""
    try:
        ensure_processed_dataset_exists(PROCESSED_DATASET_PATH)
        run_analysis(PROCESSED_DATASET_PATH)
    except (FileNotFoundError, RuntimeError) as error:
        print()
        print("Could not analyze the processed dataset.")
        print(error)
        raise SystemExit(1) from error


# This guard runs main() only when you execute this file directly.
# Importing the file from another script does not analyze the dataset.
if __name__ == "__main__":
    main()
