r"""Build the processed Ookla dataset from the raw Parquet file.

DuckDB reads the raw file and writes valid rows to data/processed.
Rejected rows are written only when at least one row fails validation.
The raw file is never modified, and pandas is not used.

From the project folder in PowerShell:

    .\.venv\Scripts\python.exe src\etl_pipeline.py

The script scans the full file, so the run can take a few minutes.
"""

from dataclasses import dataclass
import logging
import os
from pathlib import Path
import time

import duckdb


# INFO is normal progress. WARNING means some rows failed validation
# but were still written to the rejected file. ERROR means the run stopped.
logger = logging.getLogger(__name__)


# __file__ is this script. parent is src/, and parent.parent is the project folder.
# Paths built from that root work no matter which folder the terminal is in.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATASET_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "2024-10-01_performance_fixed_tiles.parquet"
)
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
VALID_OUTPUT_PATH = PROCESSED_DIR / "2024-10-01_performance_fixed_tiles.parquet"
REJECTED_OUTPUT_PATH = PROCESSED_DIR / "rejected_records.parquet"

# A row is valid only when every line inside this expression is true.
# avg_lat_down_ms and avg_lat_up_ms are intentionally left out: a null in
# either latency-direction column is kept and does not reject the row.
# There is no cutoff for high speed or high latency.
VALID_RECORD_CONDITION = """
(
    quadkey IS NOT NULL
    AND length(trim(quadkey)) > 0
    AND tile IS NOT NULL
    AND length(trim(tile)) > 0
    AND tile_x IS NOT NULL
    AND tile_x >= -180
    AND tile_x <= 180
    AND tile_y IS NOT NULL
    AND tile_y >= -90
    AND tile_y <= 90
    AND avg_d_kbps IS NOT NULL
    AND avg_d_kbps >= 0
    AND avg_u_kbps IS NOT NULL
    AND avg_u_kbps >= 0
    AND avg_lat_ms IS NOT NULL
    AND avg_lat_ms >= 0
    AND tests IS NOT NULL
    AND tests > 0
    AND devices IS NOT NULL
    AND devices > 0
)
"""


@dataclass
class PipelineCounts:
    """Row counts calculated from the raw file before any output is written."""

    input_rows: int
    valid_rows: int
    rejected_rows: int
    null_avg_lat_down_ms: int
    null_avg_lat_up_ms: int
    missing_either_latency_direction: int
    elapsed_seconds: float = 0.0
    valid_output_path: Path | None = None

    @property
    def rejection_percentage(self) -> float:
        """Percent of input rows that failed validation.

        An empty file has no rejected rows, so the percentage is 0.
        """
        return calculate_rejection_percentage(self.rejected_rows, self.input_rows)


def calculate_rejection_percentage(rejected_rows: int, input_rows: int) -> float:
    """Calculate rejected rows as a percent of input rows."""
    if input_rows == 0:
        return 0.0
    return (rejected_rows / input_rows) * 100


def configure_logging() -> None:
    """Show log messages on the console when this file is run directly.

    Tests can capture the same messages without this setup. basicConfig
    does nothing if logging was already configured.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def ensure_raw_dataset_exists(path: Path) -> None:
    """Stop with a clear message when the raw input file is absent."""
    if path.is_file():
        return

    if path.exists():
        raise FileNotFoundError(
            "The raw dataset path exists, but it is not a file:\n"
            f"  {path}"
        )

    raise FileNotFoundError(
        "The raw dataset is missing. Looked for it here:\n"
        f"  {path}\n"
        "Place the downloaded Parquet file at that path, then run this script again."
    )


def assert_safe_output(path: Path, allowed_dir: Path, source_path: Path) -> None:
    """Allow a write only inside the selected output folder.

    The command line selects data/processed. Tests select a temporary folder.
    The file being read, and anything inside data/raw, are never destinations.
    """
    output_path = path.resolve()
    allowed = allowed_dir.resolve()
    source = source_path.resolve()
    raw_file = RAW_DATASET_PATH.resolve()
    raw_dir = raw_file.parent

    if output_path == source or output_path == raw_file:
        raise RuntimeError(
            "Refusing to overwrite the input file.\n"
            f"  {output_path}"
        )
    if output_path == raw_dir or raw_dir in output_path.parents:
        raise RuntimeError(
            "Refusing to write inside data/raw.\n"
            f"  {output_path}"
        )
    if output_path != allowed and allowed not in output_path.parents:
        raise RuntimeError(
            "Refusing to write outside the selected output folder.\n"
            f"  Output: {output_path}\n"
            f"  Allowed folder: {allowed}"
        )


def sql_file_path(path: Path) -> str:
    """Turn a Windows path into a SQL string DuckDB can write to."""
    return path.resolve().as_posix().replace("'", "''")


def as_int(value: object) -> int:
    if value is None:
        return 0
    return int(value)


def collect_counts(
    connection: duckdb.DuckDBPyConnection,
    source: Path,
) -> PipelineCounts:
    """Count input, valid, rejected, and missing latency rows in one query."""
    count_sql = f"""
        SELECT
            count(*) AS input_rows,
            count(*) FILTER (WHERE {VALID_RECORD_CONDITION}) AS valid_rows,
            count(*) FILTER (WHERE NOT {VALID_RECORD_CONDITION}) AS rejected_rows,
            count(*) FILTER (WHERE avg_lat_down_ms IS NULL) AS null_avg_lat_down_ms,
            count(*) FILTER (WHERE avg_lat_up_ms IS NULL) AS null_avg_lat_up_ms,
            count(*) FILTER (
                WHERE avg_lat_down_ms IS NULL OR avg_lat_up_ms IS NULL
            ) AS missing_either_latency_direction
        FROM read_parquet(?)
    """
    result = connection.execute(count_sql, [str(source)])
    headers = [item[0] for item in result.description]
    row = result.fetchone()
    if row is None:
        raise RuntimeError("The count query finished without returning a row.")
    values = dict(zip(headers, row))

    counts = PipelineCounts(
        input_rows=as_int(values["input_rows"]),
        valid_rows=as_int(values["valid_rows"]),
        rejected_rows=as_int(values["rejected_rows"]),
        null_avg_lat_down_ms=as_int(values["null_avg_lat_down_ms"]),
        null_avg_lat_up_ms=as_int(values["null_avg_lat_up_ms"]),
        missing_either_latency_direction=as_int(values["missing_either_latency_direction"]),
    )
    accounted_rows = counts.valid_rows + counts.rejected_rows
    if accounted_rows != counts.input_rows:
        raise RuntimeError(
            "Valid and rejected counts do not add up to the input count.\n"
            f"  Input rows: {counts.input_rows:,}\n"
            f"  Valid rows: {counts.valid_rows:,}\n"
            f"  Rejected rows: {counts.rejected_rows:,}\n"
            "No processed files were written."
        )
    return counts


def write_filtered_parquet(
    connection: duckdb.DuckDBPyConnection,
    source: Path,
    destination: Path,
    where_sql: str,
    expected_rows: int,
    allowed_dir: Path,
) -> None:
    """Write a filtered copy, then replace the previous output file.

    The query selects every original column, so latency nulls stay null.
    A temporary file is published with os.replace only after its row count
    matches the count from the validation query.
    """
    assert_safe_output(destination, allowed_dir, source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial_path = destination.with_name(destination.name + ".partial")
    assert_safe_output(partial_path, allowed_dir, source)
    if partial_path.exists():
        partial_path.unlink()

    copy_sql = f"""
        COPY (
            SELECT *
            FROM read_parquet(?)
            WHERE {where_sql}
        ) TO '{sql_file_path(partial_path)}' (FORMAT PARQUET)
    """
    try:
        connection.execute(copy_sql, [str(source)])
        written_row = connection.execute(
            "SELECT count(*) FROM read_parquet(?)",
            [str(partial_path)],
        ).fetchone()
        written_rows = as_int(written_row[0] if written_row else None)
        if written_rows != expected_rows:
            raise RuntimeError(
                "The output file does not contain the expected number of rows.\n"
                f"  File: {partial_path}\n"
                f"  Expected rows: {expected_rows:,}\n"
                f"  Written rows: {written_rows:,}"
            )
        os.replace(partial_path, destination)
    except Exception:
        if partial_path.exists():
            partial_path.unlink()
        raise


def remove_stale_rejected_file(
    rejected_output_path: Path,
    allowed_dir: Path,
    source_path: Path,
) -> None:
    """Remove a rejected-record file left by an earlier run."""
    assert_safe_output(rejected_output_path, allowed_dir, source_path)
    if rejected_output_path.exists():
        rejected_output_path.unlink()


def format_quality_summary(counts: PipelineCounts) -> str:
    """Build the end-of-run data-quality summary.

    Nulls in the two latency-direction columns are reported here.
    Those nulls do not make a row invalid.
    """
    output_path = counts.valid_output_path or "not available"
    lines = [
        "Data-quality summary",
        "---------------------",
        f"Input rows: {counts.input_rows:,}",
        f"Valid rows: {counts.valid_rows:,}",
        f"Rejected rows: {counts.rejected_rows:,}",
        f"Rejection percentage: {counts.rejection_percentage:.2f}%",
        f"Null avg_lat_down_ms: {counts.null_avg_lat_down_ms:,}",
        f"Null avg_lat_up_ms: {counts.null_avg_lat_up_ms:,}",
        f"Valid output file: {output_path}",
        f"Duration: {counts.elapsed_seconds:.3f} seconds",
    ]
    return "\n".join(lines)


def file_size_text(path: Path) -> str:
    size_bytes = path.stat().st_size
    size_mb = size_bytes / (1024 * 1024)
    return f"{size_bytes:,} bytes ({size_mb:.1f} MB)"


def build_outputs(
    connection: duckdb.DuckDBPyConnection,
    counts: PipelineCounts,
    source_path: Path,
    valid_output_path: Path,
    rejected_output_path: Path,
) -> None:
    """Write this run's output files. Earlier files in that folder may be replaced."""
    if valid_output_path.parent.resolve() != rejected_output_path.parent.resolve():
        raise RuntimeError(
            "Valid and rejected outputs must be written to the same folder.\n"
            f"  Valid: {valid_output_path}\n"
            f"  Rejected: {rejected_output_path}"
        )

    allowed_dir = valid_output_path.parent
    print("Writing valid records.", flush=True)
    write_filtered_parquet(
        connection,
        source_path,
        valid_output_path,
        VALID_RECORD_CONDITION,
        counts.valid_rows,
        allowed_dir,
    )

    if counts.rejected_rows > 0:
        print("Writing rejected records.", flush=True)
        write_filtered_parquet(
            connection,
            source_path,
            rejected_output_path,
            f"NOT {VALID_RECORD_CONDITION}",
            counts.rejected_rows,
            allowed_dir,
        )
        return

    print("No records failed validation.", flush=True)
    remove_stale_rejected_file(rejected_output_path, allowed_dir, source_path)


def run_pipeline(
    source_path: Path | None = None,
    valid_output_path: Path | None = None,
    rejected_output_path: Path | None = None,
) -> PipelineCounts:
    """Validate a Parquet file and write valid and rejected outputs.

    Called with no arguments, this uses the real files in data/raw and
    data/processed. Tests pass temporary paths so the real files stay put.
    """
    started = time.perf_counter()
    source = RAW_DATASET_PATH if source_path is None else Path(source_path)
    valid_output = VALID_OUTPUT_PATH if valid_output_path is None else Path(valid_output_path)
    rejected_output = (
        REJECTED_OUTPUT_PATH
        if rejected_output_path is None
        else Path(rejected_output_path)
    )
    logger.info("Pipeline started.")
    logger.info("Input file: %s", source)
    logger.info("Valid output file: %s", valid_output)
    logger.info("Rejected output file: %s", rejected_output)
    try:
        ensure_raw_dataset_exists(source)
        raw_signature = (source.stat().st_size, source.stat().st_mtime_ns)
        print(f"Raw input: {source}", flush=True)
        print("Counting rows with a read-only DuckDB query.", flush=True)

        connection = duckdb.connect(database=":memory:")
        try:
            counts = collect_counts(connection, source)
            logger.info("Rows read: %s", f"{counts.input_rows:,}")
            build_outputs(connection, counts, source, valid_output, rejected_output)
        except duckdb.Error as error:
            raise RuntimeError(
                "DuckDB could not finish the pipeline for:\n"
                f"  {source}\n"
                f"Reason: {error}"
            ) from error
        finally:
            connection.close()

        current_signature = (source.stat().st_size, source.stat().st_mtime_ns)
        if current_signature != raw_signature:
            raise RuntimeError(
                "The input file changed during the pipeline.\n"
                f"  {source}"
            )

        logger.info(
            "Valid rows written: %s. Output file: %s",
            f"{counts.valid_rows:,}",
            valid_output,
        )
        if counts.rejected_rows > 0:
            logger.warning(
                "Validation rejected %s row(s). Those rows were written to %s.",
                f"{counts.rejected_rows:,}",
                rejected_output,
            )
        else:
            logger.info("Rejected rows: 0.")
        counts.elapsed_seconds = time.perf_counter() - started
        counts.valid_output_path = valid_output
        logger.info("%s", format_quality_summary(counts))
        logger.info("Pipeline finished in %.3f seconds.", counts.elapsed_seconds)
        return counts
    except Exception as error:
        elapsed_seconds = time.perf_counter() - started
        logger.exception(
            "Pipeline failed after %.3f seconds. Input file: %s. %s",
            elapsed_seconds,
            source,
            error,
        )
        raise


def print_report(counts: PipelineCounts) -> None:
    """Print the pipeline metrics only after the outputs are in place."""
    print()
    print("Validation rules")
    print("----------------")
    print("quadkey and tile must be present and not blank.")
    print("tile_x must be from -180 to 180, and tile_y from -90 to 90.")
    print("Download speed, upload speed, and overall latency must be zero or greater.")
    print("tests and devices must be greater than zero.")
    print("Null avg_lat_down_ms and avg_lat_up_ms values are preserved.")
    print("High speed and high latency values are preserved.")

    print()
    print("Row counts")
    print("----------")
    print(f"Input rows: {counts.input_rows:,}")
    print(f"Valid rows: {counts.valid_rows:,}")
    print(f"Rejected rows: {counts.rejected_rows:,}")
    print(
        "Input rows missing at least one latency-direction value: "
        f"{counts.missing_either_latency_direction:,}"
    )

    print()
    print("Output files")
    print("------------")
    print(f"Valid records: {VALID_OUTPUT_PATH}")
    print(f"  Size: {file_size_text(VALID_OUTPUT_PATH)}")
    if counts.rejected_rows > 0:
        print(f"Rejected records: {REJECTED_OUTPUT_PATH}")
        print(f"  Size: {file_size_text(REJECTED_OUTPUT_PATH)}")
    else:
        print("Rejected records: no file was needed.")
        print("  No records failed validation, so rejected_records.parquet was not written.")

    print()
    print(format_quality_summary(counts))
    print()
    print("Pipeline finished. The raw dataset was only read.")


def main() -> None:
    """Run the pipeline and print a message when a read or write fails."""
    configure_logging()
    try:
        counts = run_pipeline()
    except (FileNotFoundError, RuntimeError, OSError) as error:
        print()
        print("Could not build the processed dataset.")
        print(error)
        raise SystemExit(1) from error

    print_report(counts)


# This guard runs main() only when you execute this file directly.
# Importing the file from another script does not build the dataset.
if __name__ == "__main__":
    main()
