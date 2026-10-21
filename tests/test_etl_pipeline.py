"""Test the Ookla ETL validation rules with a tiny temporary Parquet file.

These tests do not read the production Parquet data. A fixture records the
size and modified time of the real data files and checks that they stay the same.
"""

import logging
import sys
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import etl_pipeline  # noqa: E402


COLUMNS = [
    "quadkey",
    "tile",
    "tile_x",
    "tile_y",
    "avg_d_kbps",
    "avg_u_kbps",
    "avg_lat_ms",
    "avg_lat_down_ms",
    "avg_lat_up_ms",
    "tests",
    "devices",
]


def base_values() -> dict[str, object]:
    """Return one valid record. Individual tests change one field at a time."""
    return {
        "quadkey": "valid",
        "tile": "valid",
        "tile_x": 0.0,
        "tile_y": 0.0,
        "avg_d_kbps": 1000,
        "avg_u_kbps": 500,
        "avg_lat_ms": 20,
        "avg_lat_down_ms": 10,
        "avg_lat_up_ms": 15,
        "tests": 5,
        "devices": 2,
    }


def make_row(label: str, **overrides: object) -> dict[str, object]:
    """Build a sample row whose quadkey and tile both start as the case label."""
    row = base_values()
    row["quadkey"] = label
    row["tile"] = label
    row.update(overrides)
    return row


def label_for(row: dict[str, object]) -> str:
    """Find the case label stored on quadkey, or on tile when quadkey is blank."""
    for column in ("quadkey", "tile"):
        value = row[column]
        if isinstance(value, str) and value.strip():
            return value
    raise AssertionError(f"Sample row has no label: {row}")


def write_sample(path: Path, rows: list[dict[str, object]]) -> None:
    """Write sample rows to a small Parquet file with DuckDB."""
    connection = duckdb.connect(database=":memory:")
    try:
        connection.execute(
            """
            CREATE TABLE sample (
                quadkey VARCHAR,
                tile VARCHAR,
                tile_x DOUBLE,
                tile_y DOUBLE,
                avg_d_kbps BIGINT,
                avg_u_kbps BIGINT,
                avg_lat_ms BIGINT,
                avg_lat_down_ms INTEGER,
                avg_lat_up_ms INTEGER,
                tests BIGINT,
                devices BIGINT
            )
            """
        )
        if rows:
            connection.executemany(
                "INSERT INTO sample VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [tuple(row[column] for column in COLUMNS) for row in rows],
            )
        destination = path.resolve().as_posix().replace("'", "''")
        connection.execute(f"COPY sample TO '{destination}' (FORMAT PARQUET)")
    finally:
        connection.close()


def read_sample(path: Path) -> list[dict[str, object]]:
    """Read a Parquet file back without using pandas."""
    connection = duckdb.connect(database=":memory:")
    try:
        result = connection.execute(
            "SELECT * FROM read_parquet(?) ORDER BY quadkey, tile",
            [str(path)],
        )
        headers = [item[0] for item in result.description]
        return [dict(zip(headers, row)) for row in result.fetchall()]
    finally:
        connection.close()


def file_signature(path: Path) -> tuple[object, ...]:
    """Capture whether a project file exists, plus its size and modified time."""
    if not path.exists():
        return ("missing",)
    stat = path.stat()
    return ("file", stat.st_size, stat.st_mtime_ns)


@pytest.fixture
def protect_project_data() -> None:
    """Fail if a test changes the real raw or processed files."""
    watched = [
        etl_pipeline.RAW_DATASET_PATH,
        etl_pipeline.VALID_OUTPUT_PATH,
        etl_pipeline.REJECTED_OUTPUT_PATH,
    ]
    before = {path: file_signature(path) for path in watched}
    yield
    after = {path: file_signature(path) for path in watched}
    assert after == before


def test_validation_keeps_good_rows_and_rejects_bad_rows(
    tmp_path: Path,
    protect_project_data: None,
) -> None:
    """Run the pipeline on one small file that contains every validation case."""
    cases = [
        ("valid-complete", {}, True),
        ("valid-null-latency-directions", {"avg_lat_down_ms": None, "avg_lat_up_ms": None}, True),
        ("valid-down-latency-null", {"avg_lat_down_ms": None}, True),
        ("valid-up-latency-null", {"avg_lat_up_ms": None}, True),
        ("valid-edge-coordinates", {"tile_x": -180.0, "tile_y": -90.0}, True),
        ("valid-other-edge", {"tile_x": 180.0, "tile_y": 90.0}, True),
        ("valid-zero-speeds-and-latency", {"avg_d_kbps": 0, "avg_u_kbps": 0, "avg_lat_ms": 0}, True),
        ("valid-high-values", {"avg_d_kbps": 50_000_000, "avg_lat_ms": 999_999}, True),
        ("missing-quadkey", {"quadkey": None}, False),
        ("empty-quadkey", {"quadkey": ""}, False),
        ("blank-quadkey", {"quadkey": "   "}, False),
        ("missing-tile", {"tile": None}, False),
        ("empty-tile", {"tile": ""}, False),
        ("blank-tile", {"tile": "   "}, False),
        ("tile-x-too-high", {"tile_x": 180.1}, False),
        ("tile-x-too-low", {"tile_x": -180.1}, False),
        ("tile-y-too-high", {"tile_y": 90.1}, False),
        ("tile-y-too-low", {"tile_y": -90.1}, False),
        ("negative-download", {"avg_d_kbps": -1}, False),
        ("negative-upload", {"avg_u_kbps": -1}, False),
        ("negative-latency", {"avg_lat_ms": -1}, False),
        ("zero-tests", {"tests": 0}, False),
        ("negative-tests", {"tests": -1}, False),
        ("zero-devices", {"devices": 0}, False),
        ("negative-devices", {"devices": -1}, False),
    ]
    rows = [make_row(label, **overrides) for label, overrides, _expected in cases]
    expected_valid = {label for label, _overrides, expected in cases if expected}
    expected_rejected = {label for label, _overrides, expected in cases if not expected}

    source = tmp_path / "input.parquet"
    valid_output = tmp_path / "valid.parquet"
    rejected_output = tmp_path / "rejected.parquet"
    write_sample(source, rows)
    original_input = source.read_bytes()

    counts = etl_pipeline.run_pipeline(
        source_path=source,
        valid_output_path=valid_output,
        rejected_output_path=rejected_output,
    )

    assert source.read_bytes() == original_input
    assert counts.input_rows == len(rows)
    assert counts.valid_rows == len(expected_valid)
    assert counts.rejected_rows == len(expected_rejected)
    assert counts.valid_rows + counts.rejected_rows == counts.input_rows
    assert valid_output.is_file()
    assert rejected_output.is_file()
    assert not (tmp_path / "valid.parquet.partial").exists()
    assert not (tmp_path / "rejected.parquet.partial").exists()

    valid_rows = read_sample(valid_output)
    rejected_rows = read_sample(rejected_output)
    assert {label_for(row) for row in valid_rows} == expected_valid
    assert {label_for(row) for row in rejected_rows} == expected_rejected

    preserved = {
        label_for(row): row
        for row in valid_rows
        if label_for(row) in {
            "valid-null-latency-directions",
            "valid-down-latency-null",
            "valid-up-latency-null",
        }
    }
    both_null = preserved["valid-null-latency-directions"]
    assert both_null["avg_lat_down_ms"] is None
    assert both_null["avg_lat_up_ms"] is None
    assert preserved["valid-down-latency-null"]["avg_lat_down_ms"] is None
    assert preserved["valid-down-latency-null"]["avg_lat_up_ms"] == 15
    assert preserved["valid-up-latency-null"]["avg_lat_down_ms"] == 10
    assert preserved["valid-up-latency-null"]["avg_lat_up_ms"] is None

    # The three kept rows above are the only rows with a missing latency direction.
    assert counts.missing_either_latency_direction == 3


def test_all_valid_rows_do_not_write_a_rejected_file(
    tmp_path: Path,
    caplog: pytest.CaptureFixture[str],
    protect_project_data: None,
) -> None:
    """A clean input removes an older rejected file instead of leaving it behind."""
    source = tmp_path / "input.parquet"
    valid_output = tmp_path / "valid.parquet"
    rejected_output = tmp_path / "rejected.parquet"
    rejected_output.write_text("stale rejected file from an earlier run", encoding="utf-8")
    write_sample(
        source,
        [make_row("valid-with-null-latency", avg_lat_down_ms=None, avg_lat_up_ms=None)],
    )

    caplog.set_level(logging.INFO, logger="etl_pipeline")
    counts = etl_pipeline.run_pipeline(
        source_path=source,
        valid_output_path=valid_output,
        rejected_output_path=rejected_output,
    )

    assert counts.input_rows == 1
    assert counts.valid_rows == 1
    assert counts.rejected_rows == 0
    assert counts.rejection_percentage == 0.0
    assert counts.null_avg_lat_down_ms == 1
    assert counts.null_avg_lat_up_ms == 1
    assert counts.missing_either_latency_direction == 1
    assert valid_output.is_file()
    assert not rejected_output.exists()
    written = read_sample(valid_output)
    assert written[0]["avg_lat_down_ms"] is None
    assert written[0]["avg_lat_up_ms"] is None
    pipeline_logs = [record for record in caplog.records if record.name == "etl_pipeline"]
    assert any(record.getMessage() == "Rejected rows: 0." for record in pipeline_logs)
    assert not any(record.levelno == logging.WARNING for record in pipeline_logs)


def test_logs_counts_duration_and_rejected_rows(
    tmp_path: Path,
    caplog: pytest.CaptureFixture[str],
    protect_project_data: None,
) -> None:
    """A mixed file logs progress at INFO and the rejected rows at WARNING."""
    source = tmp_path / "input.parquet"
    valid_output = tmp_path / "valid.parquet"
    rejected_output = tmp_path / "rejected.parquet"
    write_sample(
        source,
        [
            make_row("kept", avg_lat_down_ms=None),
            make_row("negative-download", avg_d_kbps=-5),
        ],
    )

    caplog.set_level(logging.INFO, logger="etl_pipeline")
    counts = etl_pipeline.run_pipeline(
        source_path=source,
        valid_output_path=valid_output,
        rejected_output_path=rejected_output,
    )

    assert counts.input_rows == 2
    assert counts.valid_rows == 1
    assert counts.rejected_rows == 1
    messages = {
        record.levelno: record.getMessage()
        for record in caplog.records
        if record.name == "etl_pipeline"
    }
    info_messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "etl_pipeline" and record.levelno == logging.INFO
    ]
    warning_messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "etl_pipeline" and record.levelno == logging.WARNING
    ]
    assert "Pipeline started." in info_messages
    assert f"Input file: {source}" in info_messages
    assert f"Valid output file: {valid_output}" in info_messages
    assert f"Rejected output file: {rejected_output}" in info_messages
    assert "Rows read: 2" in info_messages
    assert f"Valid rows written: 1. Output file: {valid_output}" in info_messages
    assert any(message.startswith("Pipeline finished in ") for message in info_messages)
    assert warning_messages == [
        f"Validation rejected 1 row(s). Those rows were written to {rejected_output}."
    ]
    assert counts.rejection_percentage == 50.0
    assert counts.null_avg_lat_down_ms == 1
    assert counts.null_avg_lat_up_ms == 0
    summary = next(
        record.getMessage()
        for record in caplog.records
        if record.name == "etl_pipeline"
        and record.getMessage().startswith("Data-quality summary")
    )
    assert "Input rows: 2" in summary
    assert "Valid rows: 1" in summary
    assert "Rejected rows: 1" in summary
    assert "Rejection percentage: 50.00%" in summary
    assert "Null avg_lat_down_ms: 1" in summary
    assert "Null avg_lat_up_ms: 0" in summary
    assert f"Valid output file: {valid_output}" in summary
    assert "Duration:" in summary
    assert logging.ERROR not in messages
    kept = read_sample(valid_output)
    assert kept[0]["quadkey"] == "kept"
    assert kept[0]["avg_lat_down_ms"] is None


def test_missing_input_file_reports_an_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    protect_project_data: None,
) -> None:
    """A missing source stops the command and does not print a success message."""
    missing = tmp_path / "missing.parquet"
    valid_output = tmp_path / "valid.parquet"
    rejected_output = tmp_path / "rejected.parquet"
    caplog.set_level(logging.INFO, logger="etl_pipeline")

    with pytest.raises(FileNotFoundError, match="raw dataset is missing"):
        etl_pipeline.run_pipeline(
            source_path=missing,
            valid_output_path=valid_output,
            rejected_output_path=rejected_output,
        )

    assert not valid_output.exists()
    assert not rejected_output.exists()

    monkeypatch.setattr(etl_pipeline, "RAW_DATASET_PATH", missing)
    with pytest.raises(SystemExit) as caught:
        etl_pipeline.main()

    assert caught.value.code == 1
    output = capsys.readouterr().out
    assert "Could not build the processed dataset." in output
    assert "raw dataset is missing" in output
    assert str(missing) in output
    assert "Pipeline finished." not in output
    error_logs = [
        record
        for record in caplog.records
        if record.name == "etl_pipeline" and record.levelno == logging.ERROR
    ]
    assert error_logs
    assert all(record.exc_info is not None for record in error_logs)
    assert any(str(missing) in record.getMessage() for record in error_logs)
    assert not any(
        "Pipeline finished in " in record.getMessage()
        for record in caplog.records
        if record.name == "etl_pipeline"
    )


def test_unreadable_input_logs_the_database_error(
    tmp_path: Path,
    caplog: pytest.CaptureFixture[str],
    protect_project_data: None,
) -> None:
    """A file DuckDB cannot read is logged as an error and leaves no output."""
    source = tmp_path / "bad.parquet"
    valid_output = tmp_path / "valid.parquet"
    rejected_output = tmp_path / "rejected.parquet"
    source.write_text("this is not a parquet file", encoding="utf-8")

    caplog.set_level(logging.INFO, logger="etl_pipeline")
    with pytest.raises(RuntimeError, match="DuckDB could not finish"):
        etl_pipeline.run_pipeline(
            source_path=source,
            valid_output_path=valid_output,
            rejected_output_path=rejected_output,
        )

    assert not valid_output.exists()
    assert not rejected_output.exists()
    error_logs = [
        record
        for record in caplog.records
        if record.name == "etl_pipeline" and record.levelno == logging.ERROR
    ]
    assert len(error_logs) == 1
    message = error_logs[0].getMessage()
    assert "Pipeline failed after " in message
    assert str(source) in message
    assert "DuckDB could not finish" in message
    assert error_logs[0].exc_info is not None
    assert not any(
        record.levelno == logging.INFO and "Pipeline finished in " in record.getMessage()
        for record in caplog.records
        if record.name == "etl_pipeline"
    )


def test_rejection_percentage_is_zero_for_an_empty_input() -> None:
    """No input rows means there are no rejected rows to turn into a percentage."""
    assert etl_pipeline.calculate_rejection_percentage(0, 0) == 0.0
    assert etl_pipeline.calculate_rejection_percentage(0, 4) == 0.0
    assert etl_pipeline.calculate_rejection_percentage(1, 4) == 25.0


def test_missing_directional_latency_is_counted_and_kept(
    tmp_path: Path,
    caplog: pytest.CaptureFixture[str],
    protect_project_data: None,
) -> None:
    """Null downstream or upstream latency stays in the valid output."""
    source = tmp_path / "input.parquet"
    valid_output = tmp_path / "valid.parquet"
    rejected_output = tmp_path / "rejected.parquet"
    write_sample(
        source,
        [
            make_row("both-present"),
            make_row("down-null", avg_lat_down_ms=None),
            make_row("up-null", avg_lat_up_ms=None),
            make_row("both-null", avg_lat_down_ms=None, avg_lat_up_ms=None),
        ],
    )

    caplog.set_level(logging.INFO, logger="etl_pipeline")
    counts = etl_pipeline.run_pipeline(
        source_path=source,
        valid_output_path=valid_output,
        rejected_output_path=rejected_output,
    )

    assert counts.input_rows == 4
    assert counts.valid_rows == 4
    assert counts.rejected_rows == 0
    assert counts.rejection_percentage == 0.0
    assert counts.null_avg_lat_down_ms == 2
    assert counts.null_avg_lat_up_ms == 2
    assert counts.missing_either_latency_direction == 3
    written = {row["quadkey"]: row for row in read_sample(valid_output)}
    assert written["down-null"]["avg_lat_down_ms"] is None
    assert written["down-null"]["avg_lat_up_ms"] == 15
    assert written["up-null"]["avg_lat_up_ms"] is None
    assert written["both-null"]["avg_lat_down_ms"] is None
    assert written["both-null"]["avg_lat_up_ms"] is None
    summary = next(
        record.getMessage()
        for record in caplog.records
        if record.name == "etl_pipeline"
        and record.getMessage().startswith("Data-quality summary")
    )
    assert "Null avg_lat_down_ms: 2" in summary
    assert "Null avg_lat_up_ms: 2" in summary
    assert "Rejection percentage: 0.00%" in summary


def test_zero_row_input_reports_a_zero_rejection_percentage(
    tmp_path: Path,
    caplog: pytest.CaptureFixture[str],
    protect_project_data: None,
) -> None:
    """An empty Parquet file still completes and writes an empty valid file."""
    source = tmp_path / "input.parquet"
    valid_output = tmp_path / "valid.parquet"
    rejected_output = tmp_path / "rejected.parquet"
    rejected_output.write_text("stale rejected file", encoding="utf-8")
    write_sample(source, [])

    caplog.set_level(logging.INFO, logger="etl_pipeline")
    counts = etl_pipeline.run_pipeline(
        source_path=source,
        valid_output_path=valid_output,
        rejected_output_path=rejected_output,
    )

    assert counts.input_rows == 0
    assert counts.valid_rows == 0
    assert counts.rejected_rows == 0
    assert counts.rejection_percentage == 0.0
    assert counts.null_avg_lat_down_ms == 0
    assert counts.null_avg_lat_up_ms == 0
    assert valid_output.is_file()
    assert read_sample(valid_output) == []
    assert not rejected_output.exists()
    summary = next(
        record.getMessage()
        for record in caplog.records
        if record.name == "etl_pipeline"
        and record.getMessage().startswith("Data-quality summary")
    )
    assert "Input rows: 0" in summary
    assert "Rejection percentage: 0.00%" in summary
    assert f"Valid output file: {valid_output}" in summary
    assert summary.index("Duration:") < len(summary)
    pipeline_logs = [record for record in caplog.records if record.name == "etl_pipeline"]
    assert not any(record.levelno == logging.WARNING for record in pipeline_logs)


def parquet_fields(path: Path) -> list[tuple[str, str]]:
    """Return column names and data types, without Parquet file metadata."""
    schema = pq.read_schema(path)
    return [(field.name, str(field.type)) for field in schema]


def test_multiple_batches_keep_valid_rows_and_nulls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    protect_project_data: None,
) -> None:
    """Rows in later batches follow the same rules as rows in the first batch."""
    monkeypatch.setattr(etl_pipeline, "PARQUET_BATCH_ROWS", 2)
    calls = {"batches": 0}
    original_split = etl_pipeline.split_batch

    def count_batches(connection, batch, schema):
        calls["batches"] += 1
        return original_split(connection, batch, schema)

    monkeypatch.setattr(etl_pipeline, "split_batch", count_batches)
    source = tmp_path / "input.parquet"
    valid_output = tmp_path / "valid.parquet"
    rejected_output = tmp_path / "rejected.parquet"
    write_sample(
        source,
        [
            make_row("valid-a"),
            make_row("valid-b", avg_lat_down_ms=None),
            make_row("rejected", avg_d_kbps=-1),
            make_row("valid-c", avg_lat_up_ms=None),
        ],
    )

    counts = etl_pipeline.run_pipeline(
        source_path=source,
        valid_output_path=valid_output,
        rejected_output_path=rejected_output,
    )

    assert calls["batches"] == 2
    assert counts.input_rows == 4
    assert counts.valid_rows == 3
    assert counts.rejected_rows == 1
    assert counts.rejection_percentage == 25.0
    assert counts.null_avg_lat_down_ms == 1
    assert counts.null_avg_lat_up_ms == 1
    assert counts.missing_either_latency_direction == 2
    assert parquet_fields(valid_output) == parquet_fields(source)
    assert parquet_fields(rejected_output) == parquet_fields(source)
    valid_rows = {row["quadkey"]: row for row in read_sample(valid_output)}
    assert set(valid_rows) == {"valid-a", "valid-b", "valid-c"}
    assert valid_rows["valid-b"]["avg_lat_down_ms"] is None
    assert valid_rows["valid-b"]["avg_lat_up_ms"] == 15
    assert valid_rows["valid-c"]["avg_lat_down_ms"] == 10
    assert valid_rows["valid-c"]["avg_lat_up_ms"] is None
    rejected_rows = read_sample(rejected_output)
    assert [row["quadkey"] for row in rejected_rows] == ["rejected"]
    assert not (tmp_path / "valid.parquet.partial").exists()
    assert not (tmp_path / "rejected.parquet.partial").exists()
    assert not (tmp_path / etl_pipeline.DUCKDB_TEMP_DIR_NAME).exists()


def test_failure_after_the_first_batch_keeps_existing_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    protect_project_data: None,
) -> None:
    """A failed later batch removes partial files and does not replace outputs."""
    monkeypatch.setattr(etl_pipeline, "PARQUET_BATCH_ROWS", 2)
    source = tmp_path / "input.parquet"
    valid_output = tmp_path / "valid.parquet"
    rejected_output = tmp_path / "rejected.parquet"
    write_sample(
        source,
        [
            make_row("one"),
            make_row("two"),
            make_row("three"),
            make_row("four"),
        ],
    )
    original_valid = b"original-valid"
    original_rejected = b"original-rejected"
    valid_output.write_bytes(original_valid)
    rejected_output.write_bytes(original_rejected)
    calls = {"writes": 0}
    original_append = etl_pipeline.append_table

    def fail_on_second_batch(writer, path, schema, table):
        calls["writes"] += 1
        if calls["writes"] > 1:
            raise RuntimeError("simulated batch failure")
        return original_append(writer, path, schema, table)

    monkeypatch.setattr(etl_pipeline, "append_table", fail_on_second_batch)

    with pytest.raises(RuntimeError, match="simulated batch failure"):
        etl_pipeline.run_pipeline(
            source_path=source,
            valid_output_path=valid_output,
            rejected_output_path=rejected_output,
        )

    assert calls["writes"] == 2
    assert valid_output.read_bytes() == original_valid
    assert rejected_output.read_bytes() == original_rejected
    assert not (tmp_path / "valid.parquet.partial").exists()
    assert not (tmp_path / "rejected.parquet.partial").exists()
    assert not (tmp_path / etl_pipeline.DUCKDB_TEMP_DIR_NAME).exists()


def test_leftover_partial_files_are_replaced_by_a_successful_run(
    tmp_path: Path,
    protect_project_data: None,
) -> None:
    """Partial files left by a killed run are removed when a later run finishes."""
    source = tmp_path / "input.parquet"
    valid_output = tmp_path / "valid.parquet"
    rejected_output = tmp_path / "rejected.parquet"
    write_sample(source, [make_row("kept", avg_lat_down_ms=None)])
    valid_partial = tmp_path / "valid.parquet.partial"
    rejected_partial = tmp_path / "rejected.parquet.partial"
    valid_partial.write_bytes(b"partial file from a killed run")
    rejected_partial.write_bytes(b"partial file from a killed run")

    counts = etl_pipeline.run_pipeline(
        source_path=source,
        valid_output_path=valid_output,
        rejected_output_path=rejected_output,
    )

    assert counts.valid_rows == 1
    assert counts.rejected_rows == 0
    assert not valid_partial.exists()
    assert not rejected_partial.exists()
    written = read_sample(valid_output)
    assert written[0]["quadkey"] == "kept"
    assert written[0]["avg_lat_down_ms"] is None
    assert not rejected_output.exists()


def test_empty_input_preserves_the_source_schema(
    tmp_path: Path,
    protect_project_data: None,
) -> None:
    """An empty input still publishes a valid file with the original columns."""
    source = tmp_path / "input.parquet"
    valid_output = tmp_path / "valid.parquet"
    rejected_output = tmp_path / "rejected.parquet"
    rejected_output.write_text("stale rejected file", encoding="utf-8")
    write_sample(source, [])

    counts = etl_pipeline.run_pipeline(
        source_path=source,
        valid_output_path=valid_output,
        rejected_output_path=rejected_output,
    )

    assert counts.input_rows == 0
    assert counts.valid_rows == 0
    assert counts.rejected_rows == 0
    assert counts.rejection_percentage == 0.0
    assert valid_output.is_file()
    assert read_sample(valid_output) == []
    assert parquet_fields(valid_output) == parquet_fields(source)
    assert not rejected_output.exists()
    assert not (tmp_path / "valid.parquet.partial").exists()


def test_duckdb_uses_the_memory_limit_and_output_temp_directory(
    tmp_path: Path,
    protect_project_data: None,
) -> None:
    """The pipeline connection spills into the output folder with a fixed budget.

    DuckDB stores the 512MB decimal limit and reports it as 488.2 MiB.
    """
    source = tmp_path / "input.parquet"
    temp_dir = tmp_path / etl_pipeline.DUCKDB_TEMP_DIR_NAME
    connection = duckdb.connect(database=":memory:")
    try:
        etl_pipeline.configure_duckdb(connection, temp_dir, tmp_path, source)
        settings = dict(
            connection.execute(
                """
                SELECT name, value
                FROM duckdb_settings()
                WHERE name IN (
                    'memory_limit',
                    'threads',
                    'preserve_insertion_order',
                    'temp_directory'
                )
                """
            ).fetchall()
        )
    finally:
        connection.close()

    assert settings["memory_limit"] == "488.2 MiB"
    assert settings["threads"] == "1"
    assert settings["preserve_insertion_order"] == "false"
    assert Path(settings["temp_directory"]).resolve() == temp_dir.resolve()
    assert temp_dir.is_dir()
