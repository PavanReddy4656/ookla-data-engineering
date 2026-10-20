"""Test the Ookla ETL validation rules with a tiny temporary Parquet file.

These tests do not read the production Parquet data. A fixture records the
size and modified time of the real data files and checks that they stay the same.
"""

import sys
from pathlib import Path

import duckdb
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

    counts = etl_pipeline.run_pipeline(
        source_path=source,
        valid_output_path=valid_output,
        rejected_output_path=rejected_output,
    )

    assert counts.input_rows == 1
    assert counts.valid_rows == 1
    assert counts.rejected_rows == 0
    assert counts.missing_either_latency_direction == 1
    assert valid_output.is_file()
    assert not rejected_output.exists()
    written = read_sample(valid_output)
    assert written[0]["avg_lat_down_ms"] is None
    assert written[0]["avg_lat_up_ms"] is None


def test_missing_input_file_reports_an_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    protect_project_data: None,
) -> None:
    """A missing source stops the command and does not print a success message."""
    missing = tmp_path / "missing.parquet"
    valid_output = tmp_path / "valid.parquet"
    rejected_output = tmp_path / "rejected.parquet"

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
