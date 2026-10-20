"""Test the SQL analytics layer with a tiny temporary Parquet file.

These tests do not read the production dataset. They check speeds in Mbps,
test-count terciles, top-tile order, and data-quality counts.
"""

import sys
from pathlib import Path

import duckdb
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import run_sql_analytics  # noqa: E402


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


def sample_rows() -> list[dict[str, object]]:
    """Build five tiles with known speeds, nulls, and one invalid row each."""
    def row(quadkey: str, **overrides: object) -> dict[str, object]:
        values: dict[str, object] = {
            "quadkey": quadkey,
            "tile": quadkey,
            "tile_x": 10.0,
            "tile_y": 20.0,
            "avg_d_kbps": 1000,
            "avg_u_kbps": 500,
            "avg_lat_ms": 15,
            "avg_lat_down_ms": 8,
            "avg_lat_up_ms": 9,
            "tests": 1,
            "devices": 1,
        }
        values.update(overrides)
        return values

    return [
        row("a", avg_d_kbps=3000, avg_u_kbps=1500, avg_lat_ms=40, tests=9, devices=3),
        row("b", avg_d_kbps=3000, avg_u_kbps=1500, avg_lat_ms=30, tests=5, devices=2),
        row("c", avg_d_kbps=1000, avg_u_kbps=500, avg_lat_ms=15, avg_lat_down_ms=None, tests=1),
        row(
            "   ",
            tile="blank-key",
            avg_d_kbps=2000,
            avg_u_kbps=1000,
            avg_lat_ms=25,
            avg_lat_up_ms=None,
            tests=2,
            devices=1,
        ),
        row("d", avg_d_kbps=-1000, avg_u_kbps=500, avg_lat_ms=12, tests=4, devices=1),
    ]


def write_sample(path: Path, rows: list[dict[str, object]]) -> None:
    """Write sample tiles to a small Parquet file."""
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


def file_signature(path: Path) -> tuple[object, ...]:
    if not path.exists():
        return ("missing",)
    stat = path.stat()
    return ("file", stat.st_size, stat.st_mtime_ns)


@pytest.fixture
def protect_project_data() -> None:
    """Fail if a test changes the real raw or processed files."""
    watched = [
        PROJECT_ROOT / "data" / "raw" / "2024-10-01_performance_fixed_tiles.parquet",
        PROJECT_ROOT / "data" / "processed" / "2024-10-01_performance_fixed_tiles.parquet",
        PROJECT_ROOT / "data" / "processed" / "rejected_records.parquet",
    ]
    before = {path: file_signature(path) for path in watched}
    yield
    after = {path: file_signature(path) for path in watched}
    assert after == before


def result_by_filename(
    results: list[run_sql_analytics.QueryResult],
    filename: str,
) -> run_sql_analytics.QueryResult:
    matches = [result for result in results if result.spec.filename == filename]
    assert len(matches) == 1
    return matches[0]


def test_sql_files_are_read_only_selects() -> None:
    """Every catalog query is a SELECT stored under the project sql folder."""
    filenames = [spec.filename for spec in run_sql_analytics.QUERY_CATALOG]
    assert filenames == [
        "speed_summary.sql",
        "speed_by_test_count_tercile.sql",
        "top_tiles_by_download_speed.sql",
        "data_quality_checks.sql",
    ]
    for spec in run_sql_analytics.QUERY_CATALOG:
        sql_path = run_sql_analytics.SQL_DIR / spec.filename
        sql = sql_path.read_text(encoding="utf-8")
        assert run_sql_analytics.first_sql_keyword(sql) in {"SELECT", "WITH"}
        assert "performance_tiles" in sql
        run_sql_analytics.assert_read_only_query(sql, spec.filename)


def test_write_statement_is_rejected() -> None:
    """A SQL file that changes data is stopped before DuckDB runs it."""
    with pytest.raises(RuntimeError, match="read-only SELECT"):
        run_sql_analytics.assert_read_only_query(
            "INSERT INTO performance_tiles VALUES (1)",
            "example.sql",
        )


def test_analytics_queries_use_tile_speeds_and_quality_rules(
    tmp_path: Path,
    protect_project_data: None,
) -> None:
    """Run all four SQL files against five known tiles."""
    source = tmp_path / "tiles.parquet"
    write_sample(source, sample_rows())
    original = source.read_bytes()

    results = run_sql_analytics.run_analytics(source)

    assert source.read_bytes() == original
    speed = result_by_filename(results, "speed_summary.sql").rows[0]
    assert speed["tile_count"] == 5
    assert float(speed["avg_download_mbps"]) == pytest.approx(1.6)
    assert float(speed["median_download_mbps"]) == pytest.approx(2.0)
    assert float(speed["avg_upload_mbps"]) == pytest.approx(1.0)
    assert float(speed["median_upload_mbps"]) == pytest.approx(1.0)

    groups = result_by_filename(results, "speed_by_test_count_tercile.sql").rows
    assert [row["test_count_tercile"] for row in groups] == ["low", "medium", "high"]
    assert [row["tile_count"] for row in groups] == [2, 2, 1]
    assert groups[0]["min_tests"] == 1
    assert groups[0]["max_tests"] == 2
    assert float(groups[2]["avg_download_mbps"]) == pytest.approx(3.0)
    assert float(groups[2]["avg_latency_ms"]) == pytest.approx(40.0)

    top_tiles = result_by_filename(results, "top_tiles_by_download_speed.sql").rows
    assert [row["quadkey"] for row in top_tiles] == ["a", "b", "   ", "c", "d"]
    assert float(top_tiles[0]["download_mbps"]) == pytest.approx(3.0)
    assert top_tiles[0]["tests"] == 9
    assert top_tiles[0]["latency_ms"] == 40

    quality = result_by_filename(results, "data_quality_checks.sql").rows[0]
    assert quality["tile_count"] == 5
    assert quality["missing_downstream_latency"] == 1
    assert quality["missing_upstream_latency"] == 1
    assert quality["missing_either_directional_latency"] == 2
    assert float(quality["missing_downstream_latency_pct"]) == pytest.approx(20.0)
    assert quality["invalid_quadkey"] == 1
    assert quality["invalid_download_speed"] == 1
    assert quality["invalid_core_rows"] == 2
    assert quality["invalid_tests"] == 0
    assert quality["invalid_devices"] == 0


def test_missing_dataset_and_missing_sql_file_have_clear_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    protect_project_data: None,
) -> None:
    """A missing input or SQL file names the path and does not claim success."""
    missing = tmp_path / "missing.parquet"
    with pytest.raises(FileNotFoundError, match="processed dataset is missing"):
        run_sql_analytics.run_analytics(missing)

    source = tmp_path / "tiles.parquet"
    write_sample(source, sample_rows()[:1])
    monkeypatch.setattr(
        run_sql_analytics,
        "QUERY_CATALOG",
        [
            run_sql_analytics.QuerySpec(
                filename="missing_query.sql",
                title="Missing query",
                purpose="This file is not in the project.",
            )
        ],
    )
    with pytest.raises(FileNotFoundError, match="SQL file is missing"):
        run_sql_analytics.run_analytics(source)
