-- Data-quality counts for the tile table.
-- This query only reports counts. It does not delete or change rows.
-- Missing avg_lat_down_ms or avg_lat_up_ms is allowed and is not an invalid core field.
-- The invalid core check matches the validation rule in src/etl_pipeline.py.
-- A tile can fail more than one check, so the separate counts can overlap.

SELECT
    count(*) AS tile_count,
    count(*) FILTER (WHERE avg_lat_down_ms IS NULL) AS missing_downstream_latency,
    count(*) FILTER (WHERE avg_lat_up_ms IS NULL) AS missing_upstream_latency,
    count(*) FILTER (
        WHERE avg_lat_down_ms IS NULL OR avg_lat_up_ms IS NULL
    ) AS missing_either_directional_latency,
    100.0 * count(*) FILTER (WHERE avg_lat_down_ms IS NULL)
        / nullif(count(*), 0) AS missing_downstream_latency_pct,
    100.0 * count(*) FILTER (WHERE avg_lat_up_ms IS NULL)
        / nullif(count(*), 0) AS missing_upstream_latency_pct,
    count(*) FILTER (
        WHERE quadkey IS NULL OR length(trim(quadkey)) = 0
    ) AS invalid_quadkey,
    count(*) FILTER (
        WHERE tile IS NULL OR length(trim(tile)) = 0
    ) AS invalid_tile,
    count(*) FILTER (
        WHERE tile_x IS NULL OR tile_x < -180 OR tile_x > 180
    ) AS invalid_tile_x,
    count(*) FILTER (
        WHERE tile_y IS NULL OR tile_y < -90 OR tile_y > 90
    ) AS invalid_tile_y,
    count(*) FILTER (
        WHERE avg_d_kbps IS NULL OR avg_d_kbps < 0
    ) AS invalid_download_speed,
    count(*) FILTER (
        WHERE avg_u_kbps IS NULL OR avg_u_kbps < 0
    ) AS invalid_upload_speed,
    count(*) FILTER (
        WHERE avg_lat_ms IS NULL OR avg_lat_ms < 0
    ) AS invalid_overall_latency,
    count(*) FILTER (
        WHERE tests IS NULL OR tests <= 0
    ) AS invalid_tests,
    count(*) FILTER (
        WHERE devices IS NULL OR devices <= 0
    ) AS invalid_devices,
    count(*) FILTER (
        WHERE NOT (
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
    ) AS invalid_core_rows
FROM performance_tiles;
