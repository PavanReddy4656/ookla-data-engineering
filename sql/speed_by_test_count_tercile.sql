-- Average download speed and latency by test-count tercile.
-- NTILE(3) splits tiles into three groups of nearly equal size.
-- Tiles are ordered by the tests value stored on the tile, then by quadkey.
-- Group 1 is the lowest test-count third. Group 3 is the highest third.
-- These groups are for this comparison only. They are not Ookla categories.
-- tests is a tile-level count, not a list of individual speed tests.

WITH grouped AS (
    SELECT
        ntile(3) OVER (
            ORDER BY tests ASC, quadkey ASC
        ) AS test_count_group,
        tests,
        avg_d_kbps,
        avg_lat_ms
    FROM performance_tiles
)
SELECT
    test_count_group,
    CASE test_count_group
        WHEN 1 THEN 'low'
        WHEN 2 THEN 'medium'
        WHEN 3 THEN 'high'
    END AS test_count_tercile,
    count(*) AS tile_count,
    min(tests) AS min_tests,
    max(tests) AS max_tests,
    avg(avg_d_kbps) / 1000.0 AS avg_download_mbps,
    avg(avg_lat_ms) AS avg_latency_ms
FROM grouped
GROUP BY test_count_group
ORDER BY test_count_group;
