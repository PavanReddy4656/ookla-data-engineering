-- Overall download and upload speed for geographic tiles.
-- Each row in performance_tiles is one tile, not one speed test.
-- avg_d_kbps and avg_u_kbps are already tile-level averages stored in kbps.
-- 1 Mbps = 1,000 kbps. AVG and MEDIAN summarize tiles, not individual tests.

SELECT
    count(*) AS tile_count,
    avg(avg_d_kbps) / 1000.0 AS avg_download_mbps,
    median(avg_d_kbps) / 1000.0 AS median_download_mbps,
    avg(avg_u_kbps) / 1000.0 AS avg_upload_mbps,
    median(avg_u_kbps) / 1000.0 AS median_upload_mbps
FROM performance_tiles;
