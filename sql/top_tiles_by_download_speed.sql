-- Top 100 geographic tiles by the tile's average download speed.
-- avg_d_kbps is the tile average in kbps. The result shows that speed in Mbps.
-- tests is the test count already stored on the tile.
-- Ties use tests, then quadkey, so the order stays the same on every run.

SELECT
    quadkey,
    avg_d_kbps / 1000.0 AS download_mbps,
    avg_u_kbps / 1000.0 AS upload_mbps,
    avg_lat_ms AS latency_ms,
    tests,
    devices
FROM performance_tiles
ORDER BY avg_d_kbps DESC, tests DESC, quadkey ASC
LIMIT 100;
