-- Run once when flights_current fails with CANNOT_CHANGE_DATASET_TYPE (STREAMING_TABLE → MATERIALIZED_VIEW).
-- Find the materialization table name if needed:
--   SHOW TABLES IN workspace.default LIKE '__materialization%flights_current%';

DROP TABLE IF EXISTS workspace.default.flights_current;
