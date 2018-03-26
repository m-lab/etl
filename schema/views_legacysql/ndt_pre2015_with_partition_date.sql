#legacySQL
-- Plx data pre 2015, with _PARTITIONDATE passed through.
SELECT
  test_id,
  _PARTITIONDATE AS partition_date,
  project, log_time, task_filename, parse_time, blacklist_flags,
  anomalies.*,
  connection_spec.*,
  web100_log_entry.*
FROM [legacy.ndt_pre2015]
