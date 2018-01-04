#standardSQL
-- All rows from plx and etl tables, except:
--   internal test from EB.
--   blacklisted tests
SELECT *
FROM `${DATASET}.ndt_exhaustive`
WHERE
  -- not blacklisted
  (blacklist_flags = 0 OR
    (blacklist_flags IS NULL AND anomalies.blacklist_flags IS NULL))
  -- not from EB monitoring or unknown client
  AND web100_log_entry.connection_spec.local_ip IS NOT NULL
  AND web100_log_entry.connection_spec.remote_ip IS NOT NULL
  AND web100_log_entry.connection_spec.remote_ip != "45.56.98.222"
  AND web100_log_entry.connection_spec.remote_ip != "2600:3c03::f03c:91ff:fe33:819"
