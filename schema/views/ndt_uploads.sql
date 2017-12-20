#standardSQL
--  All good quality upload tests
SELECT * FROM `${DATASET}.ndt_sensible`
WHERE
connection_spec.data_direction IS NOT NULL
-- is upload
AND connection_spec.data_direction IS NOT NULL AND connection_spec.data_direction = 0
-- sensible total bytes received.
AND web100_log_entry.snap.HCThruOctetsReceived IS NOT NULL AND web100_log_entry.snap.HCThruOctetsReceived >= 8192