#standardSQL
SELECT * FROM `${STANDARD_SUB}.ndt_sensible`
where
connection_spec.data_direction is not null
-- is upload
AND connection_spec.data_direction is not null AND connection_spec.data_direction = 0
-- sensible total bytes received.
AND web100_log_entry.snap.HCThruOctetsReceived is not null AND web100_log_entry.snap.HCThruOctetsReceived >= 8192