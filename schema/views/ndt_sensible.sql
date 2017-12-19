#standardSQL
-- All sensible rows from plx and etl tables.
-- Excludes very short and very long tests, and tests with bad end state.
SELECT * FROM `${STANDARD_SUB}.ndt_all`
where 
-- sensible TCP end state
web100_log_entry.snap.State is not null
AND (web100_log_entry.snap.State = 1 OR (web100_log_entry.snap.State >= 5 AND web100_log_entry.snap.State <= 11)) -- sensible final state
-- sensible test duration
and web100_log_entry.snap.Duration is not null
AND web100_log_entry.snap.Duration >= 9000000 AND web100_log_entry.snap.Duration < 60000000  -- between 9 seconds and 1 minute