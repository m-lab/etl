#standardSQL
-- All rows from plx and etl tables, except:
--   internal test from EB.
--   blacklisted tests
SELECT * FROM `${STANDARD_SUB}.ndt_exhaustive`
where 
-- not blacklisted
(blacklist_flags = 0 or
  (blacklist_flags is null and anomalies.blacklist_flags is null))
-- not from EB monitoring client
and web100_log_entry.connection_spec.local_ip is not null
and web100_log_entry.connection_spec.remote_ip is not null
and web100_log_entry.connection_spec.remote_ip != "45.56.98.222"
and web100_log_entry.connection_spec.remote_ip != "2600:3c03::f03c:91ff:fe33:819"