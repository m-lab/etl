#legacySQL
--  All good quality download tests
SELECT *
FROM [${DATASET}.ndt_sensible_legacysql]
WHERE
  connection_spec.data_direction IS NOT NULL
  AND connection_spec.data_direction = 1
  AND web100_log_entry.snap.HCThruOctetsAcked IS NOT NULL
  AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
  -- sum of SndLimTime is sensible - more than 9 seconds, less than 1 minute
  AND web100_log_entry.snap.SndLimTimeRwin IS NOT NULL
  AND web100_log_entry.snap.SndLimTimeCwnd IS NOT NULL
  AND web100_log_entry.snap.SndLimTimeSnd IS NOT NULL
  AND (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd) >= 9000000
  AND (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd) < 60000000
  -- Congestion was detected
  -- Note that this removes a large portion of download tests!!!
  AND web100_log_entry.snap.CongSignals IS NOT NULL AND web100_log_entry.snap.CongSignals > 0