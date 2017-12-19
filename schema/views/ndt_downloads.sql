#standardSQL
SELECT * FROM `${STANDARD_SUB}.ndt_sensible`
where
connection_spec.data_direction is not null
AND connection_spec.data_direction = 1
and web100_log_entry.snap.HCThruOctetsAcked is not null
AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
-- sum of SndLimTime is sensible - more than 9 seconds, less than 1 minute
and web100_log_entry.snap.SndLimTimeRwin is not null
and web100_log_entry.snap.SndLimTimeCwnd is not null
and web100_log_entry.snap.SndLimTimeSnd is not null
AND (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd) >= 9000000
AND (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd) < 60000000
-- Congestion was detected
-- Note that this removes a large portion of download tests!!!
and web100_log_entry.snap.CongSignals is not null and web100_log_entry.snap.CongSignals > 0