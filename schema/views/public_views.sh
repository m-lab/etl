#!/bin/bash
# Notes:
# 1. These are the public facing standard views.
# 2. All views filter out EB test results and all views filter out tests where the blacklist_flags field is NULL.
# 3. -f doesn't work on view creation, so we remove existing view first.
#
# bq mk --view validates the view query, and fails if the query isn't valid.
# This means that each view must be created before being used in other
# view definitions.
#
# ndt_all_v3.1​ (standardSQL)
# Separate views for download and upload NDT tests (data ~ XX.XX.XXXX [date]):
# ​​​ndt_downloads_v3.1 (standardSQL)
# ndt_uploads_v3.1 (standardSQL)

###########################################################################
#                        The standardSQL views                            #
###########################################################################

VIEW=measurement-lab:public.ndt_all_v3_1
echo $VIEW
bq rm -f $VIEW
bq mk \
--description='View across the all NDT data except EB and blacklisted' \
--view='#standardSQL
-- All rows from plx and etl tables, except:
--   internal test from EB.
--   blacklisted tests
select * from `measurement-lab.legacy.ndt_all`
where 
-- not blacklisted
(blacklist_flags = 0 or
  (blacklist_flags is null and anomalies.blacklist_flags is null))
-- not from EB monitoring client
and web100_log_entry.connection_spec.local_ip is not null
and web100_log_entry.connection_spec.remote_ip is not null
and web100_log_entry.connection_spec.remote_ip != "45.56.98.222"
and web100_log_entry.connection_spec.remote_ip != "2600:3c03::f03c:91ff:fe33:819"' \
$VIEW

VIEW=measurement-lab:internal.ndt_all_sensible
echo $VIEW
bq rm -f $VIEW
bq mk \
--description='View across the all NDT data excluding EB, blacklisted,
 bad end state, short or very long duration' \
--view='#standardSQL
-- All sensible rows from plx and etl tables.
-- Excludes very short and very long tests, and tests with bad end state.
select * from `measurement-lab.public.ndt_all_v3_1`
where 
-- sensible TCP end state
web100_log_entry.snap.State is not null
AND (web100_log_entry.snap.State = 1 OR (web100_log_entry.snap.State >= 5 AND web100_log_entry.snap.State <= 11)) -- sensible final state
-- sensible test duration
and web100_log_entry.snap.Duration is not null
AND web100_log_entry.snap.Duration >= 9000000 AND web100_log_entry.snap.Duration < 60000000  -- between 9 seconds and 1 minute' \
$VIEW

VIEW=measurement-lab:public.​​​ndt_downloads_v3_1
echo $VIEW
bq rm -f $VIEW
bq mk \
--description='All good quality download tests' \
--view='#standardSQL
select * from `measurement-lab.internal.ndt_all_sensible`
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
and web100_log_entry.snap.CongSignals is not null and web100_log_entry.snap.CongSignals > 0' \
$VIEW

VIEW=measurement-lab:public.​​​ndt_uploads_v3_1
echo $VIEW
bq rm -f $VIEW
bq mk \
--description='All good quality upload tests' \
--view='#standardSQL
#standardSQL
select * from `measurement-lab.internal.ndt_all_sensible`
where
connection_spec.data_direction is not null
-- is upload
AND connection_spec.data_direction is not null AND connection_spec.data_direction = 0
-- sensible total bytes received.
AND web100_log_entry.snap.HCThruOctetsReceived is not null AND web100_log_entry.snap.HCThruOctetsReceived >= 8192' \
$VIEW