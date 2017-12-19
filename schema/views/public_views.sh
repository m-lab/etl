#!/bin/bash
# Notes:
# 1. These are the public facing standard views.
# 2. All views filter out EB test results and all views filter out tests where the blacklist_flags field is NULL.
# 3. -f doesn't work on view creation, so we remove existing view first.
# 4. dot (.) cannot be used within a table name, so SemVer is expressed with _.
#
# bq mk --view validates the view query, and fails if the query isn't valid.
# This means that each view must be created before being used in other
# view definitions.
#
# ndt_all​ (standardSQL)
# Separate views for download and upload NDT tests (data ~ XX.XX.XXXX [date]):
# ​​​ndt_downloads (standardSQL)
# ndt_uploads (standardSQL)

###########################################################################
#                            Bash Options                                 #
###########################################################################
# Terminate on error.
set -e

###########################################################################
#                            Bash Parameters                              #
###########################################################################
# If this is specified, then we will also redirect this dataset.
ALIAS=$1

###########################################################################
#                        The standardSQL views                            #
###########################################################################
PUBLIC=measurement-lab:public_v3_1
INTERNAL=measurement-lab:internal_v3_1

#bq mk ${PUBLIC}
#bq mk ${INTERNAL}

VIEW=${INTERNAL}.common_etl
echo $VIEW
bq rm -f $VIEW
bq mk \
--description=\
'ETL table projected into common schema, for union with PLX legacy data.\n
This also add "ndt.iupui." prefix to the connection_spec.hostname field.' \
--view='#standardSQL
-- ETL table projected into common schema, for union with PLX legacy data.
SELECT
test_id, 
date(_partitiontime) as partition_date,
0 as project, -- not included in ETL
log_time,
task_filename,
parse_time,
anomalies.blacklist_flags as blacklist_flags,
anomalies,
struct (connection_spec.client_af,
connection_spec.client_application,
connection_spec.client_browser,
connection_spec.client_hostname,
connection_spec.client_ip,
connection_spec.client_kernel_version,
connection_spec.client_os,
connection_spec.client_version,
connection_spec.data_direction,
connection_spec.server_af,
concat("ndt.iupui.", connection_spec.server_hostname) as server_hostname,
connection_spec.server_ip,
connection_spec.server_kernel_version,
connection_spec.tls,
connection_spec.websockets,
connection_spec.client_geolocation,
connection_spec.server_geolocation)
as connection_spec,
struct(
web100_log_entry.version,
web100_log_entry.log_time,
"" as group_name,   -- not included in ETL
web100_log_entry.connection_spec,
struct(
web100_log_entry.snap.AbruptTimeouts, web100_log_entry.snap.ActiveOpen, web100_log_entry.snap.CERcvd,
web100_log_entry.snap.CongAvoid, web100_log_entry.snap.CongOverCount, web100_log_entry.snap.CongSignals,
web100_log_entry.snap.CountRTT, web100_log_entry.snap.CurAppRQueue, web100_log_entry.snap.CurAppWQueue,
web100_log_entry.snap.CurCwnd, web100_log_entry.snap.CurMSS, web100_log_entry.snap.CurRTO,
web100_log_entry.snap.CurReasmQueue, web100_log_entry.snap.CurRetxQueue, web100_log_entry.snap.CurRwinRcvd,
web100_log_entry.snap.CurRwinSent, web100_log_entry.snap.CurSsthresh, web100_log_entry.snap.CurTimeoutCount,
web100_log_entry.snap.DSACKDups, web100_log_entry.snap.DataSegsIn, web100_log_entry.snap.DataSegsOut,
web100_log_entry.snap.DupAcksIn, web100_log_entry.snap.DupAcksOut, web100_log_entry.snap.Duration,
web100_log_entry.snap.ECN, web100_log_entry.snap.FastRetran, web100_log_entry.snap.HCDataOctetsIn,
web100_log_entry.snap.HCDataOctetsOut, web100_log_entry.snap.HCThruOctetsAcked, web100_log_entry.snap.HCThruOctetsReceived,
web100_log_entry.snap.LimCwnd, web100_log_entry.snap.LimRwin, web100_log_entry.snap.LocalAddress,
web100_log_entry.snap.LocalAddressType, web100_log_entry.snap.LocalPort, web100_log_entry.snap.MSSRcvd,
web100_log_entry.snap.MaxAppRQueue, web100_log_entry.snap.MaxAppWQueue, web100_log_entry.snap.MaxMSS,
web100_log_entry.snap.MaxRTO, web100_log_entry.snap.MaxRTT, web100_log_entry.snap.MaxReasmQueue,
web100_log_entry.snap.MaxRetxQueue, web100_log_entry.snap.MaxRwinRcvd, web100_log_entry.snap.MaxRwinSent,
web100_log_entry.snap.MaxSsCwnd, web100_log_entry.snap.MaxSsthresh, web100_log_entry.snap.MinMSS,
web100_log_entry.snap.MinRTO, web100_log_entry.snap.MinRTT, web100_log_entry.snap.MinRwinRcvd,
web100_log_entry.snap.MinRwinSent, web100_log_entry.snap.MinSsthresh, web100_log_entry.snap.Nagle,
web100_log_entry.snap.NonRecovDA, web100_log_entry.snap.OctetsRetrans, web100_log_entry.snap.OtherReductions,
web100_log_entry.snap.PostCongCountRTT, web100_log_entry.snap.PostCongSumRTT, web100_log_entry.snap.PreCongSumCwnd,
web100_log_entry.snap.PreCongSumRTT, web100_log_entry.snap.QuenchRcvd, web100_log_entry.snap.RTTVar,
web100_log_entry.snap.RcvNxt, web100_log_entry.snap.RcvRTT, web100_log_entry.snap.RcvWindScale,
web100_log_entry.snap.RecInitial, web100_log_entry.snap.RemAddress, web100_log_entry.snap.RemPort,
web100_log_entry.snap.RetranThresh, web100_log_entry.snap.SACK, web100_log_entry.snap.SACKBlocksRcvd,
web100_log_entry.snap.SACKsRcvd, web100_log_entry.snap.SampleRTT, web100_log_entry.snap.SegsIn,
web100_log_entry.snap.SegsOut, web100_log_entry.snap.SegsRetrans, web100_log_entry.snap.SendStall,
web100_log_entry.snap.SlowStart, web100_log_entry.snap.SmoothedRTT, web100_log_entry.snap.SndInitial,
web100_log_entry.snap.SndLimBytesCwnd, web100_log_entry.snap.SndLimBytesRwin, web100_log_entry.snap.SndLimBytesSender,
web100_log_entry.snap.SndLimTimeCwnd, web100_log_entry.snap.SndLimTimeRwin, web100_log_entry.snap.SndLimTimeSnd,
web100_log_entry.snap.SndLimTransCwnd, web100_log_entry.snap.SndLimTransRwin, web100_log_entry.snap.SndLimTransSnd,
web100_log_entry.snap.SndMax, web100_log_entry.snap.SndNxt, web100_log_entry.snap.SndUna,
web100_log_entry.snap.SndWindScale, web100_log_entry.snap.SpuriousFrDetected, web100_log_entry.snap.StartTimeStamp,
-- mod(web100_log_entry.snap.StartTimeStamp, 1000000) as StartTimeUsec, --Not needed in common schema
web100_log_entry.snap.State, web100_log_entry.snap.SubsequentTimeouts, web100_log_entry.snap.SumRTT,
web100_log_entry.snap.TimeStamps, web100_log_entry.snap.Timeouts, web100_log_entry.snap.WinScaleRcvd,
web100_log_entry.snap.WinScaleSent, web100_log_entry.snap.X_OtherReductionsCM, web100_log_entry.snap.X_OtherReductionsCV,
web100_log_entry.snap.X_Rcvbuf, web100_log_entry.snap.X_Sndbuf, web100_log_entry.snap.X_dbg1,
web100_log_entry.snap.X_dbg2, web100_log_entry.snap.X_dbg3, web100_log_entry.snap.X_dbg4,
web100_log_entry.snap.X_rcv_ssthresh, web100_log_entry.snap.X_wnd_clamp) as snap)
as web100_log_entry
from `measurement-lab.public.ndt`' \
$VIEW

VIEW=${INTERNAL}.ndt_exhaustive
echo $VIEW
bq rm -f $VIEW
bq mk \
--description=\
'Combined view of plx legacy fast table, up to May 10, and new ETL table, from May 11, 2017 onward.\n
Includes blacklisted and EB tests, which should be removed before analysis.\n
Note that at present, data from May 10 to mid September does NOT have geo annotations.' \
--view='#standardSQL
# Combined view of plx legacy fast table, up to May 10, and
#  new ETL table, from May 10, 2017 onward.
# Includes blacklisted and EB tests, which should be removed before analysis.
# Note that at present, data from May 10 to mid September does NOT have geo annotations.
SELECT * FROM `'${INTERNAL/:/.}'.common_etl`
where partition_date > date("2017-05-10")
union all
select * from `measurement-lab.legacy.ndt_plx`' \
$VIEW

VIEW=${INTERNAL}.ndt_all
echo $VIEW
bq rm -f $VIEW
bq mk \
--description='View across the all NDT data except EB and blacklisted' \
--view='#standardSQL
-- All rows from plx and etl tables, except:
--   internal test from EB.
--   blacklisted tests
SELECT * FROM `'${INTERNAL/:/.}'.ndt_exhaustive`
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

VIEW=${INTERNAL}.ndt_sensible
echo $VIEW
bq rm -f $VIEW
bq mk \
--description='View across the all NDT data excluding EB, blacklisted,
 bad end state, short or very long duration' \
--view='#standardSQL
-- All sensible rows from plx and etl tables.
-- Excludes very short and very long tests, and tests with bad end state.
SELECT * FROM `'${INTERNAL/:/.}'.ndt_all`
where 
-- sensible TCP end state
web100_log_entry.snap.State is not null
AND (web100_log_entry.snap.State = 1 OR (web100_log_entry.snap.State >= 5 AND web100_log_entry.snap.State <= 11)) -- sensible final state
-- sensible test duration
and web100_log_entry.snap.Duration is not null
AND web100_log_entry.snap.Duration >= 9000000 AND web100_log_entry.snap.Duration < 60000000  -- between 9 seconds and 1 minute' \
$VIEW

VIEW=${INTERNAL}.ndt_downloads
echo $VIEW
bq rm -f $VIEW
bq mk \
--description='All good quality download tests' \
--view='#standardSQL
SELECT * FROM `'${INTERNAL/:/.}'.ndt_sensible`
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

VIEW=${INTERNAL}.ndt_uploads
echo $VIEW
bq rm -f $VIEW
bq mk \
--description='All good quality upload tests' \
--view='#standardSQL
#standardSQL
SELECT * FROM `'${INTERNAL/:/.}'.ndt_sensible`
where
connection_spec.data_direction is not null
-- is upload
AND connection_spec.data_direction is not null AND connection_spec.data_direction = 0
-- sensible total bytes received.
AND web100_log_entry.snap.HCThruOctetsReceived is not null AND web100_log_entry.snap.HCThruOctetsReceived >= 8192' \
$VIEW

##################################################################################
# These are the simple public views linking into the corresponding internal views.
##################################################################################

VIEW=${PUBLIC}.ndt_all
echo $VIEW
bq rm -f $VIEW
bq mk \
--description='View across the all NDT data except EB and blacklisted' \
--view='#standardSQL
SELECT * FROM `'${INTERNAL/:/.}'.ndt_all`' \
$VIEW

VIEW=${PUBLIC}.ndt_downloads
echo $VIEW
bq rm -f $VIEW
bq mk \
--description='All good quality download tests' \
--view='#standardSQL
SELECT * FROM `'${INTERNAL/:/.}'.ndt_downloads`' \
$VIEW

VIEW=${PUBLIC}.ndt_uploads
echo $VIEW
bq rm -f $VIEW
bq mk \
--description='All good quality upload tests' \
--view='#standardSQL
#standardSQL
SELECT * FROM `'${INTERNAL/:/.}'.ndt_uploads`' \
$VIEW

##################################################################################
# Redirect stable or alpha?
##################################################################################

[[ -z "$ALIAS" ]] && exit 0
echo "Setting $ALIAS alias"

VIEW=measurement-lab:${ALIAS}.ndt_all
echo $VIEW
bq rm -f $VIEW
bq mk \
--description='View across the all NDT data except EB and blacklisted' \
--view='#standardSQL
SELECT * FROM `'${INTERNAL/:/.}'.ndt_all`' \
$VIEW

VIEW=measurement-lab:${ALIAS}.ndt_downloads
echo $VIEW
bq rm -f $VIEW
bq mk \
--description='All good quality download tests' \
--view='#standardSQL
SELECT * FROM `'${INTERNAL/:/.}'.ndt_downloads`' \
$VIEW

VIEW=measurement-lab:${ALIAS}.ndt_uploads
echo $VIEW
bq rm -f $VIEW
bq mk \
--description='All good quality upload tests' \
--view='#standardSQL
#standardSQL
SELECT * FROM `'${INTERNAL/:/.}'.ndt_uploads`' \
$VIEW
