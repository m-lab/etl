#!/bin/bash
# Example$ for i in $(seq -w 01 30); do ./convert-legacy-to-common.sh 2010-04-$i &  done

date=$1
solid=${date//-/}
bq query -n 0 --allow_large_results --nouse_legacy_sql --replace --noflatten_results --batch \
--destination_table measurement-lab:legacy.ndt\$$solid --parameter='start:TIMESTAMP:'$date' 00:00:00' \
'SELECT
test_id, 
# date(timestamp_micros(if (log_time > 0, 1000000*log_time, web100_log_entry.snap.StartTimeStamp))) as partition_date,
project, # Actually missing from ETL
timestamp_seconds(log_time) as log_time,
"" as task_filename,
timestamp_seconds(0) as parse_time,
blacklist_flags,  # - not working because of missing schema field
struct<no_meta boolean, snaplog_error boolean, num_snaps int64, blacklist_flags int64
    >(null, null, null, null) as anomalies,
# connection_spec
struct<
       client_af int64, client_application STRING, client_browser STRING, client_hostname STRING,
       client_ip STRING, client_kernel_version STRING, client_os STRING, client_version STRING,
       data_direction int64, server_af int64, server_hostname STRING, server_ip STRING,
       server_kernel_version STRING, tls BOOLEAN, websockets BOOLEAN,
       client_geolocation struct<area_code int64, city STRING, continent_code STRING, country_code STRING,
              country_code3 STRING, country_name STRING, latitude float64, longitude float64,
              metro_code int64, postal_code STRING, region STRING >,
       server_geolocation struct<area_code int64, city STRING, continent_code STRING, country_code STRING,
              country_code3 STRING, country_name STRING, latitude float64, longitude float64,
              metro_code int64, postal_code STRING, region STRING > >
( connection_spec.client_af, connection_spec.client_application, connection_spec.client_browser,
connection_spec.client_hostname, connection_spec.client_ip, connection_spec.client_kernel_version,
connection_spec.client_os, connection_spec.client_version, if (connection_spec.data_direction = 1, 1, 0),
connection_spec.server_af, connection_spec.server_hostname, connection_spec.server_ip,
connection_spec.server_kernel_version, null, null,
( connection_spec.client_geolocation.area_code, connection_spec.client_geolocation.city, connection_spec.client_geolocation.continent_code,
connection_spec.client_geolocation.country_code, connection_spec.client_geolocation.country_code3, connection_spec.client_geolocation.country_name,
connection_spec.client_geolocation.latitude, connection_spec.client_geolocation.longitude, connection_spec.client_geolocation.metro_code,
connection_spec.client_geolocation.postal_code, connection_spec.client_geolocation.region),
( connection_spec.client_geolocation.area_code, connection_spec.client_geolocation.city, connection_spec.client_geolocation.continent_code,
connection_spec.client_geolocation.country_code, connection_spec.client_geolocation.country_code3, connection_spec.client_geolocation.country_name,
connection_spec.client_geolocation.latitude, connection_spec.client_geolocation.longitude, connection_spec.client_geolocation.metro_code,
connection_spec.client_geolocation.postal_code, connection_spec.client_geolocation.region)
) as connection_spec,
struct (
web100_log_entry.version,
web100_log_entry.log_time,
web100_log_entry.group_name,   # not included in ETL
struct (
web100_log_entry.connection_spec.local_af,
web100_log_entry.connection_spec.local_ip,
web100_log_entry.connection_spec.local_port,
web100_log_entry.connection_spec.remote_ip,
web100_log_entry.connection_spec.remote_port
) as connection_spec,  # different order from plx
struct (
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
#mod(web100_log_entry.snap.StartTimeStamp, 1000000) as StartTimeUsec, #TODO - consider dropping this from ETL instead
web100_log_entry.snap.State, web100_log_entry.snap.SubsequentTimeouts, web100_log_entry.snap.SumRTT,
web100_log_entry.snap.TimeStamps, web100_log_entry.snap.Timeouts, web100_log_entry.snap.WinScaleRcvd,
web100_log_entry.snap.WinScaleSent, web100_log_entry.snap.X_OtherReductionsCM, web100_log_entry.snap.X_OtherReductionsCV,
web100_log_entry.snap.X_Rcvbuf, web100_log_entry.snap.X_Sndbuf, web100_log_entry.snap.X_dbg1,
web100_log_entry.snap.X_dbg2, web100_log_entry.snap.X_dbg3, web100_log_entry.snap.X_dbg4,
web100_log_entry.snap.X_rcv_ssthresh, web100_log_entry.snap.X_wnd_clamp) as snap
) as web100_log_entry
from `plx.google.m_lab.ndt.all` 
where starts_with(test_id, format_timestamp("%Y/%m/%d", @start))'

