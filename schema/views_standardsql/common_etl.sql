 #standardSQL
-- ETL table projected into common schema, for union with PLX legacy data.
SELECT
  test_id,
  DATE(_partitiontime) AS partition_date,
  0 AS project, -- not included in ETL
  log_time,
  task_filename,
  parse_time,
  anomalies.blacklist_flags AS blacklist_flags,
  anomalies,
  STRUCT (connection_spec.client_af,
    connection_spec.client_application,
    connection_spec.client_browser,
    connection_spec.client_hostname,
    connection_spec.client_ip,
    connection_spec.client_kernel_version,
    connection_spec.client_os,
    connection_spec.client_version,
    connection_spec.data_direction,
    connection_spec.server_af,
    -- ETL pipeline currently drops the prefix, so we add it back here.
    CONCAT("ndt.iupui.", connection_spec.server_hostname) AS server_hostname,
    connection_spec.server_ip,
    connection_spec.server_kernel_version,
    connection_spec.tls,
    connection_spec.websockets,
    connection_spec.client_geolocation,
    connection_spec.server_geolocation)
  AS connection_spec,
  STRUCT(
    web100_log_entry.version,
    web100_log_entry.log_time,
    "" AS group_name,   -- not included in ETL
    web100_log_entry.connection_spec,
    STRUCT(
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
      -- mod(web100_log_entry.snap.StartTimeStamp, 1000000) AS StartTimeUsec, --Not needed in common schema
      web100_log_entry.snap.State, web100_log_entry.snap.SubsequentTimeouts, web100_log_entry.snap.SumRTT,
      web100_log_entry.snap.TimeStamps, web100_log_entry.snap.Timeouts, web100_log_entry.snap.WinScaleRcvd,
      web100_log_entry.snap.WinScaleSent, web100_log_entry.snap.X_OtherReductionsCM, web100_log_entry.snap.X_OtherReductionsCV,
      web100_log_entry.snap.X_Rcvbuf, web100_log_entry.snap.X_Sndbuf, web100_log_entry.snap.X_dbg1,
      web100_log_entry.snap.X_dbg2, web100_log_entry.snap.X_dbg3, web100_log_entry.snap.X_dbg4,
      web100_log_entry.snap.X_rcv_ssthresh, web100_log_entry.snap.X_wnd_clamp)
    AS snap)
  AS web100_log_entry
FROM `${PROJECT}.base_tables.ndt`
WHERE _PARTITIONTIME >= TIMESTAMP("2017-05-11 00:00:00")
