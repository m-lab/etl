#!/bin/bash
# Notes:
# bq mk --view validates the view query, and fails if the query isn't valid.
# This means that each view must be created before being used in other
# view definitions.

###########################################################################
#                        The standardSQL views                            #
###########################################################################

# Note: -f doesn't work on view creation, so we remove existing view first.
bq rm -f measurement-lab:legacy.ndt_plx
bq mk \
--description='Single view across the two tables containing pre and post 2015
data from plx.google:ndt_all legacy fast table' \
--view='#standardSQL
-- Single view across the two tables containing pre and post 2015
-- data from plx.google:ndt_all legacy fast table
SELECT
  test_id, DATE(pt) AS partition_date,
  project, log_time, task_filename, parse_time,
  blacklist_flags, anomalies, connection_spec, web100_log_entry
FROM
-- _partitiontime isnt preserved across the union, and partition_date is not
-- populated in the underlying tables.  So this creates pt in the union, and it
-- is converted to and replaces partition_date in the outer query.
(select _partitiontime as pt, * from `measurement-lab.legacy.ndt`
union all
select _partitiontime as pt, * from `measurement-lab.legacy.ndt_pre2015`)' \
measurement-lab:legacy.ndt_plx


bq rm -f measurement-lab:legacy.ndt_all
bq mk \
--description='Combined view of plx legacy fast table, up to May 10, and new ETL table, from May 11, 2017 onward.

Includes blacklisted and EB tests, which should be removed before analysis.

Note that at present, data from May 10 to mid September does NOT have geo annotations.' \
--view='#standardSQL
# Combined view of plx legacy fast table, up to May 10, and
#  new ETL table, from May 10, 2017 onward.
# Includes blacklisted and EB tests, which should be removed before analysis.
# Note that at present, data from May 10 to mid September does NOT have geo annotations.
SELECT * FROM `measurement-lab.public.common_etl`
where partition_date > date("2017-05-10")
union all
select * from `measurement-lab.legacy.ndt_plx`' \
measurement-lab:legacy.ndt_all

###########################################################################
#                        The legacySQL views                              #
###########################################################################

bq rm -f measurement-lab:legacy.ndt_with_partition_date
bq mk \
--description='Partial plx data with populated partition_date field.' \
--view='#legacySQL
SELECT
  test_id, DATE(_partitiontime) AS partition_date,
  project, log_time, task_filename, parse_time,
  blacklist_flags, anomalies.*, connection_spec.*, web100_log_entry.*
FROM [measurement-lab:legacy.ndt]' \
measurement-lab:legacy.ndt_with_partition_date

bq rm -f measurement-lab:legacy.ndt_pre2015_with_partition_date
bq mk \
--description='Partial plx data with populated partition_date field.' \
--view='#legacySQL
SELECT
  test_id, DATE(_partitiontime) AS partition_date,
  project, log_time, task_filename, parse_time,
  blacklist_flags, anomalies.*, connection_spec.*, web100_log_entry.*
FROM [measurement-lab:legacy.ndt_pre2015]' \
measurement-lab:legacy.ndt_pre2015_with_partition_date

