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

# Note: SQL param may use "" and ``, but should NOT use ''
create_view() {
  VIEW=$1
  DESCRIPTION=$2
  SQL="$(cat $3)"


  echo $VIEW
  bq rm -f $VIEW
  bq mk \
    --description=\'"${DESCRIPTION}"\' --view="$SQL" ${VIEW}
}

#echo $SQL

create_view ${INTERNAL}.common_etl \
'ETL table projected into common schema, for union with PLX legacy data.\n
This also add "ndt.iupui." prefix to the connection_spec.hostname field.' \
common_etl.sql

exit 0

create_view ${INTERNAL}.ndt_exhaustive \
'Combined view of plx legacy fast table, up to May 10, and new ETL table, from May 11, 2017 onward.\n
Includes blacklisted and EB tests, which should be removed before analysis.\n
Note that at present, data from May 10 to mid September does NOT have geo annotations.' \
ndt_exhaustive.sql


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
