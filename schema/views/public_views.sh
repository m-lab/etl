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
#                            Bash Parameters                              #
###########################################################################
# If this is specified, then we will also redirect this dataset.
ALIAS=$1

###########################################################################
#                       Functions and Variables                           # 
###########################################################################

# These parameters determine the destination datasets, with semantic versioning.
PUBLIC=measurement-lab:public_v3_1
INTERNAL=measurement-lab:internal_v3_1

# Create datasets, e.g. for new versions.
# These lines may fail, so we run them before set -x
bq mk ${PUBLIC}
bq mk ${INTERNAL}

# Note: SQL param may use "" and ``, but should NOT use ''
# This function expects the sql filename in parameter 4
create_view() {
  DATASET=$1
  VIEW=$2
  DESCRIPTION=$3
  SQL="$(cat $4)"

  # All table FROM refs are to INTERNAL (or legacy) tables.
  export STANDARD_SUB=${INTERNAL/:/.}
  SQL=`echo "$SQL" | envsubst '$STANDARD_SUB'`

  echo $DATASET.$VIEW
  bq rm -f $DATASET.$VIEW
  bq mk \
    --description="${DESCRIPTION}" --view="$SQL" $DATASET.$VIEW

  # This fetches the new table description as json.
  bq show --format=prettyjson $DATASET.$VIEW > $VIEW.json
}

# Note: SQL param may use "" and ``, but should NOT use ''
# This function expects the actual query string in parameter 4
create_public_view() {
  DATASET=$1
  VIEW=$2
  DESCRIPTION=$3
  SQL="$4"

  echo $DATASET.$VIEW
  bq rm -f $DATASET.$VIEW
  bq mk \
    --description="${DESCRIPTION}" --view="$SQL" $DATASET.$VIEW

  # This fetches the new table description as json.
  bq show --format=prettyjson $DATASET.$VIEW > $DATASET.$VIEW.json
}

###########################################################################
#                        The standardSQL views                            #
###########################################################################

# Terminate on error.
set -e

create_view ${INTERNAL} common_etl \
  'ETL table projected into common schema, for union with PLX legacy data.
  This also adds "ndt.iupui." prefix to the connection_spec.hostname field.' \
  common_etl.sql

create_view ${INTERNAL} ndt_exhaustive \
  'Combined view of plx legacy fast table, up to May 10, and new ETL table, from May 11, 2017 onward.
  Includes blacklisted and EB tests, which should be removed before analysis.
  Note that at present, data from May 10 to mid September does NOT have geo annotations.' \
  ndt_exhaustive.sql

create_view ${INTERNAL} ndt_all \
  'View across the all NDT data except EB and blacklisted' \
  ndt_all.sql

create_view ${INTERNAL} ndt_sensible \
  'View across the all NDT data excluding EB, blacklisted,
  bad end state, short or very long duration' \
  ndt_sensible.sql

create_view ${INTERNAL} ndt_downloads \
  'All good quality download tests' \
  ndt_downloads.sql

create_view ${INTERNAL} ndt_uploads \
  'All good quality upload tests' \
  ndt_uploads.sql


##################################################################################
# These are the simple public views linking into the corresponding internal views.
##################################################################################

create_public_view ${PUBLIC} ndt_all \
  'View across the all NDT data except EB and blacklisted' \
  '#standardSQL
  SELECT * FROM `'${INTERNAL/:/.}'.ndt_all`'

create_public_view ${PUBLIC} ndt_downloads \
  'All good quality download tests' \
  '#standardSQL
  SELECT * FROM `'${INTERNAL/:/.}'.ndt_downloads`'

create_public_view ${PUBLIC} ndt_uploads \
  'All good quality upload tests' \
  '#standardSQL
  SELECT * FROM `'${INTERNAL/:/.}'.ndt_uploads`'

##################################################################################
# Redirect stable or alpha?
##################################################################################

[[ -z "$ALIAS" ]] && exit 0
echo "Setting $ALIAS alias"

create_public_view measurement-lab:${ALIAS} ndt_all \
  'View across the all NDT data except EB and blacklisted' \
  '#standardSQL
  SELECT * FROM `'${INTERNAL/:/.}'.ndt_all`'

create_public_view measurement-lab:${ALIAS} ndt_downloads \
  'All good quality download tests' \
  '#standardSQL
  SELECT * FROM `'${INTERNAL/:/.}'.ndt_downloads`'

create_public_view measurement-lab:${ALIAS} ndt_uploads \
  'All good quality upload tests' \
  '#standardSQL
  SELECT * FROM `'${INTERNAL/:/.}'.ndt_uploads`'
