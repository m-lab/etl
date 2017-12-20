#!/bin/bash
# Creates the complete set of public views for a given version.
# Also creates internal views that the public views are built on.
# This should generally be run from a travis deployment, and the
# arguments should be derived from the deployment tag.
# The following standardSQL views are created in the public dataset:
#    ndt_all​ - all (lightly filtered) tests, excluding EB,
#              blacklisted, short and very long tests.
#    Separate views for download and upload NDT tests:
# ​​​     ndt_downloads
#      ndt_uploads
#
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

set -u
###########################################################################
#                            Bash Parameters                              #
###########################################################################

USAGE="$0 <project> <public-dataset> <internal-dataset> <alias>"
PROJECT=${1:?Please provide the google cloud project: $USAGE}
PUBLIC=${PROJECT}:${2:?Please specify the public dataset: $USAGE}
INTERNAL=${PROJECT}:${3:?Please specify the internal dataset: $USAGE}
ALIAS=${PROJECT}:${4:?Please specify the alias dataset \{alpha|stable|none\}: $USAGE}

# TODO - check that public and internal aren't swapped?
# TODO - check that project is valid?

###########################################################################
#                        Expected evolution:                              #
###########################################################################
#
# The create_view function creates views in arbitrary datasets, but the
# intended use is to create views in datasets that use semantic versioning,
# and that are not intended for general public use.
#
# The create_view function also creates views in arbitrary datasets,
# but is intended for creating views in datasets that are intended for
# direct public use.  These may or may not be versioned, but should
# generally be simple SELECT * views on "internal" views.
#
# We expect this file to evolve over time, reflecting occasional changes in
# the source table schemas, more frequent changes in semantics and query
# details, and corresponding updates to version numbers.
#
# A Pull Request changing this file might thus:
#  update one or more of the .sql files
#  update the PUBLIC and INTERNAL tags, e.g. from v3_1 to v3_2
#
# Currently, the usage allows specifying as script param $4 an alias,
# which would cause, e.g., the alpha or stable dataset to be updated to
# point to the current version.  It isn't clear yet whether this is the
# best approach.  Perhaps instead, the stable/alpha distinction should be
# hardcoded into this file?  Or perhaps there should be a explicit
# variables defined here that indicates which dataset version stable and
# alpha aliases should point to for the current configuration.
#
# Alternatively:
# * Tags like X.Y.Z should trigger adding or overwriting the minor version
#   dataset, e.g. internal_3_2.  For a new minor version, alpha is redirected.
# * Tags like X.Y should link the major version, e.g. public_3, to the existing
#   minor version, e.g. internal_3_2, and also update stable.

###########################################################################
#                              Scenarios                                  #
###########################################################################
#
# Adding fields to the underlying table schema:
#   Update the schema
#   Update the SQL for views that should incorporate the new fields.
#      (once we have all data in a single table this may not be needed)
#   Update documentation
#   Test the script in sandbox
#   Tag the script with new minor version number, triggering deployment.
#      * The new deployment should deploy to the same major version number!


###########################################################################
#                       Functions and Variables                           # 
###########################################################################

# Note: SQL param may use "" and ``, but should NOT use ''
# This function expects the actual query string in parameter 4
# The SQL string will have any occurance of $STANDARD_SUB replaced
# with the dataset parameter.
# Parameters
#  dataset - project:dataset for the view, and for any $DATASET substitutions
#  view - name of view, e.g. ndt_all
#  description - description string for the view
#  sql - optional sql string.  If not provided, it is loaded from $view.sql
create_view() {
  local dataset=$1
  local view=$2
  local description=$3
  local sql=${4:-`cat $view.sql`}

  # Some FROM targets must link to specified dataset.
  # Substitute dataset name for STANDARD_SUB sql vars.
  sql=`echo "$sql" | DATASET=${dataset/:/.} envsubst '$DATASET'`

  echo $dataset.$view
  bq rm --force $dataset.$view
  bq mk \
    --description="${description}" --view="$sql" $dataset.$view

  # TODO - Travis should cat the bigquery.log on non-zero exit status.

  # This fetches the new table description as json.
  bq show --format=prettyjson $dataset.$view > $dataset.$view.json
}

###########################################################################
#                        The standardSQL views                            #
###########################################################################

# TODO - if running in travis, set -x

# Create datasets, e.g. for new versions.
# These lines may fail, so we run them before set -e
bq mk ${PUBLIC}
bq mk ${INTERNAL}

# Terminate on error.
set -e
create_view ${INTERNAL} common_etl \
  'ETL table projected into common schema, for union with PLX legacy data.
  This also adds "ndt.iupui." prefix to the connection_spec.hostname field.' \

create_view ${INTERNAL} ndt_exhaustive \
  'Combined view of plx legacy fast table, up to May 10, and new ETL table, from May 11, 2017 onward.
  Includes blacklisted and EB tests, which should be removed before analysis.
  Note that at present, data from May 10 to mid September does NOT have geo annotations.' \

create_view ${INTERNAL} ndt_all \
  'View across the all NDT data except EB and blacklisted' \

create_view ${INTERNAL} ndt_sensible \
  'View across the all NDT data excluding EB, blacklisted,
  bad end state, short or very long duration' \

create_view ${INTERNAL} ndt_downloads \
  'All good quality download tests' \

create_view ${INTERNAL} ndt_uploads \
  'All good quality upload tests' \


##################################################################################
# These are the simple public views linking into the corresponding internal views.
##################################################################################

create_view ${PUBLIC} ndt_all \
  'View across the all NDT data except EB and blacklisted' \
  '#standardSQL
  SELECT * FROM `'${INTERNAL/:/.}'.ndt_all`'

create_view ${PUBLIC} ndt_downloads \
  'All good quality download tests' \
  '#standardSQL
  SELECT * FROM `'${INTERNAL/:/.}'.ndt_downloads`'

create_view ${PUBLIC} ndt_uploads \
  'All good quality upload tests' \
  '#standardSQL
  SELECT * FROM `'${INTERNAL/:/.}'.ndt_uploads`'
}

#############################################################################
# Redirect stable, alpha, beta
#############################################################################

# If alias parameter is alpha, beta, or stable, this will create the
# corresponding alias. These datasets are assumed to already exist, so script
# does not try to create them.
# If last parameter is "none" then we skip this section and terminate.
# TODO - should link alpha and beta when stable is linked?

if [ "${ALIAS}" != "${PROJECT}:none" ];
then 
  echo "Linking $ALIAS alias"

  create_view ${ALIAS} ndt_all \
    'View across the all NDT data except EB and blacklisted' \
    '#standardSQL
    SELECT * FROM `'${INTERNAL/:/.}'.ndt_all`'

  create_view ${ALIAS} ndt_downloads \
    'All good quality download tests' \
    '#standardSQL
    SELECT * FROM `'${INTERNAL/:/.}'.ndt_downloads`'

  create_view ${ALIAS} ndt_uploads \
    'All good quality upload tests' \
    '#standardSQL
    SELECT * FROM `'${INTERNAL/:/.}'.ndt_uploads`'
fi