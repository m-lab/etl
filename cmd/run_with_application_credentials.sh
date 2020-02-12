#!/bin/bash
#
# Run a cmd using service account credentials as application credentials.

set -x
set -e

PROJECT=${1:?Please provide the GCP project id}
KEYNAME=${2:?Please provide the service account keyname}
BASEDIR=${3:?Please provide the path to the command to run}
shift 3

keyfile=$( mktemp )
set +x; echo "${!KEYNAME}" > ${keyfile}
export GOOGLE_APPLICATION_CREDENTIALS=${keyfile}

set -x

pushd "${BASEDIR}"
  # Run command given on the rest of the command line.
  GCLOUD_PROJECT="${PROJECT}" $@
popd

exit 0
