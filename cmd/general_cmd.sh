#!/bin/bash
#
# Run a cmd using service account credentials.

set -x
set -e

PROJECT=${1:?Please provide the GCP project id}
KEYNAME=${2:?Please provide the service account keyname}
BASEDIR=${3:?Please provide the path to the command to run}
CMD=${4:?Please provide the binary name to run}

local keyfile=$( mktemp )
set +x; echo "${!KEYNAME}" > ${keyfile}
export GOOGLE_APPLICATION_CREDENTIALS=${keyfile}


# Make build artifacts available to docker build.
pushd "${BASEDIR}"

  # Automatically promote the new version to "serving".
  # For all options see:
  # https://cloud.google.com/sdk/gcloud/reference/app/deploy
  GCLOUD_PROJECT="${PROJECT}" "${CMD}"
popd

exit 0
