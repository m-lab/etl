#!/bin/bash
#
# Run a cmd using service account credentials.

set -x
set -e

PROJECT=${1:?Please provide the GCP project id}
KEYNAME=${2:?Please provide the service account keyname}
BASEDIR=${3:?Please provide the path to the command to run}
CMD=${4:?Please provide the binary name to run}

# Add gcloud to PATH.
source "${HOME}/google-cloud-sdk/path.bash.inc"
source $( dirname "${BASH_SOURCE[0]}" )/gcloudlib.sh

# Authenticate all operations using the given service account.
activate_service_account "${KEYNAME}"

local keyfile=$( mktemp )
set +x; echo "${!KEYNAME}" > ${keyfile}
export GOOGLE_APPLICATION_CREDENTIALS=${keyfile}

# For all options see:
# https://cloud.google.com/sdk/gcloud/reference/config/set
gcloud config set core/project "${PROJECT}"
gcloud config set core/disable_prompts true
gcloud config set core/verbosity info

# Make build artifacts available to docker build.
pushd "${BASEDIR}"

  # Automatically promote the new version to "serving".
  # For all options see:
  # https://cloud.google.com/sdk/gcloud/reference/app/deploy
  GCLOUD_PROJECT="${PROJECT}" "${CMD}"
popd

exit 0
