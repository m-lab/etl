#!/bin/bash

# Exit on error.
set -e

# Install test credentials for authentication:
# * gcloud commands will use the activated service account.
# * Go libraries will use the GOOGLE_APPLICATION_CREDENTIALS.
if [[ -z "$SERVICE_ACCOUNT_mlab_testing" ]] ; then
  echo "ERROR: testing service account is unavailable."
  exit 1
fi

echo "$SERVICE_ACCOUNT_mlab_testing" > $PWD/creds.json
# Make credentials available for Go libraries.
export GOOGLE_APPLICATION_CREDENTIALS=$PWD/creds.json
# TODO: update travis script to make this optional.
if [[ ! -f ${HOME}/google-cloud-sdk/path.bash.inc ]] ; then
  mkdir -p ${HOME}/google-cloud-sdk/ || :
  touch ${HOME}/google-cloud-sdk/path.bash.inc || :
fi
# Make credentials available for gcloud commands.
#travis/activate_service_account.sh SERVICE_ACCOUNT_mlab_testing

# NOTE: do this after setting the service account.
#gcloud config set project mlab-testing

go test -v -tags=integration -coverprofile=_integration.cov ./...
