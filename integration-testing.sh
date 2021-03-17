#!/bin/bash

# Exit on error.
set -e

if [[ -z "$SERVICE_ACCOUNT_mlab_testing" ]] ; then
  echo "WARNING: testing service account is unavailable."
  echo "WARNING: not running integration tests."
  exit 0
fi

# Go libraries use the GOOGLE_APPLICATION_CREDENTIALS.
echo "$SERVICE_ACCOUNT_mlab_testing" > $PWD/creds.json
export GOOGLE_APPLICATION_CREDENTIALS=$PWD/creds.json

# Run integration tests.
go test -v -tags=integration -coverprofile=_integration.cov ./...
