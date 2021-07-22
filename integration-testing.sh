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

gcloud auth list
gcloud config configurations list

set -x

# Prepare archives in mlab-testing project for some integration tests.
source ./travis/gcloudlib.sh
activate_service_account SERVICE_ACCOUNT_mlab_testing

gcloud auth list
gcloud config configurations list

gsutil cp testfiles/20210617T003002.410133Z-ndt7-mlab1-foo01-ndt.tgz \
    gs://archive-mlab-testing/ndt/ndt7/2021/06/17/20210617T003002.410133Z-ndt7-mlab1-foo01-ndt.tgz

# Run integration tests.
go test -v -tags=integration -coverprofile=_integration.cov ./...

gcloud auth list
gcloud config configurations activate default
gcloud auth list
