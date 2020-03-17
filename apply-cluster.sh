#!/bin/bash
#
# apply-cluster.sh applies the k8s cluster configuration to the currently
# configured cluster. This script may be safely run multiple times to load the
# most recent configurations.
#
# Example:
#
#   PROJECT=mlab-sandbox CLUSTER=scraper-cluster ./apply-cluster.sh

set -x
set -e
set -u

USAGE="PROJECT=<projectid> CLUSTER=<cluster> TRAVIS_TAG=<tag> TRAVIS_COMMIT=<commit> $0"
PROJECT=${PROJECT:?Please provide project id: $USAGE}
CLUSTER=${CLUSTER:?Please provide cluster name: $USAGE}
TRAVIS_TAG=${TRAVIS_TAG:-empty_tag}
BIGQUERY_DATASET=${BIGQUERY_DATASET:-empty_tag}
TRAVIS_COMMIT=${TRAVIS_COMMIT:?Please provide travis commit: $USAGE}

# Apply templates
CFG=/tmp/${CLUSTER}-${PROJECT}.yml
touch ${CFG}
pwd
kexpand expand --ignore-missing-keys k8s/${CLUSTER}/*/*.yml \
    --value GCLOUD_PROJECT=${PROJECT} \
    --value RELEASE_TAG=${TRAVIS_TAG} \
    --value GIT_COMMIT=${TRAVIS_COMMIT} \
    --value BIGQUERY_DATASET=${BIGQUERY_DATASET} \
    > ${CFG}
cat ${CFG}

# This triggers deployment of the pod.
kubectl apply -f ${CFG}
