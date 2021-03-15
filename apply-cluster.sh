#!/bin/bash
#
# apply-cluster.sh applies the k8s cluster configuration to the currently
# configured cluster. This script may be safely run multiple times to load the
# most recent configurations.
#
# Example:
#
#   PROJECT_ID=mlab-sandbox CLOUDSDK_CONTAINER_CLUSTER=data-processing ./apply-cluster.sh

set -x
set -e
set -u

USAGE="PROJECT_ID=<projectid> CLOUDSDK_CONTAINER_CLUSTER=<cluster> $0"
PROJECT_ID=${PROJECT_ID:?Please provide project id: $USAGE}
CLUSTER=${CLOUDSDK_CONTAINER_CLUSTER:?Please provide cluster name: $USAGE}
BIGQUERY_DATASET=${BIGQUERY_DATASET:-empty_tag}

# Apply templates
find k8s/${CLUSTER}/ -type f -exec \
    sed -i \
      -e 's/{{GIT_COMMIT}}/'${GIT_COMMIT}'/g' \
      -e 's/{{GCLOUD_PROJECT}}/'${PROJECT_ID}'/g' \
      -e 's/{{BIGQUERY_DATASET}}/'${BIGQUERY_DATASET}'/g' \
      {} \;

# This triggers deployment of the pod.
kubectl apply --recursive -f k8s/${CLUSTER}
