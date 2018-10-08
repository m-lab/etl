#!/bin/bash
#
# Modifies the appengine app.yaml service to include the sandbox user name. If
# this is not on a sandbox branch, then no action is taken.

set -x
set -e

SCRIPT=${1:?Please provide the deployment script}
PROJECT=${2:?Please provide the GCP project id}
KEYFILE=${3:?Please provide the service account key file}
BASEDIR=${4:?Please provide the base directory containing yaml file}
YAML=${5:?Please provide yaml file name, e.g. app-ndt.yaml}

if [[ -f ${BASEDIR}/${YAML} ]] ; then
  if [[ -n "${TRAVIS_BRANCH}" ]] ; then
    if [[ ${TRAVIS_BRANCH} == sandbox-* ]] ; then
      user=${TRAVIS_BRANCH##sandbox-}
      sed -i -e 's/^service: \(.*\)/service: \1-'${user}'/' \
          "${BASEDIR}/${YAML}"
    fi
  fi
fi

# Call actual script to deploy service.
"${SCRIPT}" "${PROJECT}" "${KEYFILE}" "${BASEDIR}" "${YAML}"
