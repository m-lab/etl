#!/bin/bash
# This script substitutes TRAVIS env variables for placeholders in deployment yaml files.
# It overwrites the specified yaml file with the substituted variables.
#
# Example:
#   ./app-tags.sh cmd/etl_worker app-ndt.yaml

BASEDIR=${1:?Please provide the base directory containing yaml file}
YAML=${2:?Please provide yaml file name, e.g. app-ndt.yaml}

set -e
set -x
set -u
pushd $TRAVIS_BUILD_DIR/cmd/etl_worker
yaml_text =`cat $YAML`
echo $yaml_text | sed "s/__COMMIT_HASH__/$TRAVIS_COMMIT/" | sed "s/__RELEASE_TAG__/$TRAVIS_TAG/" > $YAML
cat $YAML
popd
