#!/bin/bash

set -e
set -x
set -u

pushd $TRAVIS_BUILD_DIR/cmd/etl_worker
cat $1 | sed 's/__COMMIT_HASH__/$TRAVIS_COMMIT/' | sed 's/__RELEASE_TAG__/$TRAVIS_TAG/' | $2
cat $2
popd

echo $2
