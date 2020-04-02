#!/bin/bash
# NOTE: This only syncs the ndt prefix.  All other prefixes are unaffected.

set -x
set -e
tar xf ndt.tar
gsutil -m rsync -R -P -d ndt gs://archive-mlab-testing/ndt/
