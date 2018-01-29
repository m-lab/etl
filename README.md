# etl
| branch | travis-ci | coveralls |
|--------|-----------|-----------|
| master | [![Travis Build Status](https://travis-ci.org/m-lab/etl.svg?branch=master)](https://travis-ci.org/m-lab/etl) | [![Coverage Status](https://coveralls.io/repos/m-lab/etl/badge.svg?branch=master)](https://coveralls.io/github/m-lab/etl?branch=master) |
| integration | [![Travis Build Status](https://travis-ci.org/m-lab/etl.svg?branch=integration)](https://travis-ci.org/m-lab/etl) | [![Coverage Status](https://coveralls.io/repos/m-lab/etl/badge.svg?branch=integration)](https://coveralls.io/github/m-lab/etl?branch=integration) |

[![Waffle.io](https://badge.waffle.io/m-lab/etl.svg?title=Ready)](http://waffle.io/m-lab/etl)

MeasurementLab data ingestion pipeline.

To create e.g., NDT table (should rarely be required!!!):
bq mk --time_partitioning_type=DAY --schema=schema/repeated.json mlab-sandbox:mlab_sandbox.ndt

Also see schema/README.md.
(Also see schema/README.md.)

# Deployment
The pipeline consists of several components, that should be deployed (initially) in the
following order:
1. BigQuery tables
1. ETL parsers
1. task queues
1. queue-pusher
1. cloud functions

Each of these must be deployed to the appropriate domain for the pipeline in
that domain to function.  We intend to have auto-deployment through travis, but
the travis scripts do not yet implement this for all components.

## Details
For many of gcloud commands, you will need $GOPATH or $GOROOT set to appropriate
values, e.g.
`export GOPATH=~/go`
(This will become moot when automation is fully implemented)

1. BigQuery tables

    `bq mk --time_partitioning_type=DAY --schema schema/repeated.json -t measurement-lab:public.ndt_delta`

1. ETL parsers
Commit code to dev or master branch.
Trigger deployment by tagging the commit with e.g. ndt-staging-v1.2.3

1. task queues

    `gcloud beta app deploy --project=mlab-staging appengine/queue.yaml`

    TODO - update deployment through travis.

1. queue-pusher

    `gcloud beta app deploy --project=mlab-staging appengine/queue_pusher/app.yaml`

1. cloud functions

    `gcloud beta functions deploy fileNotification \
    --stage-bucket=parser-functions-sandbox \
    --trigger-bucket=scraper-mlab-staging \
    --project=mlab-staging`

