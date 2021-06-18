# etl

| branch | travis-ci | report-card | coveralls |
|--------|-----------|-----------|-------------|
| master | [![Travis Build Status](https://travis-ci.org/m-lab/etl.svg?branch=master)](https://travis-ci.org/m-lab/etl) | | [![Coverage Status](https://coveralls.io/repos/m-lab/etl/badge.svg?branch=master)](https://coveralls.io/github/m-lab/etl?branch=master) |
| integration | [![Travis Build Status](https://travis-ci.org/m-lab/etl.svg?branch=integration)](https://travis-ci.org/m-lab/etl) | [![Go Report Card](https://goreportcard.com/badge/github.com/m-lab/etl)](https://goreportcard.com/report/github.com/m-lab/etl) | [![Coverage Status](https://coveralls.io/repos/m-lab/etl/badge.svg?branch=integration)](https://coveralls.io/github/m-lab/etl?branch=integration) |

ETL (extract, transform, load) is a core component of the M-Lab data processing
pipeline. The ETL worker is responsible for parsing data archives produced by
[pusher](https://github.com/m-lab/pusher) and publishing M-Lab measurements to
[BigQuery](https://www.measurementlab.net/data/docs/bq/quickstart/).

## Local Development

```sh
go get ./cmd/etl_worker
~/bin/etl_worker -service_port :8080 -output_dir ./output -output local
```

From the command line (or with a browser) make a request to the `/v2/worker`
resource with a `filename=` parameter that names a valid M-Lab GCS archive.

```sh
URL=gs://archive-measurement-lab/ndt/ndt7/2021/06/14/20210614T003000.696927Z-ndt7-mlab1-yul04-ndt.tgz
curl "http://localhost:8080/v2/worker?filename=$URL"
```

## Generating Schema Docs

To build a new docker image with the `generate_schema_docs` command, run:

```sh
$ docker build -t measurementlab/generate-schema-docs .
$ docker run -v $PWD:/workspace -w /workspace \
  -it measurementlab/generate-schema-docs

Writing schema_ndtresultrow.md
...

```

## Moving to GKE

The universal parser will run in GKE, using parser-pool node pools, defined like this:

```sh
gcloud --project=mlab-sandbox container node-pools create parser-pool-1 \
  --cluster=data-processing   --num-nodes=3   --region=us-east1 \
  --scopes storage-ro,compute-rw,bigquery,datastore \
  --node-labels=parser-node=true   --enable-autorepair --enable-autoupgrade \
  --machine-type=n1-standard-16
```

The images come from gcr.io, and are built by google cloud build.  The build
trigger is currently found with:

```sh
gcloud beta builds triggers list --filter=m-lab/etl
```

Deployment requires adding cloud-kubernetes-deployer role to etl-travis-deploy@
in IAM.  This is done for sandbox and staging.

## Migrating to Sink interface

The parsers currently use etl.Inserter as the backend for writing records.
This API is overly shaped by bigquery, and complicates testing and extension.

The row.Sink interface, and row.Buffer define cleaner APIs for the back end
and for buffering and annotating.  This will streamline migration to
Gardener driven table selection, column partitioned tables, and possibly
future migration to BigQuery loads instead of streaming inserts.

## Factories

The TaskFactory aggregates a number of other factories for the elements
required for a Task.  Factory injection is used to generalize
ProcessGKETask, and simplify testing.

* SinkFactory produces a Sink for output.
* SourceFactory produces a Source for the input data.
* AnnotatorFactory produces an Annotator to be used to annotate rows.
