# etl
| branch | travis-ci | report-card | coveralls |
|--------|-----------|-----------|-------------|
| master | [![Travis Build Status](https://travis-ci.org/m-lab/etl.svg?branch=master)](https://travis-ci.org/m-lab/etl) | | [![Coverage Status](https://coveralls.io/repos/m-lab/etl/badge.svg?branch=master)](https://coveralls.io/github/m-lab/etl?branch=master) |
| integration | [![Travis Build Status](https://travis-ci.org/m-lab/etl.svg?branch=integration)](https://travis-ci.org/m-lab/etl) | [![Go Report Card](https://goreportcard.com/badge/github.com/m-lab/etl)](https://goreportcard.com/report/github.com/m-lab/etl) | [![Coverage Status](https://coveralls.io/repos/m-lab/etl/badge.svg?branch=integration)](https://coveralls.io/github/m-lab/etl?branch=integration) |

MeasurementLab data ingestion pipeline.

To create e.g., NDT table (should rarely be required!!!):
bq mk --time_partitioning_type=DAY --schema=schema/repeated.json mlab-sandbox:mlab_sandbox.ndt

Also see schema/README.md.

## Generating Schema Docs

To build a new docker image with the `generate_schema_docs` command, run:

```sh
$ docker build -t measurementlab/generate-schema-docs .
$ docker run -v $PWD:/workspace -w /workspace \
  -it measurementlab/generate-schema-docs

Writing schema_ndtresultrow.md
...

```
