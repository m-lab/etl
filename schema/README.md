# Schema code associated with populating bigquery entities.

Parsers in mlab-sandbox and mlab-staging target datasets that are in the same
project. Parsers in mlab-oti target datasets in the measurement-lab project.

All parsers except sidestream write to the `base_tables` dataset.  This is hardcoded in globals.go,
but may be overridden with the BIGQUERY_DATASET env var in config yaml files.

NOTE: We are revising the datasets and table names, so this may not be true after Feb 2019, and
this file should be updated.

## BigQuery Schemas
Schema json files have all been moved to the etl-schema repository.