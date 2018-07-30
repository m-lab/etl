Schema code associated with populating bigquery entities.

Parsers in mlab-sandbox and mlab-staging target datasets that are in the same
project. Parsers in mlab-oti target datasets in the measurement-lab project.

All parsers except sidestream write to the `base_tables` dataset.
See https://github.com/m-lab/etl/issues/387 for updates.

## BigQuery Schemas
Schema json files have all been moved to the etl-schema repository.