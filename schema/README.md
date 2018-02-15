Schema includes bigquery schema (json) files, and code associated with
populating bigquery entities.

Parsers in mlab-sandbox and mlab-staging target datasets that are in the same
project. Parsers in mlab-oti target datasets in the measurement-lab project.

Today, all parsers write to the `base_tables` dataset. Soon, they will be
changed to write to the `incoming` dataset, and deduplicated data will be
written to `base_tables`.  See https://github.com/m-lab/etl/issues/387 for
updates.

## NDT

legacy.json contains the schema downloaded from the existing ndt.all tables.
It has been ordered for easy comparison against the new schema.

ndt.json contains the initial schema for the NDT tables. It can be used to
create a new table in mlab-sandbox project by invoking:

    bq --project_id mlab-sandbox mk --time_partitioning_type=DAY \
        --schema schema/ndt.json -t base_tables.ndt

ndt_delta.json contains another NDT schema, including a repeated "delta" field,
intended to contain snapshot deltas.  To create a new table:

    bq --project_id mlab-sandbox mk --time_partitioning_type=DAY \
        --schema schema/ndt_delta.json -t base_tables.ndt_delta

As of May 2017, there are (still) differences between the legacy and NDT schema
that may need to be addressed.

## Paris-traceroute

pt.json contains the schema for paris traceroute tables.  To create a new table:

    bq --project_id mlab-sandbox mk --time_partitioning_type=DAY \
        --schema schema/pt.json -t base_tables.traceroute

## Sidestream

ss.json contains the schema for sidestream tables.  To create a new table:

    bq --project_id mlab-sandbox mk --time_partitioning_type=DAY \
        --schema schema/ss.json -t base_tables.sidestream

## Switch - DISCO

switch.json contains the schema for DISCO tables. To create a new table:

    bq --project_id mlab-sandbox mk --time_partitioning_type=DAY \
        --schema schema/switch.json -t base_tables.switch
