Schema includes bigquery schema (json) files, and code associated with
populating bigquery entities.

legacy.json contains the schema downloaded from the existing ndt.all tables.
It has been ordered for easy comparison against the new schema.

ndt.json contains the initial schema for the NDT tables.  It can be used to
create a new table by invoking (while logged in to the appropriate project):
    bq mk --time_partitioning_type=DAY --schema schema/ndt.json -t mlab_sandbox.ndt_test_daily

repeated.json contains another NDT schema, including a repeated "delta" field,
intended to contain snapshot deltas.  To create a new table:
    bq mk --time_partitioning_type=DAY --schema schema/repeated.json -t measurement-lab:public.ndt_delta

pt.json contains the schema for paris traceroute tables.  To create a new table:
    bq mk --time_partitioning_type=DAY --schema schema/pt.json -t mlab_sandbox.pt_test

As of May 2017, there are (still) differences between the legacy and NDT schema that may
need to be addressed.
