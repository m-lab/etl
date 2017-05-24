Schema includes bigquery schema (json) files, and code associated with
populating bigquery entities.

legacy.json contains the schema downloaded from the existing ndt.all tables.
It has been ordered for easy comparison against the new schema.

ndt.json contains the initial schema for the NDT tables.  It can be used to
create a new table by invoking (while logged in to the appropriate project):

    bq mk --time_partitioning_type=DAY --schema ndt.json -t mlab_sandbox.ndt_test_daily

As of May 2017, there are differences between the legacy and NDT schema that may
need to be addressed.
