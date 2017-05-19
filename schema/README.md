Schema includes bigquery schema (json) files, and code associated with
populating bigquery entities.

ndt.json contains the initial schema for the NDT tables.  It can be used to
create a new table by invoking (while logged in to the appropriate project):

    bq mk --time_partitioning_type=DAY --schema ndt.json -t mlab_sandbox.ndt_test_daily


