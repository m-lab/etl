dedup.go provides functions for use in a dedup cron job.  It looks for
templated tables using the __template_prefix__ parameter, check whether the
table has been updated in the past __delay__ period, and if not, does
further checks regarding number of tasks and rows.

TODO - edit for redundancy.

If the various checks are ok, copies the table into the corresponding
partition in __destination_table__, removing dups based on
__dedup_field__.

First, the source table is checked for new template tables or
partitions that have been stable for long enough that it is
deemed safe to migrate them to the destination table.

Tables should be processed in order of time since
LastModificationTime.  This means that we should start by
finding the age of all eligible tables.

For each day or partition that is "ready", we then verify that
the new content has at least 95% as many rows as the partition
it will replace.  This limits the regression in cases where
there is some problem with the new data.  This SHOULD also
generate an alert.

Once these prereqs are satisfied, we then execute a query that
dedups the rows from the source, and writes to the destination
partition.

## Useful bits:

1. bq show --format=prettyjson measurement-lab.batch.ndt_* will give
 the summary for the whole set.  Using HTTP, this would be a get request:
 ```
GET https://www.googleapis.com/bigquery/v2/projects/projectId/datasets/datasetId/tables/tableId

 ```
2.

## Other considerations:
1. Should run the task count check.  Unfortunately, there may be task
files that don't result in any rows.  How to deal with that?

