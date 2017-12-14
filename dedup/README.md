dedup.go is intended to be run as a cron job.  It looks for templated
tables using the __template_prefix__ parameter, check whether the
table has been updated in the past __delay__ period, and if not,
copies the table into the corresponding partition in
__destination_table__, removing dups based on __dedup_field__.

## Useful bits:

1. bq show --format=prettyjson measurement-lab.batch.ndt_* will give
 the summary for the whole set.  Using HTTP, this would be a get request:
 ```
GET https://www.googleapis.com/bigquery/v2/projects/projectId/datasets/datasetId/tables/tableId

 ```
2. 

## Other considerations:
1. Should run the tar file check.  Unfortunately, there may be tar
files that don't result in any rows.  How to deal with that?

