
This should be run from an environment logged into gcloud in the mlab-oti project.  mlab-oti is the default project, but can be
overridden with the -project flag.

For example:
```bash
go run cmd/reproc/reproc.go -day 2017/10/01
```
OR
```bash
go run cmd/reproc/reproc.go -month 2017/10
```

The command line flags are:
```code
  -bucket string
    	Source bucket. (default "archive-mlab-oti")
  -day string
    	Single day spec, as YYYY/MM/DD
  -experiment string
    	Experiment prefix, trailing slash optional (default "ndt")
  -month string
    	Single month spec, as YYYY/MM
  -num_queues int
    	Number of queues.  Normally determined by listing queues. (default 5)
  -project string
    	Project containing queues.
  -queue string
    	Base of queue name. (default "etl-ndt-batch-")
```

The default queues feed into etl-ndt-batch pipeline, which pushes rows to measurement-lab:batch.ndt.  This table is created by
```bash
bq mk --time_partitioning_type=DAY --schema repeated.json -t measurement-lab:batch.ndt
```

The pipeline job runs in mlab-oti, so I've granted bigquery.dataeditor permissions to measurement-lab 
for mlab-oti@appspot.gserviceaccount.com, but this is more permissive that I would like.  We should use a service-account
instead.  One already exists called etl-pipeline that would
probably make most sense.
