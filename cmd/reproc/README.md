
This should be run from an environment logged into gcloud in the mlab-oti project.  mlab-oti is the default project, but can be
overridden with the -project flag.

For example:
```
go run cmd/reproc/reproc.go -day 2017/10/01
```
OR
```
go run cmd/reproc/reproc.go -month 2017/10
```

The command line flags are:
```
	fProject   = flag.String("project", "mlab-oti", "Project containing queues.")
	fQueue     = flag.String("queue", "etl-ndt-batch-", "Base of queue name.")
	fNumQueues = flag.Int("num_queues", 5, "Number of queues.  Normally determined by listing queues.")
	fBucket    = flag.String("bucket", "archive-mlab-oti", "Source bucket.")
	fExper     = flag.String("experiment", "ndt", "Experiment prefix, trailing slash optional")
	fMonth     = flag.String("month", "", "Single month spec, as YYYY/MM")
	fDay       = flag.String("day", "", "Single day spec, as YYYY/MM/DD")
```

The default queues feed into etl-ndt-batch pipeline, which pushes rows to measurement-lab:batch.ndt.  This table is created by
```bash
bq mk --time_partitioning_type=DAY --schema repeated.json -t measurement-lab:batch.ndt
```

The pipeline job runs in mlab-oti, so I've granted bigquery.dataeditor permissions 
to mlab-oti@appspot.gserviceaccount.com, but this is more permissive that I would like.  We should use a service-account
instead.  One already exists called etl-pipeline that would
probably make most sense.
