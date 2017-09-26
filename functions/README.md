Used the [quickstart](https://cloud.google.com/functions/docs/quickstart) to
build the initial version and then moved to the [storage triggers
tutorial](https://cloud.google.com/functions/docs/tutorials/storage) to refine
things.

Note that for different deployments there are 4 things to change...
createXXXTask..., stage-bucket, trigger-bucket, and project.

To deploy this cloud function to sandbox, use:
```bash
gcloud beta functions deploy createSandboxTaskOnFileNotification \
    --stage-bucket=functions-mlab-sandbox \
    --trigger-bucket=m-lab-sandbox \
    --project=mlab-sandbox
```

To deploy this cloud function to staging, use:
```bash
gcloud beta functions deploy createStagingTaskOnFileNotification \
    --stage-bucket=functions-mlab-staging \
    --trigger-bucket=archive-mlab-staging \
    --project=mlab-staging
```

To deploy this cloud function to production, use:
```bash
gcloud beta functions deploy createProdTaskOnFileNotification \
    --stage-bucket=functions-mlab-oti \
    --trigger-bucket=archive-mlab-oti \
    --project=mlab-oti
```

---------------------------------------------------------------

To install all the dependencies on local machine:
```bash
npm install .
```

To run the unit tests:
```bash
npm test
```

