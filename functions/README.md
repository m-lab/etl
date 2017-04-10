Used the [quickstart](https://cloud.google.com/functions/docs/quickstart) to
build the initial version and then moved to the [storage triggers
tutorial](https://cloud.google.com/functions/docs/tutorials/storage) to refine
things.

To deploy a cloud function, try:

```bash
gcloud beta functions deploy helloGCS --stage-bucket [YOUR_STAGING_BUCKET_NAME]
--trigger-bucket [YOUR_UPLOAD_BUCKET_NAME]
```

where `helloGCS` needs to match the name of the function exported in `index.js`.
