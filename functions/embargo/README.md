To deploy this cloud function to sandbox, use:
```bash
gcloud beta functions deploy transferOnFileNotification \
    --stage-bucket=functions-mlab-sandbox \
    --trigger-bucket=m-lab-sandbox \
    --project=mlab-sandbox
