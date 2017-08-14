To deploy this cloud function to sandbox, use:
 ```bash
 gcloud beta functions deploy embargoOnFileNotificationSandbox \
     --stage-bucket=functions-mlab-sandbox \
     --trigger-bucket=scraper-mlab-sandbox \
     --project=mlab-sandbox
