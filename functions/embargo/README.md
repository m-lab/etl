To deploy this cloud function to sandbox, use

```bash
gcloud beta functions deploy embargoOnFileNotificationSandbox \
     --stage-bucket=functions-mlab-sandbox \
     --trigger-resource=scraper-mlab-sandbox \
     --project=mlab-sandbox
```

To deploy this cloud function to staging, use

```bash
gcloud beta functions deploy embargoOnFileNotificationStaging \
    --stage-bucket=functions-mlab-staging \
    --trigger-resource=scraper-mlab-staging \
    --project=mlab-staging
```

To deploy this cloud function to prod, use

```bash
gcloud beta functions deploy embargoOnFileNotificationOti \
    --stage-bucket=functions-mlab-oti \
    --trigger-resource=scraper-mlab-oti \
    --project=mlab-oti
```
