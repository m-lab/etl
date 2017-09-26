To deploy this cloud function to sandbox, use

$gcloud beta functions deploy embargoOnFileNotificationSandbox \
     --stage-bucket=functions-mlab-sandbox \
     --trigger-bucket=scraper-mlab-sandbox \
     --project=mlab-sandbox
     
To deploy this cloud function to staging, use

$gcloud beta functions deploy embargoOnFileNotificationStaging \
    --stage-bucket=functions-mlab-staging \
    --trigger-bucket=scraper-mlab-staging \
    --project=mlab-staging
    
To deploy this cloud function to prod, use

$gcloud beta functions deploy embargoOnFileNotificationOti \
    --stage-bucket=functions-mlab-oti \
    --trigger-bucket=scraper-mlab-oti \
    --project=mlab-oti
