Used the [quickstart](https://cloud.google.com/functions/docs/quickstart) to
build the initial version and then moved to the [storage triggers
tutorial](https://cloud.google.com/functions/docs/tutorials/storage) to refine
things.

To deploy this cloud function to mlab-sandbox, try:
```bash
gcloud beta functions deploy parserNotifier \
    --stage-bucket=parser-functions-sandbox \
    --trigger-bucket=m-lab-sandbox \
    --project=mlab-sandbox
```

To install all the dependencies:
```bash
npm install .
```

To run the unit tests:
```bash
npm test
```
