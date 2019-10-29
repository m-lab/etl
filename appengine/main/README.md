# Deploy push-queue service


## Dependencies

Install the Google Cloud SDK, i.e. `gcloud`.

```
  $ gcloud components install app-engine-go
  $ gcloud components install beta
```

## Deploy

From the `appengine/push_queue` directory:
```
$ gcloud beta app deploy
```

At the time of writing, `gcloud app deploy` does not work correctly with go
import paths.
