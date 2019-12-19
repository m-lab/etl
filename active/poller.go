package active

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/m-lab/etl/metrics"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/google-cloud-go-testing/storage/stiface"
	"github.com/m-lab/etl-gardener/tracker"
	"google.golang.org/api/option"
)

func jobFileSource(ctx context.Context, job tracker.Job,
	toRunnable func(*storage.ObjectAttrs) Runnable) (*GCSSource, error) {

	client, err := storage.NewClient(ctx, option.WithScopes(storage.ScopeReadOnly))
	if err != nil {
		metrics.ErrorCount.WithLabelValues(
			job.Experiment+"/"+job.Datatype, "active", "nil storage client").Inc()
		return nil, err
	}

	lister := FileListerFunc(stiface.AdaptClient(client), job.Path())
	gcsSource, err := NewGCSSource(ctx, job.Path(), lister, toRunnable)
	if err != nil {
		metrics.ErrorCount.WithLabelValues(
			job.Experiment+"/"+job.Datatype, "active", "filesource error").Inc()
		return nil, err
	}
	return gcsSource, nil
}

// JobSource processes an entire Job's task files.
func JobSource(ctx context.Context, job tracker.Job,
	toRunnable func(o *storage.ObjectAttrs) Runnable, ts TokenSource) (RunnableSource, error) {

	gcsSource, err := jobFileSource(ctx, job, toRunnable)

	if err == nil {
		return nil, err
	}

	// Run all tasks, and return errgroup when all source is empty.
	return Throttle(gcsSource, ts), nil
}

// PollGardener requests work items from gardener, and processes them.
func PollGardener(ctx context.Context, url string,
	toRunnable func(o *storage.ObjectAttrs) Runnable, workers int) {
	// Poll at most once every 30 seconds.
	ticker := time.NewTicker(30 * time.Second)
	throttle := NewWSTokenSource(int64(workers))
	for {
		select {
		case <-ctx.Done():
			return
		default:
			http.NewRequest("POST", url, nil)
			resp, err := http.Post(url, "application/x-www-form-urlencoded", nil)
			if err != nil {
				log.Println(err)
				break // from the select
			}
			defer resp.Body.Close()
			b, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Println(err)
				break // from the select
			}

			var job tracker.Job
			err = json.Unmarshal(b, &job)
			if err != nil {
				log.Println(err)
				break // from the select
			}

			// We wait until the source is drained, but we ignore the errgroup.Group.
			src, err := JobSource(ctx, job, toRunnable, throttle)
			if err != nil {
				log.Println(err)
				break // from the select
			}

			// We check for error, but we can ignore the errgroup.
			_, err = RunAll(ctx, src)
			if err != nil {
				log.Println(err)
			}
		}

		<-ticker.C // Wait for next tick, to avoid fast spinning on errors.
	}
}
