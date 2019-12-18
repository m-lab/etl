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

// Process processes an entire Job's task files.
func Process(ctx context.Context, job tracker.Job,
	toRunnable func(o *storage.ObjectAttrs) Runnable, ts TokenSource) error {

	client, err := storage.NewClient(context.Background(), option.WithScopes(storage.ScopeReadOnly))
	if err != nil {
		metrics.ErrorCount.WithLabelValues(
			job.Experiment+"/"+job.Datatype, "active", "nil storage client").Inc()
		return err
	}

	lister := FileListerFunc(stiface.AdaptClient(client), job.Path())
	gcsSource, err := NewGCSSource(ctx, job.Path(), lister, toRunnable)
	if err != nil {
		metrics.ErrorCount.WithLabelValues(
			job.Experiment+"/"+job.Datatype, "active", "filesource error").Inc()
		return err
	}

	// Run all tasks, and log error on completion.
	err = RunAll(ctx, Throttle(gcsSource, ts))
	if err != nil {
		// RunAll handles metrics for individual errors.
		log.Println(job.Path(), "Had errors:", err)
	}
	return err
}

// PollGardener requests work items from gardener, and processes them.
func PollGardener(ctx context.Context, url string,
	toRunnable func(o *storage.ObjectAttrs) Runnable, workers int) {
	// Poll at most once every 30 seconds.
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			http.NewRequest("POST", url, nil)
			resp, err := http.Post(url, "application/x-www-form-urlencoded", nil)
			if err != nil {
				log.Println(err)
				break
			}
			defer resp.Body.Close()
			b, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Println(err)
				break
			}

			var job tracker.Job
			err = json.Unmarshal(b, &job)
			if err != nil {
				log.Println(err)
				break
			}

			throttle := NewWSTokenSource(int64(workers))
			Process(ctx, job, toRunnable, throttle)
		}

		<-ticker.C // Wait for next tick, to avoid fast spinning on errors.
	}
}
