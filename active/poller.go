package active

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"google.golang.org/api/option"

	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/etl/metrics"
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

func pollAndRun(ctx context.Context, url string,
	toRunnable func(o *storage.ObjectAttrs) Runnable, tokens TokenSource) error {
	resp, err := http.Post(url, "application/x-www-form-urlencoded", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var job tracker.Job
	err = json.Unmarshal(b, &job)
	if err != nil {
		return err
	}

	gcsSource, err := jobFileSource(ctx, job, toRunnable)
	if err != nil {
		return err
	}
	src := Throttle(gcsSource, tokens)

	log.Println("Running", job.Path())

	// We wait until the source is drained, but we ignore the errgroup.Group.
	_, err = RunAll(ctx, src)
	return err
}

// PollGardener requests work items from gardener, and processes them.
func PollGardener(ctx context.Context, url string,
	toRunnable func(o *storage.ObjectAttrs) Runnable, maxWorkers int) {
	// Poll at most once every 10 seconds.
	ticker := time.NewTicker(10 * time.Second)
	throttle := NewWSTokenSource(maxWorkers)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			pollAndRun(ctx, url, toRunnable, throttle)
		}

		<-ticker.C // Wait for next tick, to avoid fast spinning on errors.
	}
}
