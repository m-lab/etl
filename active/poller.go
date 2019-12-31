package active

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/m-lab/etl/metrics"
	"golang.org/x/sync/errgroup"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/google-cloud-go-testing/storage/stiface"
	"github.com/m-lab/etl-gardener/tracker"
	"google.golang.org/api/option"
)

func postNoResponse(url *url.URL) error {
	resp, postErr := http.Post(url.RequestURI(), "", nil)
	if postErr != nil {
		log.Println(postErr)
	} else {
		resp.Body.Close()
	}
	return postErr
}

// RunAll will execute functions provided by Next() until there are no more,
// or the context is canceled.
// The tk URL is used for reported status back to the tracker.
func RunAll(ctx context.Context, rSrc RunnableSource, job tracker.Job, tk url.URL) (*errgroup.Group, error) {
	eg := &errgroup.Group{}
	for {
		run, err := rSrc.Next(ctx)
		if err != nil {
			debug.Println(err)
			return eg, err
		}
		debug.Println("Starting func")

		f := func() error {
			metrics.ActiveTasks.WithLabelValues(rSrc.Label()).Inc()
			defer metrics.ActiveTasks.WithLabelValues(rSrc.Label()).Dec()
			update := tracker.UpdateURL(tk, job, tracker.Parsing, run.Info())
			if postErr := postNoResponse(update); postErr != nil {
				return postErr
			}

			// TestCount and other metrics should be handled within Run().
			err = run.Run()
			if err != nil {
				errURL := tracker.ErrorURL(tk, job, err.Error())
				postNoResponse(errURL)
				return err
			}
			update = tracker.UpdateURL(tk, job, tracker.ParseComplete, run.Info())
			if postErr := postNoResponse(update); postErr != nil {
				return postErr
			}
			return nil
		}

		eg.Go(f)
	}
}

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

func pollAndRun(ctx context.Context, base url.URL,
	toRunnable func(o *storage.ObjectAttrs) Runnable, tokens TokenSource) error {

	jobURL := base
	jobURL.Path = "job"
	log.Println("job query:", jobURL.RequestURI())

	resp, err := http.Post(jobURL.RequestURI(), "application/x-www-form-urlencoded", nil)
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
	_, err = RunAll(ctx, src, job, base)
	return err
}

// PollGardener requests work items from gardener, and processes them.
func PollGardener(ctx context.Context, base url.URL,
	toRunnable func(o *storage.ObjectAttrs) Runnable, maxWorkers int) {
	// Poll at most once every 10 seconds.
	ticker := time.NewTicker(10 * time.Second)
	throttle := NewWSTokenSource(maxWorkers)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			pollAndRun(ctx, base, toRunnable, throttle)
		}

		<-ticker.C // Wait for next tick, to avoid fast spinning on errors.
	}
}
