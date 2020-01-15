package active

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"

	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/etl/metrics"
)

// JobFailures counts the all errors that result in test loss.
//
// Provides metrics:
//   etl_job_failures{prefix, year, kind}
// Example usage:
//   JobFailures.WithLabelValues("ndt/tcpinfo" "2019", "insert").Inc()
var JobFailures = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "etl_job_failures",
		Help: "Job level failures.",
	},
	// Parser type, error description.
	[]string{"prefix", "year", "type"},
)

func postAndIgnoreResponse(url *url.URL) error {
	resp, postErr := http.Post(url.String(), "", nil)
	if postErr == nil {
		resp.Body.Close()
	}
	return postErr
}

// RunAll will execute functions provided by Next() until there are no more,
// or the context is canceled.
// The tk URL is used for reporting status back to the tracker.
func RunAll(ctx context.Context, rSrc RunnableSource, job tracker.Job, tk url.URL) (*errgroup.Group, error) {
	eg := &errgroup.Group{}
	for {
		run, err := rSrc.Next(ctx)
		if err != nil {
			debug.Println(err)
			return eg, err
		}

		heartbeat := tracker.HeartbeatURL(tk, job)
		if postErr := postAndIgnoreResponse(heartbeat); postErr != nil {
			log.Println(postErr, "on heartbeat for", job.Path())
		}

		debug.Println("Starting func")

		f := func() error {
			metrics.ActiveTasks.WithLabelValues(rSrc.Label()).Inc()
			defer metrics.ActiveTasks.WithLabelValues(rSrc.Label()).Dec()

			err := run.Run()
			if err == nil {
				update := tracker.UpdateURL(tk, job, tracker.Parsing, run.Info())
				if postErr := postAndIgnoreResponse(update); postErr != nil {
					log.Println(postErr, "on update for", job.Path())
				}
			}
			return err
		}

		eg.Go(f)
	}
}

func jobFileSource(ctx context.Context, job tracker.Job,
	toRunnable func(*storage.ObjectAttrs) Runnable) (*GCSSource, error) {

	client, err := storage.NewClient(ctx, option.WithScopes(storage.ScopeReadOnly))
	if err != nil {
		JobFailures.WithLabelValues(
			job.Experiment+"/"+job.Datatype, job.Date.Format("2006"), "nil storage client").Inc()
		return nil, err
	}

	lister := FileListerFunc(stiface.AdaptClient(client), job.Path())
	gcsSource, err := NewGCSSource(ctx, job.Path(), lister, toRunnable)
	if err != nil {
		JobFailures.WithLabelValues(
			job.Experiment+"/"+job.Datatype, job.Date.Format("2006"), "filesource").Inc()
		return nil, err
	}
	return gcsSource, nil
}

func pollAndRun(ctx context.Context, base url.URL,
	toRunnable func(o *storage.ObjectAttrs) Runnable, tokens TokenSource) error {

	jobURL := base
	jobURL.Path = "job"

	resp, err := http.Post(jobURL.String(), "application/x-www-form-urlencoded", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if len(b) > 0 {
			return errors.New(string(b))
		}

		return errors.New(resp.Status)
	}
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

	update := tracker.UpdateURL(base, job, tracker.Parsing, "starting tasks")
	if postErr := postAndIgnoreResponse(update); postErr != nil {
		log.Println(postErr)
	}

	eg, err := RunAll(ctx, src, job, base)

	// Once all are dispatched, we want to wait until all have completed
	// before posting the state change.
	go func() {
		log.Println("all tasks dispatched for", job.Path())
		eg.Wait()
		log.Println("finished", job.Path())
		update := tracker.UpdateURL(base, job, tracker.ParseComplete, "")
		// TODO - should this have a retry?
		if postErr := postAndIgnoreResponse(update); postErr != nil {
			log.Println(postErr)
		}
	}()

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
			err := pollAndRun(ctx, base, toRunnable, throttle)
			if err != nil {
				log.Println(err)
			}
		}

		<-ticker.C // Wait for next tick, to avoid fast spinning on errors.
	}
}
