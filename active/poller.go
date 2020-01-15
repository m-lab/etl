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
	"github.com/m-lab/go/rtx"
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

// GardenerAPI encapsulates the backend paths and clients to connect to gardener and GCS.
type GardenerAPI struct {
	tracker url.URL
	gcs     stiface.Client
}

// NewGardenerAPI creates a GardenerAPI.
func NewGardenerAPI(tracker url.URL, gcs stiface.Client) *GardenerAPI {
	return &GardenerAPI{tracker: tracker, gcs: gcs}
}

// MustStorageClient creates a default GCS client.
func MustStorageClient(ctx context.Context) stiface.Client {
	c, err := storage.NewClient(ctx, option.WithScopes(storage.ScopeReadOnly))
	rtx.Must(err, "Failed to create storage client")
	return stiface.AdaptClient(c)
}

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
func (g *GardenerAPI) RunAll(ctx context.Context, rSrc RunnableSource, job tracker.Job) (*errgroup.Group, error) {
	eg := &errgroup.Group{}
	for {
		run, err := rSrc.Next(ctx)
		if err != nil {
			debug.Println(err)
			return eg, err
		}

		heartbeat := tracker.HeartbeatURL(g.tracker, job)
		if postErr := postAndIgnoreResponse(heartbeat); postErr != nil {
			log.Println(postErr, "on heartbeat for", job.Path())
		}

		debug.Println("Starting func")

		f := func() error {
			metrics.ActiveTasks.WithLabelValues(rSrc.Label()).Inc()
			defer metrics.ActiveTasks.WithLabelValues(rSrc.Label()).Dec()

			err := run.Run()
			if err == nil {
				update := tracker.UpdateURL(g.tracker, job, tracker.Parsing, run.Info())
				if postErr := postAndIgnoreResponse(update); postErr != nil {
					log.Println(postErr, "on update for", job.Path())
				}
			}
			return err
		}

		eg.Go(f)
	}
}

// JobFileSource creates a gcsSource for the job.
func (g *GardenerAPI) JobFileSource(ctx context.Context, job tracker.Job,
	toRunnable func(*storage.ObjectAttrs) Runnable) (*GCSSource, error) {

	lister := FileListerFunc(g.gcs, job.Path())
	gcsSource, err := NewGCSSource(ctx, job.Path(), lister, toRunnable)
	if err != nil {
		JobFailures.WithLabelValues(
			job.Experiment+"/"+job.Datatype, job.Date.Format("2006"), "filesource").Inc()
		return nil, err
	}
	return gcsSource, nil
}

func (g *GardenerAPI) NextJob(ctx context.Context) (job tracker.Job, err error) {
	jobURL := g.tracker
	jobURL.Path = "job"

	resp, err := http.Post(jobURL.String(), "application/x-www-form-urlencoded", nil)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var b []byte
		b, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return
		}
		if len(b) > 0 {
			err = errors.New(string(b))
			return
		}

		err = errors.New(resp.Status)
		return
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	err = json.Unmarshal(b, &job)
	return
}

func (g *GardenerAPI) pollAndRun(ctx context.Context,
	toRunnable func(o *storage.ObjectAttrs) Runnable, tokens TokenSource) error {
	job, err := g.NextJob(ctx)
	if err != nil {
		return err
	}

	gcsSource, err := g.JobFileSource(ctx, job, toRunnable)
	if err != nil {
		return err
	}
	src := Throttle(gcsSource, tokens)

	log.Println("Running", job.Path())

	update := tracker.UpdateURL(g.tracker, job, tracker.Parsing, "starting tasks")
	if postErr := postAndIgnoreResponse(update); postErr != nil {
		log.Println(postErr)
	}

	eg, err := g.RunAll(ctx, src, job)

	// Once all are dispatched, we want to wait until all have completed
	// before posting the state change.
	go func() {
		log.Println("all tasks dispatched for", job.Path())
		eg.Wait()
		log.Println("finished", job.Path())
		update := tracker.UpdateURL(g.tracker, job, tracker.ParseComplete, "")
		// TODO - should this have a retry?
		if postErr := postAndIgnoreResponse(update); postErr != nil {
			log.Println(postErr)
		}
	}()

	return err
}

// Poll requests work items from gardener, and processes them.
func (g *GardenerAPI) Poll(ctx context.Context,
	toRunnable func(o *storage.ObjectAttrs) Runnable, maxWorkers int, period time.Duration) {
	// Poll at most once every 10 seconds.
	ticker := time.NewTicker(period)
	throttle := NewWSTokenSource(maxWorkers)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := g.pollAndRun(ctx, toRunnable, throttle)
			if err != nil {
				log.Println(err)
			}
		}

		<-ticker.C // Wait for next tick, to avoid fast spinning on errors.
	}
}
