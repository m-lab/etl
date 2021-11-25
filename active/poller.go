package active

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	job "github.com/m-lab/etl-gardener/client"
	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/go/cloud/gcs"
	"github.com/m-lab/go/rtx"

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

// GardenerAPI encapsulates the backend paths and clients to connect to gardener and GCS.
type GardenerAPI struct {
	trackerBase url.URL
	gcs         stiface.Client
}

// NewGardenerAPI creates a GardenerAPI.
func NewGardenerAPI(trackerBase url.URL, gcs stiface.Client) *GardenerAPI {
	return &GardenerAPI{trackerBase: trackerBase, gcs: gcs}
}

// MustStorageClient creates a default GCS client.
func MustStorageClient(ctx context.Context) stiface.Client {
	c, err := storage.NewClient(ctx, option.WithScopes(storage.ScopeReadOnly))
	rtx.Must(err, "Failed to create storage client")
	return stiface.AdaptClient(c)
}

// TODO migrate this to m-lab/go
func post(ctx context.Context, url url.URL) ([]byte, int, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	req, reqErr := http.NewRequestWithContext(ctx, "POST", url.String(), nil)
	if reqErr != nil {
		return nil, 0, reqErr
	}
	resp, postErr := http.DefaultClient.Do(req)
	if postErr != nil {
		return nil, 0, postErr // Documentation says we can ignore body.
	}

	// Gauranteed to have a non-nil response and body.
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body) // Documentation recommends reading body.
	return b, resp.StatusCode, err
}

// TODO add retry in case gardener is offline (during redeployment)
// TODO add metrics to track latency, retries, and errors.
func postAndIgnoreResponse(ctx context.Context, url url.URL) error {
	_, status, err := post(ctx, url)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return errors.New(http.StatusText(status))
	}
	return nil
}

// RunAll will execute functions provided by Next() until there are no more,
// or the context is canceled.
func (g *GardenerAPI) RunAll(ctx context.Context, rSrc RunnableSource, job tracker.Job) (*errgroup.Group, error) {
	eg := &errgroup.Group{}
	count := 0
	for {
		run, err := rSrc.Next(ctx)
		if err != nil {
			if err == iterator.Done {
				debug.Printf("Dispatched total of %d archives for %s\n", count, job.String())
				return eg, nil
			} else {
				metrics.BackendFailureCount.WithLabelValues(
					job.Datatype, "rSrc.Next").Inc()
				log.Println(err, "processing", job.String())
				return eg, err
			}
		}

		heartbeat := tracker.HeartbeatURL(g.trackerBase, job)
		if postErr := postAndIgnoreResponse(ctx, *heartbeat); postErr != nil {
			log.Println(postErr, "on heartbeat for", job.Path())
		}

		debug.Println("Starting func")

		f := func() (err error) {
			metrics.ActiveTasks.WithLabelValues(rSrc.Datatype()).Inc()
			defer metrics.ActiveTasks.WithLabelValues(rSrc.Datatype()).Dec()

			// Capture any panic and convert it to an error.
			defer func(tag string) {
				if err2 := metrics.PanicToErr(err, recover(), "Runall.f: "+tag); err2 != nil {
					err = err2
				}
			}(run.Info())

			err = run.Run(ctx)
			if err == nil {
				update := tracker.UpdateURL(g.trackerBase, job, tracker.Parsing, run.Info())
				if postErr := postAndIgnoreResponse(ctx, *update); postErr != nil {
					log.Println(postErr, "on update for", job.Path())
				}
			}
			return
		}

		count++
		eg.Go(f)
	}
}

func failMetric(j tracker.Job, label string) {
	JobFailures.WithLabelValues(
		j.Experiment+"/"+j.Datatype, j.Date.Format("2006"), label).Inc()
}

// JobFileSource creates a gcsSource for the job.
func (g *GardenerAPI) JobFileSource(ctx context.Context, job tracker.Job,
	toRunnable func(*storage.ObjectAttrs) Runnable) (*GCSSource, error) {

	filter, err := regexp.Compile(job.Filter)
	if err != nil {
		failMetric(job, "filter compile")
		return nil, err
	}
	bh, err := gcs.GetBucket(ctx, g.gcs, job.Bucket)
	if err != nil {
		failMetric(job, "filesource")
		return nil, err
	}
	prefix, err := job.Prefix()
	if err != nil {
		failMetric(job, "prefix")
		return nil, err
	}
	lister := FileListerFunc(bh, prefix, filter)
	gcsSource, err := NewGCSSource(ctx, job, lister, toRunnable)
	if err != nil {
		failMetric(job, "GCSSource")
		return nil, err
	}
	return gcsSource, nil
}

// NextJob requests a new job from Gardener service.
func (g *GardenerAPI) NextJob(ctx context.Context) (tracker.JobWithTarget, error) {
	return job.NextJob(ctx, g.trackerBase)
}

func (g *GardenerAPI) pollAndRun(ctx context.Context,
	toRunnable func(o *storage.ObjectAttrs) Runnable, tokens TokenSource) error {
	job, err := g.NextJob(ctx)
	if err != nil {
		log.Println(err, "on Gardener client.NextJob()")
		return err
	}

	log.Println(job, "filter:", job.Filter)
	gcsSource, err := g.JobFileSource(ctx, job.Job, toRunnable)
	if err != nil {
		log.Println(err, "on JobFileSource")
		return err
	}
	src := Throttle(gcsSource, tokens)

	log.Println("Running", job.Path())

	update := tracker.UpdateURL(g.trackerBase, job.Job, tracker.Parsing, "starting tasks")
	if postErr := postAndIgnoreResponse(ctx, *update); postErr != nil {
		log.Println(postErr)
	}

	eg, err := g.RunAll(ctx, src, job.Job)
	if err != nil {
		log.Println(err)
	}

	// Once all are dispatched, we want to wait until all have completed
	// before posting the state change.
	go func() {
		log.Println("all tasks dispatched for", job.Path())
		err := eg.Wait()
		if err != nil {
			log.Println(err, "on wait for", job.Path())
		} else {
			log.Println("finished", job.Path())
		}
		update := tracker.UpdateURL(g.trackerBase, job.Job, tracker.ParseComplete, "")
		// TODO - should this have a retry?
		if postErr := postAndIgnoreResponse(ctx, *update); postErr != nil {
			log.Println(postErr)
		}
	}()

	return err
}

// Poll requests work items from gardener, and processes them.
func (g *GardenerAPI) Poll(ctx context.Context,
	toRunnable func(o *storage.ObjectAttrs) Runnable, maxWorkers int, period time.Duration) {
	// Poll no faster than period.
	ticker := time.NewTicker(period)
	throttle := NewWSTokenSource(maxWorkers)
	for {
		select {
		case <-ctx.Done():
			log.Println("Poller context done")
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

// Status adds a small amount of status info to w.
func (g *GardenerAPI) Status(w http.ResponseWriter) {
	fmt.Fprintf(w, "Gardener API: %s\n", g.trackerBase.String())
}
