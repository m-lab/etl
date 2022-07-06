package active

import (
	"context"
	"fmt"
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

	gardener "github.com/m-lab/etl-gardener/client/v2"
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
	jobs        *gardener.JobClient
}

// NewGardenerAPI creates a GardenerAPI.
func NewGardenerAPI(trackerBase url.URL, gcs stiface.Client) *GardenerAPI {
	c := gardener.NewJobClient(trackerBase)
	return &GardenerAPI{trackerBase: trackerBase, gcs: gcs, jobs: c}
}

// MustStorageClient creates a default GCS client.
func MustStorageClient(ctx context.Context) stiface.Client {
	c, err := storage.NewClient(ctx, option.WithScopes(storage.ScopeReadOnly))
	rtx.Must(err, "Failed to create storage client")
	return stiface.AdaptClient(c)
}

// RunAll will execute functions provided by Next() until there are no more,
// or the context is canceled.
func (g *GardenerAPI) RunAll(ctx context.Context, rSrc RunnableSource, jt *tracker.JobWithTarget) (*errgroup.Group, error) {
	eg := &errgroup.Group{}
	count := 0
	job := jt.Job
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

		if err := g.jobs.Heartbeat(ctx, jt.ID); err != nil {
			log.Println(err, "on heartbeat for", job.Path())
		}

		debug.Println("Starting func")

		f := func() (err error) {
			metrics.ActiveTasks.WithLabelValues(rSrc.Label()).Inc()
			defer metrics.ActiveTasks.WithLabelValues(rSrc.Label()).Dec()

			// Capture any panic and convert it to an error.
			defer func(tag string) {
				if err2 := metrics.PanicToErr(err, recover(), "Runall.f: "+tag); err2 != nil {
					err = err2
				}
			}(run.Info())

			err = run.Run(ctx)
			if err == nil {
				if err := g.jobs.Update(ctx, jt.ID, tracker.Parsing, run.Info()); err != nil {
					log.Println(err, "on update for", job.Path())
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
func (g *GardenerAPI) NextJob(ctx context.Context) (*tracker.JobWithTarget, error) {
	return g.jobs.Next(ctx)
}

func (g *GardenerAPI) pollAndRun(ctx context.Context,
	toRunnable func(o *storage.ObjectAttrs) Runnable, tokens TokenSource) error {
	jt, err := g.jobs.Next(ctx)
	if err != nil {
		log.Println(err, "on Gardener client.NextJob()")
		return err
	}

	log.Println(jt, "filter:", jt.Job.Filter)
	gcsSource, err := g.JobFileSource(ctx, jt.Job, toRunnable)
	if err != nil {
		log.Println(err, "on JobFileSource")
		return err
	}
	src := Throttle(gcsSource, tokens)

	log.Println("Running", jt.Job.Path())
	if err := g.jobs.Update(ctx, jt.ID, tracker.Parsing, "starting tasks"); err != nil {
		log.Println(err)
	}

	eg, err := g.RunAll(ctx, src, jt)
	if err != nil {
		log.Println(err)
	}

	// Once all are dispatched, we want to wait until all have completed
	// before posting the state change.
	go func() {
		log.Println("all tasks dispatched for", jt.Job.Path())
		err := eg.Wait()
		if err != nil {
			log.Println(err, "on wait for", jt.Job.Path())
		} else {
			log.Println("finished", jt.Job.Path())
		}
		if err := g.jobs.Update(ctx, jt.ID, tracker.ParseComplete, ""); err != nil {
			log.Println(err)
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
