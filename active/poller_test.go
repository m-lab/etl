package active_test

import (
	"context"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/m-lab/etl-gardener/tracker"
	"github.com/m-lab/etl/active"
	"github.com/m-lab/go/logx"
	"github.com/m-lab/go/rtx"
	"google.golang.org/api/iterator"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	logx.LogxDebug.Set("true")
	logx.Debug.SetFlags(log.LstdFlags | log.Lshortfile)
}

type fakeGardener struct {
	t *testing.T // for logging

	lock       sync.Mutex
	jobs       []*tracker.JobWithTarget
	heartbeats int
	updates    int
}

func (g *fakeGardener) AddJob(job tracker.Job) {
	jt := &tracker.JobWithTarget{
		ID:  job.Key(),
		Job: job,
	}
	g.jobs = append(g.jobs, jt)
}

func (g *fakeGardener) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rtx.Must(r.ParseForm(), "bad request")
	if r.Method != http.MethodPost {
		log.Fatal("Should be POST") // Not t.Fatal because this is asynchronous.
	}
	g.lock.Lock()
	defer g.lock.Unlock()
	switch {
	case r.URL.Path == "/v2/job/next":
		if len(g.jobs) < 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		j := g.jobs[0]
		g.jobs = g.jobs[1:]
		w.Write(j.Marshal())
	case r.URL.Path == "/v2/job/heartbeat":
		g.t.Log(r.URL.Path, r.URL.Query())
		g.heartbeats++

	case r.URL.Path == "/v2/job/update":
		g.t.Log(r.URL.Path, r.URL.Query())
		g.updates++

	default:
		log.Fatal(r.URL) // Not t.Fatal because this is asynchronous.
	}
}

// This sets up a fake Gardener and fake storage client, and checks functionality.
func TestGardenerAPI_JobFileSource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up a fake storage client
	c := testClient()

	// set up a fake gardener service.
	fg := fakeGardener{t: t, jobs: make([]*tracker.JobWithTarget, 0)}
	fg.AddJob(tracker.NewJob("foobar", "ndt", "ndt5", time.Date(2019, 01, 01, 0, 0, 0, 0, time.UTC)))
	tracker := httptest.NewServer(&fg)
	defer tracker.Close()
	tkURL, err := url.Parse(tracker.URL)
	rtx.Must(err, "bad url")

	// Set up GardenerAPI using the fakes.
	g := active.NewGardenerAPI(*tkURL, c)

	jt, err := g.NextJob(ctx)
	rtx.Must(err, "next job")

	// The test counter creates runnables for the jobs.
	p := newCounter(t)
	src, err := g.JobFileSource(ctx, jt.Job, p.toRunnable)
	rtx.Must(err, "file source")
	log.Println(src)

	eg, err := runAll(ctx, src)
	if err != iterator.Done {
		t.Fatal(err)
	}
	err = eg.Wait()
	if err != nil {
		t.Error(err)
	}

	if p.success != 3 {
		t.Error("1 test should have succeeded.", p.success)
	}
	if p.fail != 0 {
		t.Error("Fail", p.fail)
	}
}

func TestGardenerAPI_RunAll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up a fake storage client
	c := testClient()

	// set up a fake gardener service.
	fg := fakeGardener{t: t, jobs: make([]*tracker.JobWithTarget, 0)}
	fg.AddJob(tracker.NewJob("foobar", "ndt", "ndt5", time.Date(2019, 01, 01, 0, 0, 0, 0, time.UTC)))
	tracker := httptest.NewServer(&fg)
	defer tracker.Close()
	tkURL, err := url.Parse(tracker.URL)
	rtx.Must(err, "bad url")

	// Set up GardenerAPI using the fakes.
	g := active.NewGardenerAPI(*tkURL, c)

	jt, err := g.NextJob(ctx)
	rtx.Must(err, "next job")

	// The test counter creates runnables for the jobs.
	p := newCounter(t)
	src, err := g.JobFileSource(ctx, jt.Job, p.toRunnable)
	rtx.Must(err, "file source")
	eg, err := g.RunAll(ctx, src, jt)
	if err != nil {
		t.Fatal(err)
	}
	err = eg.Wait()
	if err != nil {
		t.Error(err)
	}

	// Expect 3 successes, with one heartbeat and one update for each job.
	if p.success != 3 {
		t.Error("1 test should have succeeded.", p.success)
	}
	if p.fail != 0 {
		t.Error("Fail", p.fail)
	}
	if fg.heartbeats != 3 {
		t.Error(&fg)
	}
	if fg.updates != 3 {
		t.Error(&fg)
	}
}

func TestGardenerAPI_Poll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up a fake storage client
	c := testClient()

	// set up a fake gardener service.
	fg := fakeGardener{t: t, jobs: make([]*tracker.JobWithTarget, 0)}
	fg.AddJob(tracker.NewJob("foobar", "ndt", "ndt5", time.Date(2019, 01, 01, 0, 0, 0, 0, time.UTC)))
	tracker := httptest.NewServer(&fg)
	defer tracker.Close()
	tkURL, err := url.Parse(tracker.URL)
	rtx.Must(err, "bad url")

	// Set up GardenerAPI using the fakes.
	g := active.NewGardenerAPI(*tkURL, c)

	// The test counter creates runnables for the jobs.
	p := newCounter(t)
	go g.Poll(ctx, p.toRunnable, 2, 100*time.Millisecond)

	time.Sleep(1000 * time.Millisecond)
	cancel()

	// Expect 3 successes, with one heartbeat and one update for each job.
	if p.success != 3 {
		t.Error("1 test should have succeeded.", p.success)
	}
	if p.fail != 0 {
		t.Error("Fail", p.fail)
	}
	if fg.heartbeats != 3 {
		t.Error(&fg)
	}
	// Should be 5 updates - starting, 3 tasks, postProcessing
	if fg.updates != 5 {
		t.Error(&fg)
	}
}
