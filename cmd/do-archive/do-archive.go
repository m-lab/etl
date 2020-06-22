package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	gcs "cloud.google.com/go/storage"

	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/m-lab/annotation-service/api"
	v2 "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/rtx"

	"github.com/m-lab/etl/active"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/storage"
	"github.com/m-lab/etl/task"
	"github.com/m-lab/etl/worker"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// This is also the annotator, so it just returns itself.
type nullAnnotatorFactory struct{}

func (ann *nullAnnotatorFactory) GetAnnotations(ctx context.Context, date time.Time, ips []string, info ...string) (*v2.Response, error) {
	return &v2.Response{AnnotatorDate: time.Now(), Annotations: make(map[string]*api.Annotations, 0)}, nil
}

func (ann *nullAnnotatorFactory) Get(ctx context.Context, dp etl.DataPath) (v2.Annotator, etl.ProcessingError) {
	return ann, nil
}

//--------------------------------------------------------------------
// Code adapted from cmd/etl_worker.go
type runnable struct {
	tf task.Factory
	gcs.ObjectAttrs
}

func (r *runnable) Run(ctx context.Context) error {
	path := fmt.Sprintf("gs://%s/%s", r.Bucket, r.Name)
	dp, err := etl.ValidateTestPath(path)
	if err != nil {
		log.Printf("Invalid filename: %v\n", err)
		return err
	}

	start := time.Now()
	log.Println("Processing", path)

	statusCode := http.StatusOK
	pErr := worker.ProcessGKETask(ctx, dp, r.tf)
	if pErr != nil {
		statusCode = pErr.Code()
	}
	metrics.DurationHistogram.WithLabelValues(
		dp.DataType, http.StatusText(statusCode)).Observe(
		time.Since(start).Seconds())
	return err
}

func (r *runnable) Info() string {
	// Should truncate this to exclude the date, maybe include the year?
	return r.Name
}

func toRunnable(obj *gcs.ObjectAttrs) active.Runnable {
	c, err := storage.GetStorageClient(false)
	if err != nil {
		return nil // TODO add an error?
	}
	taskFactory := worker.StandardTaskFactory{
		Annotator: &nullAnnotatorFactory{},
		Sink:      storage.NewSinkFactory(stiface.AdaptClient(c), *output),
		Source:    storage.GCSSourceFactory(c),
	}
	return &runnable{&taskFactory, *obj}
}

//--------------------------------------------------------------------

var (
	path   = flag.String("path", "ndt/ndt5/2020/01/01/foobar.tgz", "Full gcs archive path")
	bucket = flag.String("bucket", "gs://archive-mlab-sandbox", "gcs source bucket")
	output = flag.String("output", "gs://json-mlab-testing", "gcs output bucket")
)

// The rows can then be loaded into a BQ table, using the schema in testdata, like:
// bq load --source_format=NEWLINE_DELIMITED_JSON \
//    mlab-sandbox:gfr.small_tcpinfo gs://archive-mlab-testing/gfr/tcpinfo.json ./schema.json

func main() {
	ctx := context.Background()

	c, err := storage.GetStorageClient(true)

	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not get args from env")

	obj, err := c.Bucket(*bucket).Object(*path).Attrs(ctx)
	rtx.Must(err, "Object Attrs")

	runnable := toRunnable(obj)
	err = runnable.Run(ctx)
	rtx.Must(err, "Run failure")

}
