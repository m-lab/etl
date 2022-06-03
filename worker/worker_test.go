package worker_test

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

<<<<<<< HEAD
	"github.com/m-lab/annotation-service/api"
	v2 "github.com/m-lab/annotation-service/api/v2"
=======
	"github.com/m-lab/go/cloud/bqx"
>>>>>>> 01587ff (Remove Annotator references)
	"github.com/m-lab/go/rtx"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/factory"
	"github.com/m-lab/etl/metrics"
	etlstorage "github.com/m-lab/etl/storage"
	"github.com/m-lab/etl/worker"

	"github.com/fsouza/fake-gcs-server/fakestorage"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func counterValue(m prometheus.Metric) float64 {
	var mm dto.Metric
	m.Write(&mm)
	ctr := mm.GetCounter()
	if ctr == nil {
		log.Println(mm.GetUntyped())
		return math.Inf(-1)
	}

	return *ctr.Value
}

func checkCounter(t *testing.T, c chan prometheus.Metric, expected float64) {
	m := <-c
	v := counterValue(m)
	if v != expected {
		log.Output(2, fmt.Sprintln("For", m.Desc(), "expected:", expected, "got:", v))
		t.Error("For", m.Desc(), "expected:", expected, "got:", v)
	}
}

// Adds a path from testdata to bucket.
func add(svr *fakestorage.Server, bucket string, fn string, rdr io.Reader) {
	data, err := ioutil.ReadAll(rdr)
	rtx.Must(err, "Error reading data for", fn)
	svr.CreateObject(
		fakestorage.Object{
			BucketName: bucket,
			Name:       fn,
			Content:    data})
}

func loadFromTar(svr *fakestorage.Server, bucket string, tf *tar.Reader) *fakestorage.Server {
	for h, err := tf.Next(); err != io.EOF; h, err = tf.Next() {
		if h.Typeflag == tar.TypeReg {
			add(svr, bucket, h.Name, tf)
		}
	}
	return svr
}

func fromTar(bucket string, fn string) *fakestorage.Server {
	server := fakestorage.NewServer([]fakestorage.Object{})
	f, err := os.Open(fn)
	rtx.Must(err, "opening tar file")
	defer f.Close()
	tf := tar.NewReader(f)
	return loadFromTar(server, bucket, tf)
}

// This is also the annotator, so it just returns itself.
<<<<<<< HEAD
type fakeAnnotatorFactory struct{}

func (ann *fakeAnnotatorFactory) GetAnnotations(ctx context.Context, date time.Time, ips []string, info ...string) (*v2.Response, error) {
	return &v2.Response{AnnotatorDate: time.Now(), Annotations: make(map[string]*api.Annotations, 0)}, nil
}

func (ann *fakeAnnotatorFactory) Get(ctx context.Context, dp etl.DataPath) (v2.Annotator, etl.ProcessingError) {
	return ann, nil
=======
type fakeSinkFactory struct {
	up etl.Uploader
}

func (fsf *fakeSinkFactory) Get(ctx context.Context, dp etl.DataPath) (row.Sink, etl.ProcessingError) {
	if fsf.up == nil {
		return nil, factory.NewError(dp.DataType, "fakeSinkFactory",
			http.StatusInternalServerError, errors.New("nil uploader"))
	}
	pdt := bqx.PDT{Project: "fake-project", Dataset: "fake-dataset", Table: "fake-table"}
	in, err := bq.NewColumnPartitionedInserterWithUploader(pdt, fsf.up)
	rtx.Must(err, "Bad SinkFactory")
	return in, nil
>>>>>>> 01587ff (Remove Annotator references)
}

type fakeSourceFactory struct {
	client stiface.Client
}

func (sf *fakeSourceFactory) Get(ctx context.Context, dp etl.DataPath) (etl.TestSource, etl.ProcessingError) {
	label := dp.TableBase()
	tr, err := etlstorage.NewTestSource(sf.client, dp, label)
	if err != nil {
		panic("error opening gcs file:" + err.Error())
	}
	return tr, nil
}

func NewSourceFactory(bucket string) factory.SourceFactory {
	gcsClient := fromTar(bucket, "../testfiles/ndt.tar").Client()
	return &fakeSourceFactory{client: stiface.AdaptClient(gcsClient)}
}

func NewSinkFactory(bucket string) (*fakestorage.Server, factory.SinkFactory) {
	fs := fakestorage.NewServer([]fakestorage.Object{})
	fs.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucket})
	return fs, etlstorage.NewSinkFactory(stiface.AdaptClient(fs.Client()), bucket)
}

func TestProcessGKETask(t *testing.T) {
	if testing.Short() {
		t.Log("Skipping integration test")
	}

	// Create sink factory.
	fs, sf := NewSinkFactory("test-bucket")

	fakeFactory := worker.StandardTaskFactory{
<<<<<<< HEAD
		Sink:   sf,
		Source: NewSourceFactory("test-bucket"),
=======
		Sink:   &fakeSinkFactory{up: up},
		Source: NewSourceFactory(),
>>>>>>> 01587ff (Remove Annotator references)
	}

	filename := "gs://test-bucket/ndt/ndt5/2019/12/01/20191201T020011.395772Z-ndt5-mlab1-bcn01-ndt.tgz"
	path, err := etl.ValidateTestPath(filename)
	if err != nil {
		t.Fatal(err, filename)
	}
	err = worker.ProcessGKETask(context.Background(), path, &fakeFactory)
	if err != nil {
		t.Fatal("Expected", http.StatusOK, "Got:", err)
	}

	// This section checks that prom metrics are updated appropriately.
	c := make(chan prometheus.Metric, 10)

	metrics.FileCount.Collect(c)
	checkCounter(t, c, 488)

	metrics.TaskTotal.Collect(c)
	checkCounter(t, c, 1)

	metrics.TestTotal.Collect(c)
	checkCounter(t, c, 478)

	// Lookup output from task.
	o, err := fs.GetObject("test-bucket", "ndt/ndt5/2019/12/01/20191201T020011.395772Z-ndt5-mlab1-bcn01-ndt.tgz.json")
	if err != nil {
		t.Errorf("GetObject() expected nil error, got %v", err)
	}
	// Read the file contents to determine the number of rows written.
	lines := strings.Split(string(o.Content), "\n")
	if len(lines)-1 != 512 { // -1 to strip final newline.
		t.Error("Expected 512 tests, got", len(lines)-1)
	}
	metrics.FileCount.Reset()
	metrics.TaskTotal.Reset()
	metrics.TestTotal.Reset()
}
