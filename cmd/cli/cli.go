package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/m-lab/annotation-service/api"
	v2 "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/rtx"

	"github.com/m-lab/etl/fake"
	"github.com/m-lab/etl/storage"
	"github.com/m-lab/etl/worker"
)

var usage = `
SUMMARY

USAGE
  $ 

`

type fakeAnnotator struct{}

func (ann *fakeAnnotator) GetAnnotations(ctx context.Context, date time.Time, ips []string, info ...string) (*v2.Response, error) {
	return &v2.Response{AnnotatorDate: time.Now(), Annotations: make(map[string]*api.Annotations, 0)}, nil
}

var (
	filename = flag.String("filename", "", "gs:// file path")
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s\n", os.Args[0])
		fmt.Fprintf(os.Stderr, usage)
		fmt.Fprintln(os.Stderr, "Flags:")
		flag.PrintDefaults()
	}
}

var mainCtx, mainCancel = context.WithCancel(context.Background())

func main() {
	defer mainCancel()

	flag.Parse()
	flagx.ArgsFromEnv(flag.CommandLine)

	gcsClient, err := storage.GetStorageClient(false)
	rtx.Must(err, "GetStorageClient")

	up := fake.NewFakeUploader()
	status, err := worker.ProcessGKETaskWithClient(*filename, gcsClient, up, &fakeAnnotator{})
	rtx.Must(err, "ProcessGKETask")
	if status != http.StatusOK {
		log.Fatal("Expected", http.StatusOK, "Got:", status)
	}
}
