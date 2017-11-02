// Package main defines a command line tool for submitting date
// ranges for reprocessing
package main

/*
Strategies...
  1. Work from a prefix, or range of prefixes.
  2. Work from a date range
  3. Work from a month prefix, but explicitly iterate over days.
      maybe use a separate goroutine for each date?

Usage:




*/

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"sync"

	"github.com/spaolacci/murmur3"
	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
)

var (
	fProject   = flag.String("project", "", "Project containing queues.")
	fQueue     = flag.String("queue", "etl-ndt-batch_", "Base of queue name.")
	fNumQueues = flag.Int("num_queues", 5, "Number of queues.  Normally determined by listing queues.")
	fBucket    = flag.String("bucket", "archive-mlab-oti", "Source bucket.")
	fExper     = flag.String("experiment", "ndt", "Experiment prefix, trailing slash optional")
	fMonth     = flag.String("month", "", "Single month spec, as YYYY/MM")
	fDay       = flag.String("day", "", "Single day spec, as YYYY/MM/DD")

	errCount      = 1
	storageClient *storage.Client
	bucket        *storage.BucketHandle

	hasher = murmur3.New32()
)

func init() {
}

func postOne(queue string, bucket string, fn string) error {
	reqStr := fmt.Sprintf("http://queue-pusher-dot-mlab-oti.appspot.com/receiver?queue=%s?filename=gs://%s/%s", queue, bucket, fn)
	resp, err := http.Get(reqStr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New("http error: " + resp.Status)
	}

	return nil
}

// Post all items in an ObjectIterator into specific
// queue.
func postDay(wg *sync.WaitGroup, queue string, it *storage.ObjectIterator) {
	defer wg.Done()
	wg.Add(1)
	for o, err := it.Next(); err != iterator.Done; o, err = it.Next() {
		fmt.Println(o.Name)
		if err != nil {
			fmt.Println(err)
			errCount++
			if errCount > 10 {
				panic(err)
			}
		}

		err = postOne(*fQueue, *fBucket, o.Name)
		if err != nil {
			fmt.Println(err)
			errCount++
			if errCount > 10 {
				panic(err)
			}
		}
	}
}

func queueFor(prefix string) string {
	hasher.Reset()
	hasher.Write([]byte(prefix))
	hash := hasher.Sum32()
	return fmt.Sprintf("%s%d", *fQueue, int(hash)%*fNumQueues)
}

func day(prefix string) {
	fmt.Println(prefix)
	q := storage.Query{
		Delimiter: "/",
		// TODO - validate.
		Prefix: prefix,
	}
	it := bucket.Objects(context.Background(), &q)
	fmt.Printf("%+v\n", it)
	queue := queueFor(prefix)
	var wg sync.WaitGroup
	defer wg.Wait()
	go postDay(&wg, queue, it)
}

func month(prefix string) {
	fmt.Println(prefix)
	q := storage.Query{
		Delimiter: "/",
		// TODO - validate.
		Prefix: prefix,
	}
	it := bucket.Objects(context.Background(), &q)

	fmt.Printf("%+v\n", it.PageInfo())
	var wg sync.WaitGroup
	for o, err := it.Next(); err != iterator.Done; o, err = it.Next() {
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("%+v\n", o)
		if o.Prefix != "" {
			q := storage.Query{
				Delimiter: "/",
				// TODO - validate.
				Prefix: o.Prefix,
			}
			it := bucket.Objects(context.Background(), &q)
			queue := queueFor(o.Prefix)
			go postDay(&wg, queue, it)
		} else {
			fmt.Println("Skipping: ", o.Name)
		}
	}
}

func main() {
	flag.Parse()

	var err error
	storageClient, err = storage.NewClient(context.Background())
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	bucket = storageClient.Bucket(*fBucket)
	attr, err := bucket.Attrs(context.Background())
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	fmt.Println(attr)

	if *fMonth != "" {
		month(*fExper + "/" + *fMonth + "/")
	} else if *fDay != "" {
		day(*fExper + "/" + *fDay + "/")
	}
}
