// Package main defines a command line tool for submitting date
// ranges for reprocessing
package main

// TODO - note about setting up batch table and giving permission.

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
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
)

var (
	fProject   = flag.String("project", "", "Project containing queues.")
	fQueue     = flag.String("queue", "etl-ndt-batch-", "Base of queue name.")
	// TODO implement listing queues to determine number of queue, and change this to 0
	fNumQueues = flag.Int("num_queues", 8, "Number of queues.  Normally determined by listing queues.")
	fBucket    = flag.String("bucket", "archive-mlab-oti", "Source bucket.")
	fExper     = flag.String("experiment", "ndt", "Experiment prefix, trailing slash optional")
	fMonth     = flag.String("month", "", "Single month spec, as YYYY/MM")
	fDay       = flag.String("day", "", "Single day spec, as YYYY/MM/DD")

	qpErrCount      int32
	gcsErrCount int32
	storageClient *storage.Client
	bucket        *storage.BucketHandle
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func postOne(queue string, bucket string, fn string) error {
	reqStr := fmt.Sprintf("https://queue-pusher-dot-%s.appspot.com/receiver?queue=%s&filename=gs://%s/%s", *fProject, queue, bucket, fn)
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
	if wg != nil {
		defer wg.Done()
	}
	for o, err := it.Next(); err != iterator.Done; o, err = it.Next() {
		if err != nil {
			// TODO - should this retry?
			log.Println(err)
			ec := atomic.AddInt32(&gcsErrCount, 1)
			if ec > 10 {
				panic(err)
			}
		}

		err = postOne(queue, *fBucket, o.Name)
		if err != nil {
			// TODO - should this retry?
			log.Println(err)
			ec := atomic.AddInt32(&qpErrCount, 1)
			if ec > 10 {
				panic(err)
			}
		}
	}
}

// Initially this used a hash, but using day ordinal is better
// as it distributes across the queues more evenly.
func queueFor(date time.Time) string {
	day := date.Unix() / (24 * 60 * 60)
	return fmt.Sprintf("%s%d", *fQueue, int(day)%*fNumQueues)
}

func dateFromPrefixOrDie(prefix string) time.Time {
	date, err := time.Parse("ndt/2006/01/02/", prefix)
	if err != nil {
		log.Println(err)
		panic(err)
	}
	return date
}

func day(wg *sync.WaitGroup, prefix string) {
	queue := queueFor(dateFromPrefixOrDie(prefix))
	log.Println("Adding ", prefix, " to ", queue)
	q := storage.Query{
		Delimiter: "/",
		// TODO - validate.
		Prefix: prefix,
	}
	it := bucket.Objects(context.Background(), &q)
	go postDay(wg, queue, it)
}

func month(prefix string) {
	q := storage.Query{
		Delimiter: "/",
		// TODO - validate.
		Prefix: prefix,
	}
	it := bucket.Objects(context.Background(), &q)

	var wg sync.WaitGroup
	for o, err := it.Next(); err != iterator.Done; o, err = it.Next() {
		if err != nil {
			log.Println(err)
		} else if o.Prefix != "" {
			wg.Add(1)
			day(&wg, o.Prefix)
		} else {
			log.Println("Skipping: ", o.Name)
		}
	}
	wg.Wait()
}

func main() {
	flag.Parse()

	var err error
	storageClient, err = storage.NewClient(context.Background())
	if err != nil {
		log.Println(err)
		panic(err)
	}

	bucket = storageClient.Bucket(*fBucket)
	_, err = bucket.Attrs(context.Background())
	if err != nil {
		log.Println(err)
		panic(err)
	}

	if *fMonth != "" {
		month(*fExper + "/" + *fMonth + "/")
	} else if *fDay != "" {
		day(nil, *fExper+"/"+*fDay+"/")
	}
}
