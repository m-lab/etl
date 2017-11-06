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
	"time"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
)

// BucketHandle defines the interface for either storage.BucketHandle or
// test fakes.
type BucketHandle interface {
	Attrs(ctx context.Context) (*storage.BucketAttrs, error)
	Objects(ctx context.Context, q *storage.Query) *storage.ObjectIterator
}

var (
	fProject = flag.String("project", "", "Project containing queues.")
	fQueue   = flag.String("queue", "etl-ndt-batch-", "Base of queue name.")
	// TODO implement listing queues to determine number of queue, and change this to 0
	fNumQueues = flag.Int("num_queues", 8, "Number of queues.  Normally determined by listing queues.")
	fBucket    = flag.String("bucket", "archive-mlab-oti", "Source bucket.")
	fExper     = flag.String("experiment", "ndt", "Experiment prefix, trailing slash optional")
	fMonth     = flag.String("month", "", "Single month spec, as YYYY/MM")
	fDay       = flag.String("day", "", "Single day spec, as YYYY/MM/DD")
	fDryRun    = flag.Bool("dry_run", false, "Prevents all output to queue_pusher.")

	storageClient *storage.Client
	bucket        BucketHandle
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// postOne sends a single https request to the queue pusher to add a task.
// Iff dryRun is true, this does nothing.
func postOne(queue string, bucket string, fn string) error {
	reqStr := fmt.Sprintf("https://queue-pusher-dot-%s.appspot.com/receiver?queue=%s&filename=gs://%s/%s", *fProject, queue, bucket, fn)
	if *fDryRun {
		return nil
	}

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
	qpErrCount := 0
	gcsErrCount := 0
	fileCount := 0
	if wg != nil {
		defer wg.Done()
	}
	for o, err := it.Next(); err != iterator.Done; o, err = it.Next() {
		if err != nil {
			// TODO - should this retry?
			log.Println(err)
			if gcsErrCount > 3 {
				log.Printf("Failed after %d files to %s.\n", fileCount, queue)
				return
			}
		}

		err = postOne(queue, *fBucket, o.Name)
		if err != nil {
			// TODO - should this retry?
			log.Println(err)
			if qpErrCount > 3 {
				log.Printf("Failed after %d files to %s.\n", fileCount, queue)
				return
			}
		} else {
			fileCount++
		}
	}
	log.Println("Added ", fileCount, " tasks to ", queue)
}

// Initially this used a hash, but using day ordinal is better
// as it distributes across the queues more evenly.
func queueFor(date time.Time) string {
	day := date.Unix() / (24 * 60 * 60)
	return fmt.Sprintf("%s%d", *fQueue, int(day)%*fNumQueues)
}

// day fetches an iterator over the objects with ndt/YYYY/MM/DD prefix,
// and passes the iterator to postDay with appropriate queue.
// Iff wq is not nil, postDay will call done on wg when finished
// posting.
func day(wg *sync.WaitGroup, prefix string) {
	date, err := time.Parse("ndt/2006/01/02/", prefix)
	if err != nil {
		log.Println("Failed parsing date from ", prefix)
		log.Println(err)
		if wg != nil {
			wg.Done()
		}
		return
	}
	queue := queueFor(date)
	log.Println("Adding ", prefix, " to ", queue)
	q := storage.Query{
		Delimiter: "/",
		Prefix:    prefix,
	}
	// TODO - can this error?
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
	if *fProject == "" && !*fDryRun {
		log.Println("Must specify project (or --dry_run)")
		flag.PrintDefaults()
		return
	}

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
