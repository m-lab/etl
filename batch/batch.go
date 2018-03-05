// Package batch provides tools for various batch processing actions,
// such as reprocessing a month or day or data, and handling deduplication.
package batch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/appengine/taskqueue"
)

// *******************************************************************
// OK Client, that just returns status ok and empty body
// For use when -dry_run is specified.
// *******************************************************************
type okTransport struct {
	lastReq *http.Request
}

type nopCloser struct {
	io.Reader
}

func (nc *nopCloser) Close() error { return nil }

// RoundTrip implements the RoundTripper interface, logging the
// request, and the response body, (which may be json).
func (t *okTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp := &http.Response{}
	resp.StatusCode = http.StatusOK
	resp.Body = &nopCloser{strings.NewReader("")}
	return resp, nil
}

func DryRunQueuerClient() *http.Client {
	client := &http.Client{}
	client.Transport = &okTransport{}
	return client
}

// This is used to intercept Get requests to the queue_pusher when invoked
// with -dry_run.
type dryRunHTTP struct{}

func (dr *dryRunHTTP) Get(url string) (resp *http.Response, err error) {
	resp = &http.Response{}
	resp.Body = ioutil.NopCloser(bytes.NewReader([]byte{}))
	resp.Status = "200 OK"
	resp.StatusCode = 200
	return
}

// *******************************************************************
// Queuer handles queueing of reprocessing requests
// *******************************************************************

// A Queuer provides the environment for queuing reprocessing requests.
type Queuer struct {
	Project    string                // project containing task queue
	QueueBase  string                // task queue base name
	NumQueues  int                   // number of task queues.
	BucketName string                // name of bucket containing task files
	Bucket     *storage.BucketHandle // bucket handle
	HTTPClient *http.Client          // Client to be used for http requests to queue pusher.
}

// ErrNilClient is returned when client is not provided.
var ErrNilClient = errors.New("nil http client not allowed")

// CreateQueuer creates a Queuer struct from provided parameters.  This does network ops.
//   httpClient - client to be used for queue_pusher calls.  Allows injection of fake for testing.
//                must be non-null.
//   opts       - ClientOptions, e.g. credentials, for tests that need to access storage buckets.
func CreateQueuer(httpClient *http.Client, opts []option.ClientOption, queueBase string, numQueues int, project, bucketName string, dryRun bool) (Queuer, error) {
	if httpClient == nil {
		return Queuer{}, ErrNilClient
	}

	storageClient, err := storage.NewClient(context.Background(), opts...)
	if err != nil {
		log.Println(err)
		panic(err)
	}

	bucket := storageClient.Bucket(bucketName)
	// Check that the bucket is valid, by fetching it's attributes.
	// Bypass check if we are running travis tests.
	if !dryRun {
		_, err = bucket.Attrs(context.Background())
		if err != nil {
			return Queuer{}, err
		}
	}

	return Queuer{project, queueBase, numQueues, bucketName, bucket, httpClient}, nil
}

// GetTaskqueueStats returns the stats for a list of queues.
// It should return a taskqueue.QueueStatistics for each queue.
// If any errors occur, the last error will be returned.
//  On request error, it will return a partial map (usually empty) and the error.
// If there are missing queues or parse errors, it will add an empty QueueStatistics
// object to the map, and continue, saving the error for later return.
// TODO update queue_pusher to process multiple queue names in one request.
// TODO add unit test.
func (q *Queuer) GetTaskqueueStats() (map[string][]taskqueue.QueueStatistics, error) {
	queueNames := make([]string, q.NumQueues)
	for i := range queueNames {
		queueNames[i] = fmt.Sprintf("%s%d", q.QueueBase, i)
	}
	allStats := make(map[string][]taskqueue.QueueStatistics, len(queueNames))
	var finalErr error
	for i := range queueNames {
		// Would prefer to use this, but it does not work from flex![]
		// stats, err := taskqueue.QueueStats(context.Background(), queueNames)
		resp, err := http.Get(fmt.Sprintf(`https://queue-pusher-dot-%s.appspot.com/stats?queuename=%s`, q.Project, queueNames[i]))
		if err != nil {
			finalErr = err
			log.Println(err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			finalErr = errors.New("Bad status on http request")
			break
		}
		data := make([]byte, 10000)
		var n int
		n, err = io.ReadFull(resp.Body, data)
		resp.Body.Close()
		if err != io.ErrUnexpectedEOF {
			finalErr = err
			log.Println(err)
			continue
		}
		var stats []taskqueue.QueueStatistics
		err = json.Unmarshal(data[:n], &stats)
		if err != nil {
			finalErr = err
			log.Println(err)
			log.Printf(`https://queue-pusher-dot-%s.appspot.com/stats?queuename=%s`, q.Project, queueNames[i])
			continue
		}
		allStats[queueNames[i]] = stats
	}
	return allStats, finalErr
}

// Initially this used a hash, but using day ordinal is better
// as it distributes across the queues more evenly.
func (q Queuer) queueForDate(date time.Time) string {
	day := date.Unix() / (24 * 60 * 60)
	return fmt.Sprintf("%s%d", q.QueueBase, int(day)%q.NumQueues)
}

// postOne sends a single https request to the queue pusher to add a task.
// Iff dryRun is true, this does nothing.
// TODO - move retry into this method.
// TODO - should use AddMulti - should be much faster.
//   however - be careful not to exceed quotas
func (q Queuer) postOneTask(queue, fn string) error {
	reqStr := fmt.Sprintf("https://queue-pusher-dot-%s.appspot.com/receiver?queue=%s&filename=gs://%s/%s", q.Project, queue, q.BucketName, fn)

	resp, err := q.HTTPClient.Get(reqStr)
	if err != nil {
		log.Println(err)
		// TODO - we don't see errors here or below when the queue doesn't exist.
		// That seems bad.
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		message := string(buf)
		if strings.Contains(message, "UNKNOWN_QUEUE") {
			return errors.New(message + " " + queue)
		}
		log.Println(string(buf))
		return errors.New(resp.Status + " :: " + message)
	}

	return nil
}

// postOneDay posts all items in an ObjectIterator into the appropriate queue.
// TODO - should this delete the data from the target partition before posting tasks?
func (q Queuer) postOneDay(wg *sync.WaitGroup, queue string, it *storage.ObjectIterator) error {
	if wg != nil {
		defer wg.Done()
	}

	fileCount := 0
	defer func() {
		log.Println("Added ", fileCount, " tasks to ", queue)
	}()

	qpErrCount := 0
	gcsErrCount := 0
	for o, err := it.Next(); err != iterator.Done; o, err = it.Next() {
		if err != nil {
			// TODO - should this retry?
			log.Println(err, "on it.Next")
			gcsErrCount++
			if gcsErrCount > 5 {
				log.Printf("Failed after %d files to %s.\n", fileCount, queue)
				return err
			}
			continue
		}

		err = q.postOneTask(queue, o.Name)
		if err != nil {
			log.Println(err)
			if strings.Contains(err.Error(), "UNKNOWN_QUEUE") {
				return err
			}
			if strings.Contains(err.Error(), "invalid filename") {
				// Invalid filename is never going to get better.
				continue
			}
			log.Println(err, o.Name, "Retrying")
			// Retry once
			time.Sleep(10 * time.Second)
			err = q.postOneTask(queue, o.Name)

			if err != nil {
				log.Println(err, o.Name, "FAILED")
				qpErrCount++
				if qpErrCount > 5 {
					log.Printf("Failed after %d files to %s (on %s).\n", fileCount, queue, o.Name)
					return err
				}
			}
		} else {
			fileCount++
		}
	}
	return nil
}

// PostDay fetches an iterator over the objects with ndt/YYYY/MM/DD prefix,
// and passes the iterator to postDay with appropriate queue.
// Iff wq is not nil, PostDay will call done on wg when finished
// posting.
// This typically takes about 10 minutes, whether single or parallel days.
func (q Queuer) PostDay(wg *sync.WaitGroup, prefix string) error {
	date, err := time.Parse("ndt/2006/01/02/", prefix)
	if err != nil {
		log.Println("Failed parsing date from ", prefix)
		log.Println(err)
		if wg != nil {
			wg.Done()
		}
		return err
	}
	queue := q.queueForDate(date)
	log.Println("Adding ", prefix, " to ", queue)
	qry := storage.Query{
		Delimiter: "/",
		Prefix:    prefix,
	}
	// TODO - can this error?  Or do errors only occur on iterator ops?
	it := q.Bucket.Objects(context.Background(), &qry)
	if wg != nil {
		// TODO - this ignores errors.
		go q.postOneDay(wg, queue, it)
	} else {
		return q.postOneDay(nil, queue, it)
	}
	return nil
}

// PostMonth adds all of the files from dates within a specified month.
// It is difficult to test without hitting GCS.  8-(
// This typically takes about 10 minutes, processing all days concurrently.
// May return an error, but some PostDay errors may not be detected or propagated.
func (q Queuer) PostMonth(prefix string) error {
	qry := storage.Query{
		Delimiter: "/",
		// TODO - validate.
		Prefix: prefix,
	}
	it := q.Bucket.Objects(context.Background(), &qry)

	var wg sync.WaitGroup
	defer wg.Wait()

	errCount := 0
	const maxErrors = 20
	for o, err := it.Next(); err != iterator.Done; o, err = it.Next() {
		if err != nil {
			log.Println(err)
			if strings.Contains(err.Error(),
				"does not have storage.objects.list access") {
				// Inadequate permissions
				return err
			}
			if errCount++; errCount > maxErrors {
				log.Println("Too many errors.  Breaking loop.")
				return err
			}
		} else if o.Prefix != "" {
			wg.Add(1)
			err := q.PostDay(&wg, o.Prefix)
			if err != nil {
				log.Println(err)
				if errCount++; errCount > maxErrors {
					log.Println("Too many errors.  Breaking loop.")
					return err
				}
			}
		} else {
			log.Println("Skipping: ", o.Name)
		}
	}
	return nil
}
