package gcs

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"time"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/google-cloud-go-testing/storage/stiface"
	"github.com/m-lab/etl/etl"
	"google.golang.org/api/iterator"
)

// getFilesAfter returns list of all normal file objects with prefix and mTime > after.
// returns (objects, byteCount, error)
func getFilesSince(ctx context.Context, bh stiface.BucketHandle, prefix string, since time.Time) ([]*storage.ObjectAttrs, int64, error) {
	qry := storage.Query{
		Delimiter: "/", // This prevents traversing subdirectories.
		Prefix:    prefix,
	}
	// TODO - handle timeout errors?
	// TODO - should we add a deadline?
	it := bh.Objects(ctx, &qry)
	if it == nil {
		log.Println("Nil object iterator for", bh)
		return nil, 0, fmt.Errorf("Object iterator is nil.  BucketHandle: %v Prefix: %s", bh, prefix)
	}

	files := make([]*storage.ObjectAttrs, 0, 1000)

	byteCount := int64(0)
	gcsErrCount := 0
	for o, err := it.Next(); err != iterator.Done; o, err = it.Next() {
		if err != nil {
			// TODO - should this retry?
			// log the underlying error, with added context
			log.Println(err, "when attempting it.Next()")
			gcsErrCount++
			if gcsErrCount > 5 {
				log.Printf("Failed after %d files.\n", len(files))
				return files, byteCount, err
			}
			continue
		}

		if o.Updated.Before(since) {
			continue
		}
		byteCount += o.Size
		files = append(files, o)
	}
	return files, byteCount, nil
}

// *******************************************************************
// Storage Bucket related stuff.
//  TODO move to another package?
// *******************************************************************

// getBucket gets a storage bucket.
// TODO - this is currently duplicated in etl-gardener/state/state.go
//   opts       - ClientOptions, e.g. credentials, for tests that need to access storage buckets.
func getBucket(ctx context.Context, sClient stiface.Client, project, bucketName string) (stiface.BucketHandle, error) {
	bucket := sClient.Bucket(bucketName)
	if bucket == nil {
		return nil, errors.New("Nil bucket")
	}
	// Check that the bucket is valid, by fetching it's attributes.
	// Bypass check if we are running travis tests.
	_, err := bucket.Attrs(ctx)
	if err != nil {
		return nil, err
	}
	return bucket, nil
}

// GetFilesSince gets list of all storage objects with prefix, created or updated since given date.
// TODO - similar to code in etl-gardener/cloud/tq/tq.go.  Should move to go/cloud/gcs
func GetFilesSince(ctx context.Context, sClient stiface.Client, project string, prefix string, since time.Time) ([]*storage.ObjectAttrs, int64, error) {
	// Submit all files from the bucket that match the prefix.
	p, err := ParsePrefix(prefix)
	if err != nil {
		// If there is a parse error, log and skip request.
		log.Println(err)
		return nil, 0, err
	}

	// Use a real storage bucket.
	// TODO - add a persistent storageClient to the rex object?
	// TODO - try cancelling the context instead?
	bucket, err := getBucket(ctx, sClient, project, p.Bucket)
	if err != nil {
		// if err == io.EOF && env.TestMode {
		// 	log.Println("Using fake client, ignoring EOF error")
		// 	return nil, 0, nil
		// }
		log.Println(err)
		return nil, 0, err
	}
	if bucket == nil {
		log.Println("Nil bucket for", project, prefix)
		return nil, 0, fmt.Errorf("Nil bucket for %s %s", project, prefix)
	}

	return getFilesSince(ctx, bucket, p.Path(), since)
}

// CODE BELOW is common with etl-gardener state.go.

// These are here to facilitate use across queue-pusher and parsing components.
var (
	// This matches any valid test file name, and some invalid ones.
	prefixPattern = regexp.MustCompile(etl.BucketPattern + // #1 - e.g. gs://archive-mlab-sandbox
		etl.ExpTypePattern + // #2 #3 - e.g. ndt/tcpinfo
		etl.DatePathPattern) // #4 - YYYY/MM/DD
)

// Prefix is a valid gs:// prefix for either legacy or new platform data.
type Prefix struct {
	Bucket     string    // the GCS bucket name.
	Experiment string    // the experiment name
	DataType   string    // if empty, this is legacy, and DataType is same as Experiment
	DatePath   string    // the YYYY/MM/DD date path.
	Date       time.Time // the time.Time corresponding to the datepath.
}

// Path returns the path within the bucket, not including the leading gs://bucket/
func (p Prefix) Path() string {
	if p.Experiment == "" {
		return p.DataType + "/" + p.DatePath + "/"
	}
	return p.Experiment + "/" + p.DataType + "/" + p.DatePath + "/"
}

// ParsePrefix Parses prefix, returning {bucket, experiment, date string}, error
// Unless it returns error, the result will be exactly length 3.
func ParsePrefix(prefix string) (*Prefix, error) {
	fields := prefixPattern.FindStringSubmatch(prefix)

	if fields == nil {
		return nil, errors.New("Invalid test path: " + prefix)
	}

	date, err := time.Parse("2006/01/02", fields[4])
	if err != nil {
		return nil, err
	}
	p := Prefix{
		Bucket:     fields[1],
		Experiment: fields[2],
		DataType:   fields[3],
		DatePath:   fields[4],
		Date:       date,
	}

	return &p, nil
}
