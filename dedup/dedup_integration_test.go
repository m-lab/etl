// +build integration

package dedup_test

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/m-lab/etl/dedup"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"gopkg.in/m-lab/go.v1/bqext"
)

func newTestingDataset(project, dataset) (*bqext.Dataset, error) {
	if os.Getenv("TRAVIS") != "" {
		authOpt := option.WithCredentialsFile("../travis-testing.key")
		opts = append(opts, authOpt)
	}
	return bqext.NewDataset("mlab-testing", "go", opts...)
}

func TestCheckAndDedup(t *testing.T) {
	dsExt, err := NewTestingDataset("mlab-testing", "etl")
	if err != nil {
		t.Fatal(err)
	}

	info, err := dedup.GetInfoMatching(&dsExt, "TestDedupSrc_19990101")

	_, err = dedup.CheckAndDedup(&dsExt, info[0], "TestDedupDest", time.Hour, false)
	if err != nil {
		log.Println(err)
	}
	_, err = dedup.CheckAndDedup(&dsExt, info[0], "TestDedupDest", time.Hour, true)
	if err != nil {
		t.Error(err)
	}
}

func xTest() {
	dsExt, err := NewTestingDataset("mlab-testing", "etl")
	if err != nil {
		log.Fatal(err)
	}

	info, err := dedup.GetInfoMatching(&dsExt, "TestDedupSrc_19990101")
	log.Println(info)

	for i := range info {
		// TODO Query to check number of tar files processed.
		fmt.Printf("%v\n", info[i])

		// TODO Query to check number of rows?
		queryString := fmt.Sprintf("select count(test_id) as Tests, task_filename as Task from `%s` group by task_filename order by task_filename", info[i].Name)
		q := dsExt.ResultQuery(queryString, false)
		it, err := q.Read(context.Background())
		if err != nil {
			log.Println(err)
			continue
		}
		for {
			var result struct {
				Task  string
				Tests int
			}
			err := it.Next(&result)
			if err != nil {
				if err != iterator.Done {
					log.Println(err)
				}
				break
			}
			log.Println(result)
			// TODO compare the tasks to those in the existing
			// partition.  If there are some missing, then delay
			// further, and log a warning.  If still missing when
			// we commit or more than 3 missing, log an error.
		}
	}
}
