package bqext

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
	go_bqext "gopkg.in/m-lab/go.v1/bqext" // TODO - update when the package is stable.
)

// Dataset wraps the bqExt.Dataset, and adds a few more methods.
// TODO - migrate these into m-lab/go/bqExt
type Dataset struct {
	go_bqext.Table
}

// NewDataset creates an underlying m-lab/go/bqext/Table, and wraps it in a Dataset.
func NewDataset(project, dataset string, clientOpts ...option.ClientOption) (Dataset, error) {
	dsExt, err := go_bqext.NewTable(project, dataset, clientOpts...)
	return Dataset{dsExt}, err
}

// TableInfo contains the critical stats for a specific table
// or partition.
type TableInfo struct {
	Name             string
	IsPartitioned    bool
	NumBytes         int64
	NumRows          uint64
	CreationTime     time.Time
	LastModifiedTime time.Time
}

// PartitionInfo provides basic information about a partition.
type PartitionInfo struct {
	PartitionID  string
	CreationTime time.Time
	LastModified time.Time
}

// PartitionInfo provides basic information about a partition.
func (dsExt Dataset) GetPartitionInfo(table string, partition string) (PartitionInfo, error) {
	// This uses legacy, because PARTITION_SUMMARY is not supported in standard.
	queryString := fmt.Sprintf(
		`#legacySQL
		SELECT
		  partition_id as PartitionID,
		  msec_to_timestamp(creation_time) AS CreationTime,
		  msec_to_timestamp(last_modified_time) AS LastModified
		FROM
		  [%s$__PARTITIONS_SUMMARY__]
		where partition_id = "%s" `, table, partition)
	pi := PartitionInfo{}

	err := dsExt.QueryAndParse(queryString, &pi)
	if err != nil {
		log.Println(err)
		return PartitionInfo{}, err
	}
	return pi, nil
}

// DestinationQuery constructs a query with common Config settings for
// writing results to a table.
// Generally, may need to change WriteDisposition.
func (dsExt *Dataset) DestinationQuery(query string, dest *bigquery.Table) *bigquery.Query {
	q := dsExt.BqClient.Query(query)
	if dest != nil {
		q.QueryConfig.Dst = dest
	} else {
		q.QueryConfig.DryRun = true
	}
	q.QueryConfig.AllowLargeResults = true
	// Default for unqualified table names in the query.
	q.QueryConfig.DefaultProjectID = dsExt.Dataset.ProjectID
	q.QueryConfig.DefaultDatasetID = dsExt.Dataset.DatasetID
	q.QueryConfig.DisableFlattenedResults = true
	return q
}

///////////////////////////////////////////////////////////////////
// Specific queries.
///////////////////////////////////////////////////////////////////

// TODO - really should take the one that was parsed last, instead
// of random
var dedupTemplate = "" +
	"#standardSQL\n" +
	"# Delete all duplicate rows based on test_id\n" +
	"SELECT * except (row_number)\n" +
	"FROM (\n" +
	"  SELECT *, ROW_NUMBER() OVER (PARTITION BY %s) row_number\n" +
	"  FROM `%s`)\n" +
	"WHERE row_number = 1\n"

// Dedup executes a query that dedups and writes to an appropriate
// partition.
// src is relative to the project:dataset of dsExt.
// dedupOn names the field to be used for dedupping.
// project, dataset, table specify the table to write into.
// NOTE: destination table must include the partition suffix.  This
// avoids accidentally overwriting TODAY's partition.
func (dsExt *Dataset) Dedup(src string, dedupOn string, overwrite bool, project, dataset, table string) (*bigquery.JobStatus, error) {
	if !strings.Contains(table, "$") {
		return nil, errors.New("Destination table does not specify partition")
	}
	queryString := fmt.Sprintf(dedupTemplate, dedupOn, src)
	dest := dsExt.BqClient.DatasetInProject(project, dataset)
	q := dsExt.DestinationQuery(queryString, dest.Table(table))
	if overwrite {
		q.QueryConfig.WriteDisposition = bigquery.WriteTruncate
	}
	log.Printf("Removing dups (of %s) and writing to %s\n", dedupOn, table)
	job, err := q.Run(context.Background())
	if err != nil {
		return nil, err
	}
	log.Println("JobID:", job.ID())
	status, err := job.Wait(context.Background())
	if err != nil {
		return status, err
	}
	return status, nil
}
