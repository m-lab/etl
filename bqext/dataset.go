package bqext

import (
	"fmt"
	"log"
	"sort"
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

// GetInfoMatching finDataset all tables matching table filter.
// and collects the basic stats about each of them.
// Returns slice ordered by decreasing age.
func (dsExt *Dataset) GetInfoMatching(dataset, filter string) []TableInfo {
	result := make([]TableInfo, 0)
	ctx := context.Background()
	ti := dsExt.Dataset.Tables(ctx)
	var t *bigquery.Table
	var err error
	for t, err = ti.Next(); err == nil; t, err = ti.Next() {
		// TODO should this be starts with?  Or a regex?
		if strings.Contains(t.TableID, filter) {
			meta, err := t.Metadata(ctx)
			if err != nil {
				log.Println(err)
			} else {
				if meta.Type != bigquery.RegularTable {
					continue
				}
				ts := TableInfo{
					Name:             t.TableID,
					IsPartitioned:    meta.TimePartitioning != nil,
					NumBytes:         meta.NumBytes,
					NumRows:          meta.NumRows,
					CreationTime:     meta.CreationTime,
					LastModifiedTime: meta.LastModifiedTime,
				}
				log.Println(t.TableID, " : ", meta.Name, " : ", meta.LastModifiedTime)
				result = append(result, ts)
			}
		}
	}
	if err != nil {
		log.Println(err)
	}
	sort.Slice(result[:], func(i, j int) bool {
		return result[i].LastModifiedTime.Before(result[j].LastModifiedTime)
	})
	return result
}

// PartitionInfo provides basic information about a partition.
type PartitionInfo struct {
	PartitionID  string
	CreationTime time.Time
	LastModified time.Time
}

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
	"  SELECT *, ROW_NUMBER() OVER (PARTITION BY test_id) row_number\n" +
	"  FROM `%s`)\n" +
	"WHERE row_number = 1\n"

// Dedup executes a query that dedups and writes to an appropriate
// partition.
// src is relative to the project:dataset of dsExt.
// project, dataset, table specify the table to write into.
// destination partition is based on the src suffix.
func (dsExt *Dataset) Dedup(src string, overwrite bool, project, dataset, table string) {
	queryString := fmt.Sprintf(dedupTemplate, src)
	dest := dsExt.BqClient.DatasetInProject(project, dataset)
	q := dsExt.DestinationQuery(queryString, dest.Table(table))
	if overwrite {
		q.QueryConfig.WriteDisposition = bigquery.WriteTruncate
	}
	job, err := q.Run(context.Background())
	if err != nil {
		// TODO add metric.
		log.Println(err)
	}
	log.Println(job.ID())
	log.Println(job.LastStatus())
	status, err := job.Wait(context.Background())
	if err != nil {
		log.Println(err)
	} else {
		log.Println(status)
		if status.Done() {
			log.Println("Done")
			log.Printf("%+v\n", *status.Statistics)
			log.Printf("%+v\n", status.Statistics.Details)
		}
	}
}

// TODO - really should take the one that was parsed last, instead
// of random
var dedupInPlace = "" +
	"# Delete all duplicate rows based on test_id\n" +
	"DELETE\n" +
	"  `%s` copy\n" +
	"WHERE\n" +
	"  CONCAT(copy.test_id, CAST(copy.parse_time AS string)) IN (\n" +
	"  SELECT\n" +
	"    CONCAT(test_id, CAST(parse_time AS string))\n" +
	"  FROM (\n" +
	"    SELECT\n" +
	"      test_id,\n" +
	"      parse_time,\n" +
	"      ROW_NUMBER() OVER (PARTITION BY test_id) row_number\n" +
	"    FROM\n" +
	"      `%s`)\n" +
	"  WHERE\n" +
	"    row_number > 1 )"

// DedupInPlace executes a query that dedups a table, in place, using DELETE.
// TODO interpret and return status.
func (dsExt *Dataset) DedupInPlace(src string) {
	queryString := fmt.Sprintf(dedupInPlace, src, src)
	q := dsExt.ResultQuery(queryString, false)
	job, err := q.Run(context.Background())
	if err != nil {
		// TODO add metric.
		log.Println(err)
	}
	log.Println(job.ID())
	log.Println(job.LastStatus())
	status, err := job.Wait(context.Background())
	if err != nil {
		log.Println(err)
	} else {
		log.Println(status)
		if status.Done() {
			log.Println("Done")
			log.Printf("%+v\n", *status.Statistics)
			log.Printf("%+v\n", status.Statistics.Details)
		}
	}
}
