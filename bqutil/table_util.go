package bqutil

import (
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
	go_bqutil "gopkg.in/m-lab/go.v0/bqutil" // TODO - update when the package is stable.
)

// TableUtil wraps the bqutil.TableUtil, and adds a few more methods.
// TODO - migrate these into m-lab/go/bqutil
type TableUtil struct {
	go_bqutil.TableUtil
}

// NewTableUtil creates an underlying m-lab/go/bqutil/TableUtil, and wraps it in a TableUtil.
func NewTableUtil(project, dataset string, httpClient *http.Client, clientOpts ...option.ClientOption) (TableUtil, error) {
	util, err := go_bqutil.NewTableUtil(project, dataset, httpClient, clientOpts...)
	return TableUtil{util}, err
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
func (util *TableUtil) GetInfoMatching(dataset, filter string) []TableInfo {
	result := make([]TableInfo, 0)
	ctx := context.Background()
	ti := util.Dataset.Tables(ctx)
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

// DestinationQuery constructs a query with common Config settings for
// writing results to a table.
// Generally, may need to change WriteDisposition.
func (util *TableUtil) DestinationQuery(query string, dest *bigquery.Table) *bigquery.Query {
	q := util.BqClient.Query(query)
	if dest != nil {
		q.QueryConfig.Dst = dest
	} else {
		q.QueryConfig.DryRun = true
	}
	q.QueryConfig.AllowLargeResults = true
	// Default for unqualified table names in the query.
	q.QueryConfig.DefaultProjectID = util.Dataset.ProjectID
	q.QueryConfig.DefaultDatasetID = util.Dataset.DatasetID
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
func (util *TableUtil) Dedup(src string, overwrite bool, project, dataset, table string) {
	queryString := fmt.Sprintf(dedupTemplate, src)
	ds := util.BqClient.DatasetInProject(project, dataset)
	q := util.DestinationQuery(queryString, ds.Table(table))
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

// DedupInPlace executes a query that dedups a table.
// TODO interpret and return status.
func (util *TableUtil) DedupInPlace(src string) {
	queryString := fmt.Sprintf(dedupInPlace, src, src)
	q := util.ResultQuery(queryString, false)
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
