// Package dedup provides facilities for deduplicating
// template tables and copying into a destination partitions.
// It is currently somewhat NDT specific:
//  1. It expects tables to have task_filename field.
//  2. It expects destination table to be partitioned.
//  3. It does not explicitly check for schema compatibility,
//      though it will fail if they are incompatible.

package dedup

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/etl"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"gopkg.in/m-lab/go.v1/bqext"
)

var (
	// ErrorNotRegularTable is returned when a table is not a regular table (e.g. views)
	ErrorNotRegularTable = errors.New("Not a regular table")
	// ErrorSrcOlderThanDest is returned if a source table is older than the destination partition.
	ErrorSrcOlderThanDest = errors.New("Source older than destination partition")
	// ErrorTooFewTasks is returned when the source table has fewer task files than the destination.
	ErrorTooFewTasks = errors.New("Too few tasks")
	// ErrorTooFewTests is returned when the source table has fewer tests than the destination.
	ErrorTooFewTests = errors.New("Too few tests")
)

// Detail provides more detailed information about a partition or table.
type Detail struct {
	PartitionID   string // May be empty.  Used for slices of partitions.
	TaskFileCount int
	TestCount     int
}

// GetTableDetail fetches more detailed info about a partition or table.
// Expects table to have test_id, and task_filename fields.
//func GetTableDetail(dsExt *bqext.Dataset, table string, partition string) (Detail, error) {
func GetTableDetail(dsExt *bqext.Dataset, table *bigquery.Table) (Detail, error) {
	// If table is a partition, then we have to separate out the partition part for the query.
	parts := strings.Split(table.TableID, "$")
	dataset := table.DatasetID
	tableName := parts[0]
	where := ""
	if len(parts) > 1 {
		if len(parts[1]) == 8 {
			where = "where _PARTITIONTIME = PARSE_TIMESTAMP(\"%Y%m%d\",\"" + parts[1] + "\")"
		} else {
			return Detail{}, errors.New("Invalid partition string: " + parts[1])
		}
	}
	detail := Detail{}
	queryString := fmt.Sprintf(`
		#standardSQL
		SELECT SUM(tests) AS TestCount, COUNT(task)-1 AS TaskFileCount
		FROM (
			-- This avoids null counts when the partition doesn't exist or is empty.
  		    SELECT 0 AS tests, "fake-task" AS Task 
  		    UNION ALL
		  	SELECT COUNT(test_id) AS tests, task_filename AS task
		  	FROM `+"`%s.%s`"+`
		  	%s  -- where clause
		  	GROUP BY Task
		)`, dataset, tableName, where)

	err := dsExt.QueryAndParse(queryString, &detail)
	return detail, err
}

// TableInfo contains the basic stats for a specific table
// or partition.
type TableInfo struct {
	Name             string
	IsPartitioned    bool
	NumBytes         int64
	NumRows          uint64
	CreationTime     time.Time
	LastModifiedTime time.Time
}

// GetTableInfo returns the basic info for a single table.
func GetTableInfo(t *bigquery.Table) (TableInfo, error) {
	ctx := context.Background()
	meta, err := t.Metadata(ctx)
	if err != nil {
		return TableInfo{}, err
	}
	if meta.Type != bigquery.RegularTable {
		return TableInfo{}, ErrorNotRegularTable
	}
	ts := TableInfo{
		Name:             t.TableID,
		IsPartitioned:    meta.TimePartitioning != nil,
		NumBytes:         meta.NumBytes,
		NumRows:          meta.NumRows,
		CreationTime:     meta.CreationTime,
		LastModifiedTime: meta.LastModifiedTime,
	}
	return ts, nil
}

// GetTableInfoMatching finds all tables matching table filter
// and collects the basic stats about each of them.
// Returns slice ordered by decreasing age.
func GetTableInfoMatching(dsExt *bqext.Dataset, filter string) ([]TableInfo, error) {
	result := make([]TableInfo, 0)
	ctx := context.Background()
	ti := dsExt.Tables(ctx)
	for t, err := ti.Next(); err == nil; t, err = ti.Next() {
		// TODO should this be starts with?  Or a regex?
		if strings.Contains(t.TableID, filter) {
			// TODO - make this run in parallel
			ts, err := GetTableInfo(t)
			if err == ErrorNotRegularTable {
				continue
			}
			if err != nil {
				return []TableInfo{}, err
			}
			result = append(result, ts)
		}
	}
	sort.Slice(result[:], func(i, j int) bool {
		return result[i].LastModifiedTime.Before(result[j].LastModifiedTime)
	})
	return result, nil
}

var denseDateSuffix = regexp.MustCompile(`(.*)([_$])(` + etl.YYYYMMDD + `)$`)

// Get the table prefix/base, separator, and partition/suffix
func getTableParts(tableName string) ([]string, error) {
	date := denseDateSuffix.FindStringSubmatch(tableName)
	if len(date) != 4 || len(date[3]) != 8 {
		return []string{}, errors.New("Invalid template suffix: " + tableName)
	}
	return date[1:4], nil
}

func getTable(bqClient *bigquery.Client, project, dataset, table, partition string) (*bigquery.Table, error) {
	// This should fail
	date := denseDateSuffix.FindStringSubmatch(table)
	if len(date) > 0 {

		return nil, errors.New("Invalid table base: " + table)
	}
	return bqClient.DatasetInProject(project, dataset).Table(table + "$" + partition), nil
}

// GetPartitionInfo provides basic information about a partition.
// Unlike bqext.GetPartitionInfo, this works directly on a bigquery.Table.
// table should include partition spec.
// dsExt should have access to the table, but its project and dataset are not used.
// TODO - possibly migrate this to go/bqext.
func GetPartitionInfo(ctx context.Context, dsExt *bqext.Dataset, table *bigquery.Table) (bqext.PartitionInfo, error) {
	tableName := table.TableID
	parts := strings.Split(tableName, "$")
	if len(parts) != 2 {
		return bqext.PartitionInfo{}, errors.New("TableID missing partition: " + tableName)
	}
	fullTable := fmt.Sprintf("%s:%s.%s", table.ProjectID, table.DatasetID, parts[0])

	// This uses legacy, because PARTITION_SUMMARY is not supported in standard.
	queryString := fmt.Sprintf(
		`#legacySQL
		SELECT
		  partition_id AS PartitionID,
		  MSEC_TO_TIMESTAMP(creation_time) AS CreationTime,
		  MSEC_TO_TIMESTAMP(last_modified_time) AS LastModified
		FROM
		  [%s$__PARTITIONS_SUMMARY__]
		WHERE partition_id = "%s" `, fullTable, parts[1])
	pi := bqext.PartitionInfo{}

	err := dsExt.QueryAndParse(queryString, &pi)
	if err != nil {
		// If the partition doesn't exist, just return empty Info, no error.
		if err == iterator.Done {
			return bqext.PartitionInfo{}, nil
		}
		return bqext.PartitionInfo{}, err
	}
	return pi, nil
}

func checkDetails(srcDetail, destDetail Detail) error {
	log.Println("Details: src:", srcDetail, " dest:", destDetail)
	// Check that new table contains at least 99% as many tasks as
	// old table.
	if destDetail.TaskFileCount > srcDetail.TaskFileCount {
		log.Printf("Warning - fewer task files: %d < %d\n", srcDetail.TaskFileCount, destDetail.TaskFileCount)
	} else if destDetail.TaskFileCount > int(1.01*float32(srcDetail.TaskFileCount)) {
		return ErrorTooFewTasks
	}

	// Check that new table contains at least 95% as many tests as
	// old table.  This may be fewer if the destination table still has dups.
	if destDetail.TestCount > srcDetail.TestCount {
		log.Printf("Warning - fewer tests: %d < %d\n", srcDetail.TestCount, destDetail.TestCount)
	} else if destDetail.TestCount > int(1.05*float32(srcDetail.TestCount)) {
		return ErrorTooFewTests
	}
	return nil
}

// TODO consider param to check whether source is older than dest.
func checkDestOlder(ctx context.Context, dsExt *bqext.Dataset, srcInfo TableInfo, dest *bigquery.Table) error {
	// Creation time of new table should be newer than last update
	// of old table.
	destPartitionInfo, err := GetPartitionInfo(ctx, dsExt, dest)
	if err != nil {
		return err
	}

	// If the source table is older than the destination table, then
	// don't overwrite it.
	if srcInfo.LastModifiedTime.Before(destPartitionInfo.LastModified) {
		// TODO should perhaps delete the source table?
		return ErrorSrcOlderThanDest
	}
	return nil
}

func waitForJob(ctx context.Context, job *bigquery.Job) error {
	previous := 0
	backoff := 1
	for {
		status, err := job.Status(ctx)
		if err != nil {
			return err
		}
		if status.Done() {
			if status.Err() != nil {
				return status.Err()
			}
			break
		}
		if backoff < 10 {
			tmp := previous
			previous = backoff
			backoff = backoff + tmp
		}
		time.Sleep(time.Duration(backoff))
	}

	return nil
}

// SafeCopyPartition uses several safety mechanisms to improve copy safety.
// Caller should also have checked source and destination task/test counts.
//  1. Source is required to be a partition.
//  2. Destination partition is derived from source partition.
//  3. Source and destination have the same partition date.
//  4. Source table mod time later than destination table mod time.
// TODO(gfr) Ideally this should be done by a separate process with
// higher priviledge than the reprocessing and dedupping processes.
func SafeCopyPartition(ctx context.Context, client *bigquery.Client, srcTable *bigquery.Table, destDataset, destTableName string) error {
	parts, err := getTableParts(srcTable.TableID)
	if err != nil {
		return err
	}

	// TODO Copy table from intermediate to destination.
	destTable, err := getTable(client, srcTable.ProjectID, destDataset, destTableName, parts[2])
	if err != nil {
		log.Println(err)
		return err
	}

	copier := destTable.CopierFrom(srcTable)
	copier.WriteDisposition = bigquery.WriteTruncate
	log.Println("Copying...")
	job, err := copier.Run(ctx)
	if err != nil {
		return err
	}

	err = waitForJob(context.Background(), job)
	log.Println("Done")
	return err
}

// Options provides processing options for Dedup_Alpha
type Options struct {
	MinSrcAge     time.Duration
	IgnoreDestAge bool
	DryRun        bool
}

// CheckAndDedup checks various criteria, and if they all pass,
// dedups the table.  Returns true if criteria pass, false if they fail.
// Returns nil error on success, or non-nil error if there was an error
// at any point.
// Criteria:
//   1.  Source table modification time is at least XXX in the past.
//   2.  Source table mod time is later than destination table mod time.
//   3.  Source reflects at least as many task files as destination.
//   4.  Source has at least 98% as many tests as destination (including dups)
//   5.  After deduplication, intermediate table has at least 98% as many tests as destination.
//
// dsExt         - bqext.Dataset for operations.
// srcInfo       - TableInfo for the source
// destTable     - destination Table (possibly in another dataset)
// minSrcAge     - minimum source age
// ignoreDestAge - if true, will ignore destination age sanity check
//
// TODO(gfr) Should we check that intermediate table is NOT a production table?
func CheckAndDedup(ctx context.Context, dsExt *bqext.Dataset, srcInfo TableInfo, destTable *bigquery.Table, options Options) (bool, error) {
	// Check if the last update was at least minSrcAge in the past.
	if time.Now().Sub(srcInfo.LastModifiedTime) < options.MinSrcAge {
		return false, errors.New("Source is too recent")
	}

	parts, err := getTableParts(srcInfo.Name)
	if err != nil {
		return false, err
	}
	intermediateTable, err := getTable(dsExt.BqClient, dsExt.ProjectID, dsExt.DatasetID, parts[0], parts[2])
	if err != nil {
		log.Println(err)
		return false, err
	}

	if destTable.DatasetID == intermediateTable.DatasetID {
		return false, errors.New("Intermediate and Destination should be in different datasets: " + intermediateTable.FullyQualifiedName())
	}

	t := dsExt.Table(srcInfo.Name)

	// Just check that srcInfo table exists.
	_, err = t.Metadata(ctx)
	if err != nil {
		log.Println(err)
		return false, err
	}

	if !options.IgnoreDestAge {
		err = checkDestOlder(ctx, dsExt, srcInfo, destTable)
		if err != nil {
			log.Println(err)
			return false, err
		}
	}
	srcTable := dsExt.Table(srcInfo.Name)

	srcDetail, err := GetTableDetail(dsExt, srcTable)
	if err != nil {
		return false, err
	}
	destDetail, err := GetTableDetail(dsExt, destTable)
	if err != nil {
		return false, err
	}

	err = checkDetails(srcDetail, destDetail)
	if err != nil {
		log.Println(err)
		return false, err
	}

	// Do the deduplication to intermediate table with same root name.
	if options.DryRun {
		log.Println("Dedup dry run:", srcInfo.Name, "test_id", intermediateTable)
		return false, nil
	}

	// TODO - are we checking for source newer than intermediate destination?  Should we?
	_, err = dsExt.Dedup_Alpha(srcInfo.Name, "test_id", intermediateTable)
	if err != nil {
		log.Println(err)
		return false, err
	}

	// Now compare number of rows and tasks in intermediate table to destination table.
	intermediateDetail, err := GetTableDetail(dsExt, intermediateTable)
	if err != nil {
		return false, err
	}
	err = checkDetails(intermediateDetail, destDetail)
	if err != nil {
		return false, err
	}

	destParts, err := getTableParts(destTable.TableID)
	if err != nil {
		return false, err
	}

	err = SafeCopyPartition(ctx, dsExt.BqClient, intermediateTable, destTable.DatasetID, destParts[0])
	if err != nil {
		return false, err
	}

	// TODO If DeleteAfterDedup, then delete the source table.

	// TODO Update status table
	// We should have a status table that has a row for each table dedup operation.
	// It should record:
	//    date, dedup version, requester, cmd params
	//    source {table_date, mod date, task count, row count},
	//    precheck outcome
	//    dedup stats {elapsed time, bytes, rows, error}
	//    intermediate {table$date, task count, row count, byte count}
	//    destination {table$date, prev mod date, prev task count, prev row count, prev byte count}
	//    copycheck outcome
	//    final copy stats {elapsed time, bytes, rows, error}
	//    outcome (succeed, precheck failed, dedup failed, copycheck failed, copy failed)

	return true, nil
}

// ProcessTablesMatching lists all tables matching a template pattern, and for
// any that are at least two days old, attempts to dedup and copy them to
// partitions in the destination table.
func ProcessTablesMatching(dsExt *bqext.Dataset, srcPattern string, destDataset, destBase string, options Options) error {
	srcParts := strings.Split(srcPattern, "_")
	if len(srcParts) != 2 {
		return errors.New("Invalid source pattern: " + srcPattern)
	}

	// These are sorted by LastModification, oldest first.
	info, err := GetTableInfoMatching(dsExt, srcPattern)
	if err != nil {
		return err
	}
	log.Println("Examining", len(info), "source tables")
	for i := range info {
		log.Println(info[i])
	}
	for i := range info {
		srcInfo := info[i]
		// Skip any partition that has been updated in the past minAge.
		if time.Since(srcInfo.LastModifiedTime) < options.MinSrcAge {
			continue
		}

		parts, err := getTableParts(srcInfo.Name)
		if err != nil {
			return err
		}

		destTable, _ := getTable(dsExt.BqClient, dsExt.ProjectID, destDataset, destBase, parts[2])
		_, err = CheckAndDedup(context.Background(), dsExt, srcInfo, destTable, options)
		if err != nil {
			log.Println(err, "dedupping", dsExt.DatasetID+"."+srcInfo.Name, "to", destDataset+"."+destBase)
			return err
		}
	}
	return nil
}
