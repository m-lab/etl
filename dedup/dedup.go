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
	"github.com/m-lab/go/bqext"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

var (
	// ErrNotRegularTable is returned when a table is not a regular table (e.g. views)
	ErrNotRegularTable = errors.New("Not a regular table")
	// ErrSrcOlderThanDest is returned if a source table is older than the destination partition.
	ErrSrcOlderThanDest = errors.New("Source older than destination partition")
	// ErrTooFewTasks is returned when the source table has fewer task files than the destination.
	ErrTooFewTasks = errors.New("Too few tasks")
	// ErrTooFewTests is returned when the source table has fewer tests than the destination.
	ErrTooFewTests = errors.New("Too few tests")
)

// TableInfo contains the basic stats for a specific table or partition.
type TableInfo struct {
	Name             string
	IsPartitioned    bool
	NumBytes         int64
	NumRows          uint64
	CreationTime     time.Time
	LastModifiedTime time.Time
}

// GetTableInfo returns the basic info for a single table.
// It executes a single network request.
func GetTableInfo(ctx context.Context, t *bigquery.Table) (TableInfo, error) {
	meta, err := t.Metadata(ctx)
	if err != nil {
		return TableInfo{}, err
	}
	if meta.Type != bigquery.RegularTable {
		return TableInfo{}, ErrNotRegularTable
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

// Detail provides more detailed information about a partition or table.
type Detail struct {
	PartitionID   string // May be empty.  Used for slices of partitions.
	TaskFileCount int
	TestCount     int
}

// GetTableDetail fetches more detailed info about a partition or table.
// Expects table to have test_id, and task_filename fields.
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

	// TODO - this should take a context?
	err := dsExt.QueryAndParse(queryString, &detail)
	return detail, err
}

// GetTableInfoMatching finds all tables matching table filter
// and collects the basic stats about each of them.
// It performs many network operations, possibly two per table.
// Returns slice ordered by decreasing age.
func GetTableInfoMatching(ctx context.Context, dsExt *bqext.Dataset, filter string) ([]TableInfo, error) {
	result := make([]TableInfo, 0)
	ti := dsExt.Tables(ctx)
	for t, err := ti.Next(); err == nil; t, err = ti.Next() {
		// TODO should this be starts with?  Or a regex?
		if strings.Contains(t.TableID, filter) {
			// TODO - make this run in parallel
			ts, err := GetTableInfo(ctx, t)
			if err == ErrNotRegularTable {
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

// tableNameParts is used to describe a templated table or table partition.
type tableNameParts struct {
	prefix        string
	isPartitioned bool
	yyyymmdd      string
}

// getTableParts separates a table name into prefix/base, separator, and partition date.
// If tableName does not include valid yyyymmdd suffix, returns an error.
func getTableParts(tableName string) (tableNameParts, error) {
	date := denseDateSuffix.FindStringSubmatch(tableName)
	if len(date) != 4 || len(date[3]) != 8 {
		return tableNameParts{}, errors.New("Invalid template suffix: " + tableName)
	}
	return tableNameParts{date[1], date[2] == "$", date[3]}, nil
}

// getTable constructs a bigquery Table object from project/dataset/table/partition.
// The project/dataset/table/partition may or may not actually exist.
// This does NOT do any network operations.
// TODO(gfr) Probably should move this to go/bqext
func getTable(bqClient *bigquery.Client, project, dataset, table, partition string) (*bigquery.Table, error) {
	// This checks that the table name is NOT a partitioned or templated table.
	if strings.Contains(table, "$") || strings.Contains(table, "_") {
		return nil, errors.New("Table base must not include _ or $: " + table)
	}
	date := denseDateSuffix.FindStringSubmatch(table)
	if len(date) > 0 {
		return nil, errors.New("Table base must not include partition or template suffix: " + table)
	}

	full := table + "$" + partition
	_, err := getTableParts(full)
	if err != nil {
		return nil, err
	}

	// Oddly, a nil client is just fine here.
    // A nil client works here, but may lead to failures later, e.g.
    // if you create a copier.
	return bqClient.DatasetInProject(project, dataset).Table(full), nil
}

// GetPartitionInfo provides basic information about a partition.
// Unlike bqext.GetPartitionInfo, this works directly on a bigquery.Table.
// table should include partition spec.
// dsExt should have access to the table, but its project and dataset are not used.
// TODO - possibly migrate this to go/bqext.
func GetPartitionInfo(ctx context.Context, dsExt *bqext.Dataset, table *bigquery.Table) (bqext.PartitionInfo, error) {
	tableName := table.TableID
	parts, err := getTableParts(tableName)
	if err != nil || !parts.isPartitioned {
		return bqext.PartitionInfo{}, errors.New("TableID missing partition: " + tableName)
	}
	fullTable := fmt.Sprintf("%s:%s.%s", table.ProjectID, table.DatasetID, parts.prefix)

	// This uses legacy, because PARTITION_SUMMARY is not supported in standard.
	queryString := fmt.Sprintf(
		`#legacySQL
		SELECT
		  partition_id AS PartitionID,
		  MSEC_TO_TIMESTAMP(creation_time) AS CreationTime,
		  MSEC_TO_TIMESTAMP(last_modified_time) AS LastModified
		FROM
		  [%s$__PARTITIONS_SUMMARY__]
		WHERE partition_id = "%s" `, fullTable, parts.yyyymmdd)
	pInfo := bqext.PartitionInfo{}

	err = dsExt.QueryAndParse(queryString, &pInfo)
	if err != nil {
		// If the partition doesn't exist, just return empty Info, no error.
		if err == iterator.Done {
			return bqext.PartitionInfo{}, nil
		}
		return bqext.PartitionInfo{}, err
	}
	return pInfo, nil
}

func checkDetails(srcDetail, destDetail Detail) error {
	// Check that new table contains at least 99% as many tasks as
	// old table.
	if destDetail.TaskFileCount > int(1.01*float32(srcDetail.TaskFileCount)) {
		return ErrTooFewTasks
	} else if destDetail.TaskFileCount > srcDetail.TaskFileCount {
		log.Printf("Warning - fewer task files: %d < %d\n", srcDetail.TaskFileCount, destDetail.TaskFileCount)
	}

	// Check that new table contains at least 95% as many tests as
	// old table.  This may fail if the destination table still has dups.
	if destDetail.TestCount > int(1.05*float32(srcDetail.TestCount)) {
		return ErrTooFewTests
	} else if destDetail.TestCount > srcDetail.TestCount {
		log.Printf("Warning - fewer tests: %d < %d\n", srcDetail.TestCount, destDetail.TestCount)
	}
	return nil
}

// Fetches info about destination table, and checks that it's mtime is older than the source table's.
func checkDestOlder(ctx context.Context, dsExt *bqext.Dataset, srcInfo TableInfo, dest *bigquery.Table) error {
	// TODO - save and use existing info.
	destPartitionInfo, err := GetPartitionInfo(ctx, dsExt, dest)
	if err != nil {
		return err
	}

	if srcInfo.LastModifiedTime.Before(destPartitionInfo.LastModified) {
		return ErrSrcOlderThanDest
	}
	return nil
}

// WaitForJob waits for job to complete.  Uses fibonacci backoff until the backoff
// >= maxBackoff, at which point it continues using same backoff.
// TODO - move this to go/bqext, since it is bigquery specific and general purpose.
func WaitForJob(ctx context.Context, job *bigquery.Job, maxBackoff int) error {
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
		if backoff < maxBackoff {
			tmp := previous
			previous = backoff
			backoff = backoff + tmp
		}
		time.Sleep(time.Duration(backoff))
	}
	return nil
}

// SanityCheckAndCopy uses several sanity checks to improve copy safety.
// Caller should also have checked source and destination ages, and task/test counts.
//  1. Source is required to be a single partition or templated table with yyyymmdd suffix.
//  2. Destination partition is derived from source partition.
// TODO(gfr) Ideally this should be done by a separate process with
// higher priviledge than the reprocessing and dedupping processes.
// TODO(gfr) Also support copying from a template instead of partition?
func SanityCheckAndCopy(ctx context.Context, client *bigquery.Client, srcTable *bigquery.Table, destDataset, destTableName string) error {
	// Extract the
	parts, err := getTableParts(srcTable.TableID)
	if err != nil {
		return err
	}

	destTable, err := getTable(client, srcTable.ProjectID, destDataset, destTableName, parts.yyyymmdd)
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

	err = WaitForJob(context.Background(), job, 10)
	log.Println("Done")
	return err
}

// Options provides processing options for Dedup_Alpha
type Options struct {
	MinSrcAge     time.Duration
	IgnoreDestAge bool
	DryRun        bool
	CopyOnly      bool // Skip the dedup step and copy from intermediate to destination
}

// CheckAndDedup checks various criteria, and if they all pass,
// dedups the table.  Returns true if criteria pass, false if they fail.
// Returns nil error on success, or non-nil error if there was an error
// at any point.
// Criteria:
//   1.  Source and Dest are in different datasets.
//   2.  Source table modification time is at least XXX in the past.
//   3.  Source table mod time is later than destination table mod time.
//   4.  Source reflects at least 99% as many task files as destination.
//   5.  Source has at least 95% as many tests as destination (including dups)
//   6.  After deduplication, intermediate table has at least 95% as many tests as destination.
//
// dsExt         - bqext.Dataset containing the source (and used for intermediate)
// srcInfo       - TableInfo for the source
// destDataset   - destination dataset
// destBase      - destination table (base name without partition)
// options       - options, including min source age, ignoreDestAge, copyOnly
//
func CheckAndDedup(ctx context.Context, dsExt *bqext.Dataset, srcInfo TableInfo, destTable *bigquery.Table, options Options) (bool, error) {
	destParts, err := getTableParts(destTable.TableID)
	if err != nil {
		return false, err
	}

	// Check if the last update was at least minSrcAge in the past.
	if time.Now().Sub(srcInfo.LastModifiedTime) < options.MinSrcAge {
		return false, errors.New("Source is too recent")
	}

	if destTable.DatasetID == dsExt.DatasetID {
		return false, errors.New("Source and Destination should be in different datasets: ")
	}

	srcParts, err := getTableParts(srcInfo.Name)
	if err != nil {
		return false, err
	}

	if destParts.yyyymmdd != srcParts.yyyymmdd {
		return false, errors.New("Source and Destination should have same partition/template date: ")
	}
	intermediateTable, err := getTable(dsExt.BqClient, dsExt.ProjectID, dsExt.DatasetID, srcParts.prefix, srcParts.yyyymmdd)
	if err != nil {
		log.Println(err)
		return false, err
	}

	srcTable := dsExt.Table(srcInfo.Name)

	// Just check that table exists.
	_, err = srcTable.Metadata(ctx)
	if err != nil {
		log.Println(err)
		return false, err
	}

	if !options.IgnoreDestAge {
		// TODO - this fails if the destination partition does not exist.
		err = checkDestOlder(ctx, dsExt, srcInfo, destTable)
		if err != nil {
			log.Println(err)
			return false, err
		}
	}

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

	if !options.CopyOnly {
		// TODO - are we checking for source newer than intermediate destination?  Should we?
		_, err = dsExt.Dedup_Alpha(srcInfo.Name, "test_id", intermediateTable)
		if err != nil {
			log.Println(err)
			return false, err
		}
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

	err = SanityCheckAndCopy(ctx, dsExt.BqClient, intermediateTable, destTable.DatasetID, destParts.prefix)
	if err != nil {
		return false, err
	}

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

	// TODO If DeleteAfterDedup, then delete the source table.
	// bq rm 'intermediate$20160301' ??
	// bq rm 'batch.ndt_20160301'

	return true, nil
}

// ProcessTablesMatching lists all tables matching a template pattern, and for
// any that are at least the age specified in options, dedups and copies them to
// corresponding partitions in the destination table.
func ProcessTablesMatching(dsExt *bqext.Dataset, srcPattern string, destDataset, destBase string, options Options) error {
	// This may not have full suffix, so we can't use getTableParts.
	srcParts := strings.Split(srcPattern, "_")
	if len(srcParts) != 2 {
		return errors.New("Invalid source pattern: " + srcPattern)
	}

	// These are sorted by LastModification, oldest first.
	info, err := GetTableInfoMatching(context.Background(), dsExt, srcPattern)
	if err != nil {
		return err
	}

	// Print a summary of tables to be processed.
	log.Println("Examining", len(info), "source tables")
	for i := range info {
		log.Println(info[i])
	}

	// Process each table, serially, to avoid problems with too many concurrent
	// bigquery queries.
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

		destTable, _ := getTable(dsExt.BqClient, dsExt.ProjectID, destDataset, destBase, parts.yyyymmdd)
		_, err = CheckAndDedup(context.Background(), dsExt, srcInfo, destTable, options)
		if err != nil {
			log.Println(err, "dedupping", dsExt.DatasetID+"."+srcInfo.Name, "to", destDataset+"."+destBase)
			return err
		}
	}
	return nil
}
