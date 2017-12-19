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
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
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

// Extracts date suffix from src table name, and forms full destination table.
func getDestTable(dsExt *bqext.Dataset, srcInfo TableInfo, destDataset, destBase string) (*bigquery.Table, error) {
	parts := strings.Split(srcInfo.Name, "_")
	if len(parts) != 2 {
		log.Println(parts)
		return nil, errors.New("Source doesn't have template suffix: " + srcInfo.Name)
	}
	destTable := dsExt.BqClient.DatasetInProject(dsExt.ProjectID, destDataset).Table(destBase + "$" + parts[1])
	return destTable, nil
}

// GetPartitionInfo provides basic information about a partition.
// Unlike bqextGetPartitionInfo, this works directly on a bigquery.Table.
// table should include partition spec.
// dsExt should have access to the table, but its project and dataset are not used.
// TODO - possibly migrate this to go/bqext.
func GetPartitionInfo(dsExt *bqext.Dataset, table *bigquery.Table) (bqext.PartitionInfo, error) {
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

func checkTasksAndTests(dsExt *bqext.Dataset, srcInfo TableInfo, dest *bigquery.Table) error {
	// Check if all task files in the old table are also present
	// in the new table.
	srcTable := dsExt.Table(srcInfo.Name)
	srcDetail, err := GetTableDetail(dsExt, srcTable)
	if err != nil {
		log.Println(err)
		return err
	}
	destDetail, err := GetTableDetail(dsExt, dest)
	if err != nil {
		log.Println(err)
		return err
	}

	log.Println("Details: src:", srcDetail, " dest:", destDetail)
	// Check that new table contains at least 99% as many tasks as
	// old table.
	if destDetail.TaskFileCount > int(1.01*float32(srcDetail.TaskFileCount)) {
		return ErrorTooFewTasks
	}
	// Check that new table contains at least 95% as many tests as
	// old table.  This may be fewer if the destination table still has dups.
	if destDetail.TestCount > int(1.05*float32(srcDetail.TestCount)) {
		return ErrorTooFewTests
	}
	return nil
}

// TODO consider param to check whether source is older than dest.
func checkDestOlder(dsExt *bqext.Dataset, srcInfo TableInfo, dest *bigquery.Table) error {
	// Creation time of new table should be newer than last update
	// of old table.
	destPartitionInfo, err := GetPartitionInfo(dsExt, dest)
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
// dsExt         - bqext.Dataset for operations.
// srcInfo       - TableInfo for the source
// destTable     - destination Table (possibly in another dataset)
// minSrcAge     - minimum source age
// ignoreDestAge - if true, will ignore destination age sanity check
func CheckAndDedup(dsExt *bqext.Dataset, srcInfo TableInfo, destTable *bigquery.Table, options Options) (bool, error) {
	// Check if the last update was at least minSrcAge in the past.
	if time.Now().Sub(srcInfo.LastModifiedTime) < options.MinSrcAge {
		return false, errors.New("Source is too recent")
	}

	t := dsExt.Table(srcInfo.Name)
	ctx := context.Background()

	// Just check that srcInfo table exists.
	_, err := t.Metadata(ctx)
	if err != nil {
		return false, err
	}

	if !options.IgnoreDestAge {
		err = checkDestOlder(dsExt, srcInfo, destTable)
		if err != nil {
			return false, err
		}
	}
	err = checkTasksAndTests(dsExt, srcInfo, destTable)
	if err != nil {
		return false, err
	}

	if options.DryRun {
		log.Println("Dedup dry run:", srcInfo.Name, "test_id", destTable)
	} else {
		dsExt.Dedup_Alpha(srcInfo.Name, "test_id", destTable)
	}

	// If DeleteAfterDedup, then delete the source table.

	return true, nil
}

// ProcessTablesMatching lists all tables matching a template pattern, and for
// any that are at least two days old, attempts to dedup and copy them to
// partitions in the destination table.
func ProcessTablesMatching(dsExt *bqext.Dataset, srcPattern string, destDataset, destBase string, options Options) error {
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

		destTable, err := getDestTable(dsExt, srcInfo, destDataset, destBase)
		if err != nil {
			return err
		}

		_, err = CheckAndDedup(dsExt, srcInfo, destTable, options)
		if err != nil {
			log.Println(err, "processing", srcInfo.Name)
		}
	}
	return nil
}
