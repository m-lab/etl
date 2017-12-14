// Package dedup provides facilities for deduplicating
// template tables and copying into a destination partitions.
// It is currently somewhat NDT specific:
//  1. It expects tables to have task_filename field.
//  2. It expected destination table to be partitioned.
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
	"golang.org/x/net/context"
	"gopkg.in/m-lab/go.v1/bqext"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// Error is a simple string satisfying the error interface.
type Error string

func (e Error) Error() string { return string(e) }

const (
	// ErrorNotRegularTable is returned when a table is not a regular table (e.g. views)
	ErrorNotRegularTable = Error("Not a regular table")
	// ErrorSrcOlderThanDest is returned if a source table is older than the destination partition.
	ErrorSrcOlderThanDest = Error("Source older than destination partition")
	// ErrorTooFewTasks is returned when the source table has fewer task files than the destination.
	ErrorTooFewTasks = Error("Too few tasks")
	// ErrorTooFewTests is returned when the source table has fewer tests than the destination.
	ErrorTooFewTests = Error("Too few tests")
)

// Detail provides more detailed information about a partition.
type Detail struct {
	PartitionID   string // May be empty.  Used for slices of partitions.
	TaskFileCount int
	TestCount     int
}

// GetNDTTableDetail fetches more detailed info about a partition or table.
// Expects table to have test_id, task_filename, and parse_time fields.
// `partition` should be in YYYY-MM-DD format.
func GetNDTTableDetail(dsExt *bqext.Dataset, table string, partition string) (Detail, error) {
	detail := Detail{}
	where := ""
	if len(partition) == 10 {
		where = "where _PARTITIONTIME = timestamp(date(\"" + partition + "\"))"
	} else if len(partition) != 0 {
		return detail, errors.New("Invalid partition string: " + partition)

	}
	queryString := fmt.Sprintf(`
		#standardSQL
		select sum(tests) as TestCount, count(Task) as TaskFileCount
		from (
		  select count(test_id) as Tests, task_filename as Task
		  from `+"`"+"%s"+"`"+`
		  %s  -- where clause
		  group by Task
		)`, table, where)

	log.Println(queryString)
	err := dsExt.QueryAndParse(queryString, &detail)
	return detail, err
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

// GetInfoMatching finds all tables matching table filter
// and collects the basic stats about each of them.
// If filter includes a $, then this fetches just the individual metadata
// for a single table partition.
// Returns slice ordered by decreasing age.
func GetInfoMatching(dsExt *bqext.Dataset, filter string) ([]TableInfo, error) {
	result := make([]TableInfo, 0)
	ctx := context.Background()
	ti := dsExt.Tables(ctx)
	for t, err := ti.Next(); err == nil; t, err = ti.Next() {
		// TODO should this be starts with?  Or a regex?
		if strings.Contains(t.TableID, filter) {
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

// CheckAndDedup checks various criteria, and if they all pass,
// dedups the table.  Returns true if criteria pass, false if they fail.
// Returns nil error on success, or non-nil error if there was an error
// at any point.
// dsExt
// srcInfo
// dest
// minSrcAge
// force - if true, will ignore age and test count sanity checks
func CheckAndDedup(dsExt *bqext.Dataset, srcInfo TableInfo, dest string, minSrcAge time.Duration, force bool) (bool, error) {
	// Check if the last update was at least fDelay in the past.
	if time.Now().Sub(srcInfo.LastModifiedTime) < minSrcAge {
		return false, errors.New("Source is too recent")
	}

	t := dsExt.Table(srcInfo.Name)
	ctx := context.Background()

	_, err := t.Metadata(ctx)
	if err != nil {
		return false, err
	}

	// Creation time of new table should be newer than last update
	// of old table.
	// TODO replace table name with destination table name.
	re := regexp.MustCompile("(.*)_([0-9]{4})([0-9]{2})([0-9]{2})")
	match := re.FindStringSubmatch(srcInfo.Name)
	if len(match) != 5 {
		log.Println(match)
		return false, errors.New("No matching partition_id: " + srcInfo.Name)
	}
	//base := string(match[1])
	suffix := string(match[2] + match[3] + match[4])
	destPartitionInfo, err := dsExt.GetPartitionInfo(dest, suffix)
	if err != nil {
		log.Println(err)
		return false, err
	}

	destTable := dsExt.Table(dest + "$" + suffix)
	if !force {
		// If the source table is older than the destination table, then
		// don't overwrite it.
		if srcInfo.LastModifiedTime.Before(destPartitionInfo.LastModified) {
			// TODO should perhaps delete the source table?
			return false, ErrorSrcOlderThanDest
		}

		// Get info on old table tasks and rows (and age).
		destInfo, err := GetTableInfo(destTable)
		if err != nil {
			log.Println(err)
			return false, err
		}
		log.Println(destInfo)

		// Double check against destination table info.
		// If the source table is older than the destination table, then
		// don't overwrite it.
		if srcInfo.LastModifiedTime.Before(destInfo.LastModifiedTime) {
			// TODO should perhaps delete the source table?
			return false, ErrorSrcOlderThanDest
		}

		// Check if all task files in the old table are also present
		// in the new table.
		srcDetail, err := GetNDTTableDetail(dsExt, srcInfo.Name, "")
		if err != nil {
			log.Println(err)
			return false, err
		}
		destDate := fmt.Sprintf("%s-%s-%s", match[2], match[3], match[4])
		destDetail, err := GetNDTTableDetail(dsExt, dest, destDate)
		if err != nil {
			log.Println(err)
			return false, err
		}

		// Check that new table contains at least 90% as many tasks as
		// old table.
		if destDetail.TaskFileCount > int(1.1*float32(srcDetail.TaskFileCount)) {
			return false, ErrorTooFewTasks
		}
		// Check that new table contains at least 95% as many tests as
		// old table.
		if destDetail.TestCount > int(1.05*float32(srcDetail.TestCount)) {
			return false, ErrorTooFewTests
		}
	}

	dsExt.Dedup_Alpha(srcInfo.Name, "test_id", destTable)

	// If DeleteAfterDedup, then delete the source table.

	return true, nil
}
