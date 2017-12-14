// Package main defines a command line tool for deduplicating
// tempalte tables and copying into a destination partitions.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"gopkg.in/m-lab/go.v1/bqext"
)

// TODO - should move this to a command line specific module,
// and replace with fields in a struct that is used as the receiver
// for most of the functions.
var (
	// TODO - replace this with a service account?
	fProject          = flag.String("project", "", "BigQuery project.")
	fTemplatePrefix   = flag.String("template_prefix", "etl.src", "table prefix")
	fDelay            = flag.Float64("delay", 48, "delay (hours) from last update")
	fDestinationTable = flag.String("destination_table", "etl.dest", "destination table")
	fDedupField       = flag.String("dedup_field", "", "Field for deduplication")
	fDeleteAfterCopy  = flag.Bool("delete", false, "Should delete table after copy")
	fDryRun           = flag.Bool("dry_run", false, "Print actions instead of executing")
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

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

// Error is a simple string satisfying the error interface.
type Error string

func (e Error) Error() string { return string(e) }

// ErrorNotRegularTable is returned when a table is not a regular table (e.g. views)
const ErrorNotRegularTable = Error("Not a regular table")

// ErrorSrcOlderThanDest is returned if a source table is older than the destination partition.
const ErrorSrcOlderThanDest = Error("Source older than destination partition")
const ErrorTooFewTasks = Error("Too few tasks")
const ErrorTooFewTests = Error("Too few tests")

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
// Uses flags to determine most of the parameters.
func CheckAndDedup(dsExt *bqext.Dataset, srcInfo TableInfo, dest string) (bool, error) {
	// Check if the last update was at least fDelay in the past.
	if time.Now().Sub(srcInfo.LastModifiedTime).Hours() < *fDelay {
		return false, nil
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
	// If the source table is older than the destination table, then
	// don't overwrite it.
	if srcInfo.LastModifiedTime.Before(destPartitionInfo.LastModified) {
		// TODO should perhaps delete the source table?
		//		return false, ErrorSrcOlderThanDest
	}

	// Get info on old table tasks and rows (and age).
	// TODO - fix so that we don't need Dataset.
	destTable := dsExt.Table(dest + "$" + suffix)
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
		//return false, ErrorSrcOlderThanDest
	}

	// Check if all task files in the old table are also present
	// in the new table.
	srcDetail, err := GetNDTTableDetail(dsExt, srcInfo.Name, "")
	if err != nil {
		log.Println(err)
	} else {
		log.Println(srcDetail)
	}
	if srcDetail.TaskFileCount == 0 {
		// No tasks - should ignore?
	}
	destDate := fmt.Sprintf("%s-%s-%s", match[2], match[3], match[4])
	destDetail, err := GetNDTTableDetail(dsExt, dest, destDate)
	if err != nil {
		log.Println(err)
	} else {
		log.Println(destDetail)
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

	// If fDryRun, then don't execute the destination write.
	if *fDryRun {
		return false, nil
	}

	dsExt.Dedup_Alpha(srcInfo.Name, "test_id", destTable)

	// If DeleteAfterDedup, then delete the source table.

	return true, nil
}

// TODO - move this to the README.
// First, the source table is checked for new template tables or
// partitions that have been stable for long enough that it is
// deemed safe to migrate them to the destination table.
//
// Tables should be processed in order of time since
// LastModificationTime.  This means that we should start by
// finding the age of all eligible tables.
//
// For each day or partition that is "ready", we then verify that
// the new content has at least 95% as many rows as the partition
// it will replace.  This limits the regression in cases where
// there is some problem with the new data.  This SHOULD also
// generate an alert.
//
// Once these prereqs are satisfied, we then execute a query that
// dedups the rows from the source, and writes to the destination
// partition.

func main() {
	flag.Parse()
	// Check that either project is set.
	if *fProject == "" {
		log.Println("Must specify project")
		flag.PrintDefaults()
		return
	}

	dsExt, err := bqext.NewDataset(*fProject, "etl")
	if err != nil {
		log.Fatal(err)
	}

	info, err := GetInfoMatching(&dsExt, "TestDedupSrc_19990101")
	log.Println(info)

	_, err = CheckAndDedup(&dsExt, info[0], "TestDedupDest")
	if err != nil {
		log.Println(err)
	}
	os.Exit(1)

	for i := range info {
		// TODO Query to check number of tar files processed.
		fmt.Printf("%v\n", info[i])

		// TODO Query to check number of rows?
		queryString := fmt.Sprintf("select count(test_id) as Tests, task_filename as Task from `%s` group by task_filename order by task_filename", info[i].Name)
		q := dsExt.ResultQuery(queryString, *fDryRun)
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
