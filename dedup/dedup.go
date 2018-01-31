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
	// ErrNilContext is returned when a request could not be completed with
	// a nil context.
	ErrNilContext = errors.New("Could not be completed without context")
	// ErrNotRegularTable is returned when a table is not a regular table (e.g. views)
	ErrNotRegularTable = errors.New("Not a regular table")
	// ErrNoDataset is returned when a operation requires a dataset, but none is provided/available.
	ErrNoDataset = errors.New("No dataset available")
	// ErrSrcOlderThanDest is returned if a source table is older than the destination partition.
	ErrSrcOlderThanDest = errors.New("Source older than destination partition")
	// ErrTooFewTasks is returned when the source table has fewer task files than the destination.
	ErrTooFewTasks = errors.New("Too few tasks")
	// ErrTooFewTests is returned when the source table has fewer tests than the destination.
	ErrTooFewTests = errors.New("Too few tests")
)

// Detail provides more detailed information about a partition or table.
type Detail struct {
	PartitionID   string // May be empty.  Used for slices of partitions.
	TaskFileCount int
	TestCount     int
}

// AnnotatedTable binds a bigquery.Table with associated additional info.
type AnnotatedTable struct {
	bigquery.Table
	dataset *bqext.Dataset // A dataset that can query the table.  May be nil.
	meta    *bigquery.TableMetadata
	detail  *Detail
	err     error // first error (if any) when attempting to fetch annotation
}

// NewAnnotatedTable creates an AnnotatedTable
func NewAnnotatedTable(t bigquery.Table, ds *bqext.Dataset) *AnnotatedTable {
	return &AnnotatedTable{Table: t, dataset: ds}
}

// CachedMeta returns metadata if available.
// If ctx is non-nil, will attempt to fetch metadata if it is not already cached.
// Returns error if meta not available.
func (at *AnnotatedTable) CachedMeta(ctx context.Context) (*bigquery.TableMetadata, error) {
	if at.meta != nil {
		return at.meta, nil
	}
	if at.err != nil {
		return nil, at.err
	}
	if ctx == nil {
		return nil, ErrNilContext
	}
	at.meta, at.err = at.Metadata(ctx)
	return at.meta, at.err
}

func (at *AnnotatedTable) LastModifiedTime(ctx context.Context) time.Time {
	meta, err := at.CachedMeta(ctx)
	if err != nil {
		return time.Now() // Hack - not sure what to do instead.
	}
	return meta.LastModifiedTime
}

// CachedDetail returns the cached detail, or fetches it if possible.
func (at *AnnotatedTable) CachedDetail(ctx context.Context) (*Detail, error) {
	if at.detail != nil {
		return at.detail, nil
	}
	if at.err != nil {
		return nil, at.err
	}
	if at.dataset == nil {
		return nil, ErrNoDataset
	}
	if ctx == nil {
		return nil, ErrNilContext
	}
	// TODO - use context
	// TODO - GetTableDetail should return pointer
	var detail Detail
	detail, at.err = GetTableDetail(at.dataset, &at.Table)
	if at.err == nil {
		at.detail = &detail
	}
	return at.detail, at.err
}

// CheckIsRegular returns nil if table exists and is a regular table.
// Otherwise returns an error, indicating
// If metadata is not available, returns false and associated error.
func (at *AnnotatedTable) CheckIsRegular(ctx context.Context) error {
	meta, err := at.CachedMeta(ctx)
	if err != nil {
		return err
	}
	if meta.Type != bigquery.RegularTable {
		return ErrNotRegularTable
	}
	return nil
}

/*
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

// */

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
func GetTableInfoMatching(ctx context.Context, dsExt *bqext.Dataset, filter string) ([]AnnotatedTable, error) {
	alt := make([]AnnotatedTable, 0) // TODO should this be (..., 31, 0)
	//	result := make([]TableInfo, 0)
	ti := dsExt.Tables(ctx)
	for t, err := ti.Next(); err == nil; t, err = ti.Next() {
		// TODO should this be starts with?  Or a regex?
		if strings.Contains(t.TableID, filter) {
			// TODO - make this run in parallel
			at := AnnotatedTable{Table: *t, dataset: dsExt}
			_, err := at.CachedMeta(ctx)
			if err == ErrNotRegularTable {
				continue
			}
			if err != nil {
				return nil, err
			}
			//	result = append(result, ts)
			alt = append(alt, at)
		}
	}
	sort.Slice(alt[:], func(i, j int) bool {
		return alt[i].LastModifiedTime(ctx).Before(alt[j].LastModifiedTime(ctx))
	})
	return alt, nil
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
	if float32(srcDetail.TaskFileCount) < 0.99*float32(destDetail.TaskFileCount) {
		return ErrTooFewTasks
	} else if srcDetail.TaskFileCount < destDetail.TaskFileCount {
		log.Printf("Warning - fewer task files: %d < %d\n", srcDetail.TaskFileCount, destDetail.TaskFileCount)
	}

	// Check that new table contains at least 95% as many tests as
	// old table.  This may be fewer if the destination table still has dups.
	if float32(srcDetail.TestCount) < 0.95*float32(destDetail.TestCount) {
		return ErrTooFewTests
	} else if srcDetail.TestCount < destDetail.TestCount {
		log.Printf("Warning - fewer tests: %d < %d\n", srcDetail.TestCount, destDetail.TestCount)
	}
	return nil
}

// Fetches info about destination table, and checks that it's mtime is older than the source table's.
func checkDestOlder(ctx context.Context, dsExt *bqext.Dataset, src, dest *AnnotatedTable) error {
	// TODO - save and use existing info.
	destPartitionInfo, err := GetPartitionInfo(ctx, dsExt, &dest.Table)
	if err != nil {
		return err
	}

	// If the source table is older than the destination table, then
	// don't overwrite it.
	srcMeta, err := src.CachedMeta(ctx)
	if err != nil {
		return err
	}
	if srcMeta.LastModifiedTime.Before(destPartitionInfo.LastModified) {
		// TODO should perhaps delete the source table?
		return ErrSrcOlderThanDest
	}
	return nil
}

// WaitForJob waits for job to complete.  Uses fibonacci backoff until the backoff
// >= maxBackoff, at which point it continues using same backoff.
// TODO - move this to go/bqext, since it is bigquery specific and general purpose.
func WaitForJob(ctx context.Context, job *bigquery.Job, maxBackoff time.Duration) error {
	backoff := 10 * time.Millisecond
	previous := backoff
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
		time.Sleep(backoff)
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

	err = WaitForJob(context.Background(), job, 10*time.Second)
	log.Println("Done")
	return err
}

// Options provides processing options for Dedup_Alpha
type Options struct {
	MinSrcAge     time.Duration // Minimum time since last source modification
	IgnoreDestAge bool          // Don't check age of destination partition.
	DryRun        bool          // Do all checks, but don't dedup or copy.
	CopyOnly      bool          // Skip the dedup step and copy from intermediate to destination
}

// Job keeps track of a number of resources involved in dedupping.
type Job struct {
	// Initialization objects
	*bqext.Dataset                 // Dataset extension associated with source
	srcTable       *AnnotatedTable // Source table to be deduplicated
	destTable      *AnnotatedTable // destination table that result will be committed to.

	// Additional information generated during processing.
	dedupTable *bigquery.Table // dedup table that will receive deduped result before commit
}

// NewJob creates a DedupJob struct
func NewJob(ds *bqext.Dataset, srcTable, destTable *AnnotatedTable) *Job {
	return &Job{Dataset: ds, srcTable: srcTable, destTable: destTable}
}

// This should be called after doing various sanity checks.
func (job *Job) dedupAndCopy(ctx context.Context, options Options) error {
	if !options.CopyOnly {
		// Do the deduplication to intermediate "dedup" table with same root name.
		// TODO - are we checking for source newer than intermediate destination?  Should we?
		_, err := job.Dedup_Alpha(job.srcTable.TableID, "test_id", job.dedupTable)
		if err != nil {
			log.Println(err)
			return err
		}
	}

	// Now compare number of rows and tasks in dedup table to destination table.
	dedupDetail, err := GetTableDetail(job.Dataset, job.dedupTable)
	if err != nil {
		return err
	}
	destDetail, err := job.destTable.CachedDetail(ctx)
	err = checkDetails(dedupDetail, *destDetail)
	if err != nil {
		return err
	}

	destParts, err := getTableParts(job.destTable.TableID)
	if err != nil {
		return err
	}

	err = SanityCheckAndCopy(ctx, job.Dataset.BqClient, job.dedupTable, job.destTable.DatasetID, destParts.prefix)
	if err != nil {
		return err
	}

	return nil
}

// CheckAndDedup checks various criteria, and if they all pass, dedups the table.
// The destination table must exist, must be in a separate dataset, and may or may not
// have a corresponding partition.  If the partition already exists, then additional
// sanity checks occur.
//
// First checks initial criteria and deduplicates into the appropriate partitioned
// table corresponding to source.
// Then if all criteria pass, copies into the destination table.
//
// Returns nil if successful, error if criteria fail, or dedup fails.
//
// Criteria:
//   1. Source table modification time is at least as old as options.MinSrcAge
//   2. Source table mod time is later than destination table mod time, unless options.IgnoreDestAge.
//   3. If destination partition exists then
//       3a. Source reflects at least as many task files as destination (if dest)
//       3b. Source has at least 98% as many tests as destination (including dups)
//       3c. After deduplication, intermediate table has at least 98% as many tests as destination.
//
// srcExt         - bqext.Dataset for operations.
// srcInfo       - TableInfo for the source
// destTable     - destination Table (possibly in another dataset)
// options       - dedup options, MinSrcAge, IgnoreDestAge, DryRun, CopyOnly.
//
// TODO(gfr) Should we check that intermediate table is NOT a production table?
func (job *Job) CheckAndDedup(ctx context.Context, options Options) error {

	srcParts, err := getTableParts(job.srcTable.TableID)
	if err != nil {
		return err
	}

	// Check if the last update was at least minSrcAge in the past.
	if time.Now().Sub(job.srcTable.LastModifiedTime(ctx)) < options.MinSrcAge {
		return errors.New("Source is too recent")
	}

	if job.destTable.DatasetID == job.Dataset.DatasetID {
		return errors.New("Source and Destination should be in different datasets: ")
	}

	destParts, err := getTableParts(job.destTable.TableID)
	if err != nil {
		return err
	}

	if destParts.yyyymmdd != srcParts.yyyymmdd {
		return errors.New("Source and Destination should have same partition/template date: ")
	}

	// We do the deduplication into the corresponding partition, derived from the source table template.
	// We will eventually use dedupTable to create a copier, so it must use a valid bigquery.Client.
	job.dedupTable, err = getTable(job.BqClient, job.ProjectID, job.DatasetID, srcParts.prefix, srcParts.yyyymmdd)
	if err != nil {
		log.Println(err)
		return err
	}

	//	job.srcTable = NewAnnotatedTable(job.srcT, job.ds)
	// Just check that table exists.
	_, err = job.srcTable.Metadata(ctx)
	if err != nil {
		log.Println(err)
		return err
	}

	if job.destTable.DatasetID == job.dedupTable.DatasetID {
		return errors.New("Dedup and Destination should be in different datasets: " + job.dedupTable.FullyQualifiedName())
	}

	if !options.IgnoreDestAge {
		// TODO - this fails if the destination partition does not exist.
		err = checkDestOlder(ctx, job.Dataset, job.srcTable, job.destTable)
		if err != nil {
			log.Println(err)
			return err
		}
	}

	srcDetail, err := job.srcTable.CachedDetail(ctx)
	if err != nil {
		return err
	}
	destDetail, err := job.destTable.CachedDetail(ctx)
	if err != nil {
		return err
	}

	// If dest partition exists, sanity check that we have reasonable numbers in source.
	err = checkDetails(*srcDetail, *destDetail)
	if err != nil {
		log.Println(err)
		return err
	}

	if options.DryRun {
		log.Println("Dedup dry run:", job.srcTable.TableID, "test_id", job.dedupTable)
		return nil
	}

	err = job.dedupAndCopy(ctx, options)

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

	return nil
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
	atList, err := GetTableInfoMatching(context.Background(), dsExt, srcPattern)
	if err != nil {
		return err
	}

	// Print a summary of tables to be processed.
	log.Println("Examining", len(atList), "source tables")
	for i := range atList {
		log.Println(atList[i].Table)
	}

	// Process each table, serially, to avoid problems with too many concurrent
	// bigquery queries.
	for i := range atList {
		srcTable := atList[i]
		// Skip any partition that has been updated in the past minAge.
		if time.Since(srcTable.LastModifiedTime(context.Background())) < options.MinSrcAge {
			continue
		}

		parts, err := getTableParts(srcTable.TableID)
		if err != nil {
			return err
		}

		destTable, _ := getTable(dsExt.BqClient, dsExt.ProjectID, destDataset, destBase, parts.yyyymmdd)
		job := NewJob(dsExt, &srcTable, NewAnnotatedTable(*destTable, dsExt))
		err = job.CheckAndDedup(context.Background(), options)
		if err != nil {
			log.Println(err, "dedupping", dsExt.DatasetID+"."+srcTable.TableID, "to", destDataset+"."+destBase)
			return err
		}
	}
	return nil
}
