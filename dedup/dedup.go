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
	// ErrMismatchedPartitions is returned when partition dates should match but don't.
	ErrMismatchedPartitions = errors.New("Partition dates don't match")
)

// Detail provides more detailed information about a partition or table.
type Detail struct {
	PartitionID   string // May be empty.  Used for slices of partitions.
	TaskFileCount int
	TestCount     int
}

// AnnotatedTable binds a bigquery.Table with associated additional info.
// It keeps track of all the info we have so far, automatically fetches
// more data as needed, and thus avoids possibly fetching multiple times.
type AnnotatedTable struct {
	*bigquery.Table
	dataset *bqext.Dataset // A dataset that can query the table.  May be nil.
	meta    *bigquery.TableMetadata
	detail  *Detail
	pInfo   *bqext.PartitionInfo
	err     error // first error (if any) when attempting to fetch annotation
}

// NewAnnotatedTable creates an AnnotatedTable
func NewAnnotatedTable(t *bigquery.Table, ds *bqext.Dataset) *AnnotatedTable {
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

// LastModifiedTime returns the LMT field from the metadata.  If not
// available, returns time.Time{} which is the zero time.
// Caller should take care if missing metadata might be a problem.
func (at *AnnotatedTable) LastModifiedTime(ctx context.Context) time.Time {
	meta, err := at.CachedMeta(ctx)
	if err != nil {
		return time.Time{} // Default time.  Caller might need to check for this.
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
	// TODO - maybe just embed code here.
	at.detail, at.err = GetTableDetail(at.dataset, at.Table)
	return at.detail, at.err
}

// CachedPartitionInfo returns the cached PInfo, possibly fetching it if possible.
func (at *AnnotatedTable) CachedPartitionInfo(ctx context.Context) (*bqext.PartitionInfo, error) {
	if at.pInfo != nil {
		return at.pInfo, nil
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
	// TODO - maybe just embed code here.
	at.pInfo, at.err = at.GetPartitionInfo(ctx)
	return at.pInfo, at.err
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

// GetTableDetail fetches more detailed info about a partition or table.
// Expects table to have test_id, and task_filename fields.
func GetTableDetail(dsExt *bqext.Dataset, table *bigquery.Table) (*Detail, error) {
	// If table is a partition, then we have to separate out the partition part for the query.
	parts := strings.Split(table.TableID, "$")
	dataset := table.DatasetID
	tableName := parts[0]
	where := ""
	if len(parts) > 1 {
		if len(parts[1]) == 8 {
			where = "where _PARTITIONTIME = PARSE_TIMESTAMP(\"%Y%m%d\",\"" + parts[1] + "\")"
		} else {
			return nil, errors.New("Invalid partition string: " + parts[1])
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
	return &detail, err
}

// GetTablesMatching finds all tables matching table filter
// and collects the basic stats about each of them.
// It performs many network operations, possibly two per table.
// Returns slice ordered by decreasing age.
func GetTablesMatching(ctx context.Context, dsExt *bqext.Dataset, filter string) ([]AnnotatedTable, error) {
	alt := make([]AnnotatedTable, 0)
	ti := dsExt.Tables(ctx)
	for t, err := ti.Next(); err == nil; t, err = ti.Next() {
		// TODO should this be starts with?  Or a regex?
		if strings.Contains(t.TableID, filter) {
			// TODO - make this run in parallel
			at := AnnotatedTable{Table: t, dataset: dsExt}
			_, err := at.CachedMeta(ctx)
			if err == ErrNotRegularTable {
				continue
			}
			if err != nil {
				return nil, err
			}
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
// Unlike bqext.GetPartitionInfo, this gets project and dataset from the
// table, which should include partition spec.
// at.dataset should have access to the table, but its project and dataset are not used.
// TODO - possibly migrate this to go/bqext.
func (at *AnnotatedTable) GetPartitionInfo(ctx context.Context) (*bqext.PartitionInfo, error) {
	tableName := at.Table.TableID
	parts, err := getTableParts(tableName)
	if err != nil || !parts.isPartitioned {
		return nil, errors.New("TableID missing partition: " + tableName)
	}
	// Assemble the FQ table name, without the partition suffix.
	fullTable := fmt.Sprintf("%s:%s.%s", at.ProjectID, at.DatasetID, parts.prefix)

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

	err = at.dataset.QueryAndParse(queryString, &pInfo)
	if err != nil {
		// If the partition doesn't exist, just return empty Info, no error.
		if err == iterator.Done {
			return &bqext.PartitionInfo{}, nil
		}
		return nil, err
	}
	return &pInfo, nil
}

// TODO - move these up with other methods after review.
func (at *AnnotatedTable) checkAlmostAsBig(ctx context.Context, other *AnnotatedTable) error {
	thisDetail, err := at.CachedDetail(ctx)
	if err != nil {
		return err
	}
	otherDetail, err := other.CachedDetail(ctx)
	if err != nil {
		return err
	}

	// Check that receiver table contains at least 99% as many tasks as
	// other table.
	if float32(thisDetail.TaskFileCount) < 0.99*float32(otherDetail.TaskFileCount) {
		return ErrTooFewTasks
	} else if thisDetail.TaskFileCount < otherDetail.TaskFileCount {
		log.Printf("Warning - fewer task files: %d < %d\n", thisDetail.TaskFileCount, otherDetail.TaskFileCount)
	}

	// Check that receiver table contains at least 95% as many tests as
	// other table.  This may be fewer if the destination table still has dups.
	if float32(thisDetail.TestCount) < 0.95*float32(otherDetail.TestCount) {
		return ErrTooFewTests
	} else if thisDetail.TestCount < otherDetail.TestCount {
		log.Printf("Warning - fewer tests: %d < %d\n", thisDetail.TestCount, otherDetail.TestCount)
	}
	return nil
}

// TODO - should we use the metadata, or the PI?  Are they the same?
func (at *AnnotatedTable) checkModifiedAfter(ctx context.Context, other *AnnotatedTable) error {
	// If the source table is older than the destination table, then
	// don't overwrite it.
	thisMeta, err := at.CachedMeta(ctx)
	if err != nil {
		return err
	}
	// Note that if other doesn't actually exist, its LastModifiedTime will be the time zero value,
	// so this will generally work as intended.
	if thisMeta.LastModifiedTime.Before(other.LastModifiedTime(ctx)) {
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
//  2. Destination partition matches source partition/suffix
// TODO(gfr) Ideally this should be done by a separate process with
// higher priviledge than the reprocessing and dedupping processes.
// TODO(gfr) Also support copying from a template instead of partition?
func SanityCheckAndCopy(ctx context.Context, client *bigquery.Client, srcTable, destTable *bigquery.Table) error {
	// Extract the
	srcParts, err := getTableParts(srcTable.TableID)
	if err != nil {
		return err
	}

	destParts, err := getTableParts(destTable.TableID)
	if err != nil {
		return err
	}
	if destParts.yyyymmdd != srcParts.yyyymmdd {
		return ErrMismatchedPartitions
	}

	copier := destTable.CopierFrom(srcTable)
	copier.WriteDisposition = bigquery.WriteTruncate
	log.Println("Copying...", srcTable.TableID)
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
	dedupTable *AnnotatedTable // dedup table that will receive deduped result before commit
}

// NewJob creates a Job struct
func NewJob(ds *bqext.Dataset, srcTable, destTable *AnnotatedTable) *Job {
	return &Job{Dataset: ds, srcTable: srcTable, destTable: destTable}
}

// This should be called after doing various sanity checks.
func (job *Job) dedupAndCopy(ctx context.Context, options Options) error {
	if !options.CopyOnly {
		// Do the deduplication to intermediate "dedup" table with same root name.
		// TODO - are we checking for source newer than intermediate destination?  Should we?
		_, err := job.Dedup_Alpha(job.srcTable.TableID, "test_id", job.dedupTable.Table)
		if err != nil {
			log.Println(err)
			return err
		}
	}

	// Now compare number of rows and tasks in dedup table to destination table.
	err := job.dedupTable.checkAlmostAsBig(ctx, job.destTable)
	if err != nil {
		return err
	}

	err = SanityCheckAndCopy(ctx, job.Dataset.BqClient, job.dedupTable.Table, job.destTable.Table)
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
//   3. Source and destination are in different datasets, and source dataset is NOT "base_tables"
//   4. If destination partition exists then
//       4a. Source reflects at least 99% as many task files as final destination
//       4b. Source has at least 95% as many tests as destination (including dups)
//       4c. After deduplication, intermediate table has at least 95% as many tests as destination.
//
// options - dedup options, MinSrcAge, IgnoreDestAge, DryRun, CopyOnly.
//
func (job *Job) CheckAndDedup(ctx context.Context, options Options) error {
	// Check that source and destination are both templated or partitioned.
	srcParts, err := getTableParts(job.srcTable.TableID)
	if err != nil {
		return err
	}
	destParts, err := getTableParts(job.destTable.TableID)
	if err != nil {
		return err
	}
	// Check that date suffixes match.
	if destParts.yyyymmdd != srcParts.yyyymmdd {
		return errors.New("Source and Destination should have same partition/template date: ")
	}

	// Check if the last update was at least minSrcAge in the past.
	if time.Since(job.srcTable.LastModifiedTime(ctx)) < options.MinSrcAge {
		return errors.New("Source is too recent")
	}

	if job.destTable.DatasetID == job.Dataset.DatasetID {
		return errors.New("Source and Destination should be in different datasets: ")
	}

	// For the dedup destination, we use the partitioned table with the same dataset/prefix
	// as the source table.
	// We do the deduplication into the partition with the same date.
	// dedupTable will later be used to create a copier, so it must use a valid bigquery.Client.
	tmpTable, err := getTable(job.BqClient, job.ProjectID, job.DatasetID, srcParts.prefix, srcParts.yyyymmdd)
	if err != nil {
		log.Println(err)
		return err
	}
	job.dedupTable = NewAnnotatedTable(tmpTable, job.Dataset)

	// Just check that table exists.
	_, err = job.srcTable.CachedMeta(ctx)
	if err != nil {
		log.Println(err)
		return err
	}

	// Check that temporary dedup target is neither final destination, nor "base_tables" dataset.
	if job.dedupTable.DatasetID == job.destTable.DatasetID || job.dedupTable.DatasetID == "base_tables" {
		return errors.New("Dedup and Destination should be in different datasets: " + job.dedupTable.FullyQualifiedName())
	}

	if !options.IgnoreDestAge {
		err = job.srcTable.checkModifiedAfter(ctx, job.destTable)
		if err != nil {
			log.Println(err)
			return err
		}
	}

	err = job.srcTable.checkAlmostAsBig(ctx, job.destTable)
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
func ProcessTablesMatching(srcDS *bqext.Dataset, srcPattern string, destDataset string, options Options) error {
	// This may not have full suffix, so we can't use getTableParts.
	srcParts := strings.Split(srcPattern, "_")
	if len(srcParts) != 2 {
		return errors.New("Invalid source pattern: " + srcPattern)
	}

	// These are sorted by LastModification, oldest first.
	atList, err := GetTablesMatching(context.Background(), srcDS, srcPattern)
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

		srcParts, err := getTableParts(srcTable.TableID)
		if err != nil {
			return err
		}

		// destination should be in a different dataset, but same project and same table name and
		// date suffix as source.
		destTable, _ := getTable(srcDS.BqClient, srcDS.ProjectID,
			destDataset, srcParts.prefix, srcParts.yyyymmdd)
		job := NewJob(srcDS, &srcTable, NewAnnotatedTable(destTable, srcDS))
		err = job.CheckAndDedup(context.Background(), options)
		if err != nil {
			log.Println(err, "dedupping", srcDS.DatasetID+"."+srcTable.TableID,
				"to", destTable.DatasetID+"."+destTable.TableID)
			return err
		}
	}
	return nil
}
