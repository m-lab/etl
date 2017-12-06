// Package main defines a command line tool for deduplicating
// tempalte tables and copying into a destination partitions.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"time"

	"github.com/m-lab/etl/bqutil"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
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

// PartitionInfo provides basic information about a partition.
type PartitionInfo struct {
	PartitionID  string    `qfield:"partition_id"`
	CreationTime time.Time `qfield:"created"`
	LastModified time.Time `qfield:"last_modified"`
}

// GetPartitionInfo gets basic information about a partition.
func GetPartitionInfo(util *bqutil.TableUtil, table, partition string) (PartitionInfo, error) {
	// This uses legacy, because PARTITION_SUMMARY is not supported in standard.
	queryString := fmt.Sprintf(
		`#legacySQL
		SELECT
		  partition_id,
		  msec_to_timestamp(creation_time) AS created,
		  msec_to_timestamp(last_modified_time) AS last_modified
		FROM
		  [%s$__PARTITIONS_SUMMARY__]
		where partition_id = "%s" `, table, partition)
	x, err := util.QueryAndParse(queryString, PartitionInfo{})
	return x.(PartitionInfo), err
}

// PartitionDetail provides more detailed information about a partition.
type PartitionDetail struct {
	PartitionID   string
	TaskFileCount int
	TestCount     int
	MaxParseTime  time.Time
}

// GetNDTPartitionDetail fetches more detailed info about a partition or table.
// Expects table to have test_id, task_filename, and parse_time fields.
// `partition` should be in YYYY-MM-DD format.
func GetNDTPartitionDetail(util *bqutil.TableUtil, table, partition string) (PartitionDetail, error) {
	queryString := fmt.Sprintf(`
	   #standardSQL
	   select sum(tests) as tests, count(Task) as tasks, max(last_parse) as last_parse
	   from (
	   select count(test_id) as Tests, max(parse_time) as last_parse, task_filename as Task
	    from `+"`"+"%s"+"`"+`
	   where _partitiontime = timestamp("%s 00:00:00") group by task_filename)`,
		table, partition)

	x, err := util.QueryAndParse(queryString, PartitionDetail{})
	return x.(PartitionDetail), err
}

// CheckAndDedup checks various criteria, and if they all pass,
// dedups the table.  Returns true if criteria pass, false if they fail.
// Returns nil error on success, or non-nil error if there was an error
// at any point.
// Uses flags to determine most of the parameters.
func CheckAndDedup(util *bqutil.TableUtil, info bqutil.TableInfo) (bool, error) {
	// Check if the last update was at least fDelay in the past.
	if time.Now().Sub(info.LastModifiedTime).Hours() < *fDelay {
		return false, nil
	}

	t := util.Dataset.Table(info.Name)
	ctx := context.Background()

	_, err := t.Metadata(ctx)
	if err != nil {
		return false, err
	}

	// Creation time of new table should be newer than last update
	// of old table??
	// TODO replace table name with destination table name.
	re := regexp.MustCompile(".*_(.*)")
	suffix := re.FindString(info.Name)
	partInfo, err := GetPartitionInfo(util, "ndt", suffix)

	// TODO fix
	log.Println(partInfo)

	// Get info on new table tasks and rows.

	// Get info on old table tasks and rows (and age).

	// Check if all task files in the old table are also present
	// in the new table.

	// Check that new table contains at least 95% as many rows as
	// old table.

	// If fDryRun, then don't execute the destination write.

	// 	util.Dedup("ndt_20170601", true, "measurement-lab", "batch", "ndt$20170601")

	// If DeleteAfterDedup, then delete the source table.

	return false, nil
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

	//setup(bqutil.LoggingClient())
	util, err := bqutil.NewTableUtil(*fProject, "etl", nil)
	if err != nil {
		log.Fatal(err)
	}
	util.GetTableStats("TestDedupSrc")
	info := util.GetInfoMatching("etl", "TestDedupSrc_19990101")

	if !*fDryRun {
		util.Dedup("TestDedupSrc_19990101", true, "mlab-testing", "etl", "TestDedupDest$19990101")
		//util.DedupInPlace("ndt_20170601")
	}
	os.Exit(1)

	for i := range info {
		// TODO Query to check number of tar files processed.
		fmt.Printf("%v\n", info[i])

		// TODO Query to check number of rows?
		queryString := fmt.Sprintf("select count(test_id) as Tests, task_filename as Task from `%s` group by task_filename order by task_filename", info[i].Name)
		q := util.ResultQuery(queryString, *fDryRun)
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
