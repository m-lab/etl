package main

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/m-lab/etl/dedup"
	"github.com/m-lab/go/bqext"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

var (
	// TODO - replace this with a service account?
	fProject          = flag.String("project", "", "BigQuery project.")
	fTemplatePrefix   = flag.String("template_prefix", "", "source dataset.table_prefix")
	fDelay            = flag.Duration("delay", 7*24*time.Hour, "delay (hours) from last update")
	fDestinationTable = flag.String("destination_table", "", "destination dataset.table")
	fDeleteAfterCopy  = flag.Bool("delete", false, "Should delete table after copy")
	fIgnoreDestAge    = flag.Bool("ignore_dest_age", false, "Ignore destination age in sanity check")
	fDryRun           = flag.Bool("dry_run", false, "Print actions instead of executing")
	fSkipDedup        = flag.Bool("skip_dedup", false, "Assume dedup has been done, and just copy.")
)

func main() {
	flag.Parse()

	src := strings.Split(*fTemplatePrefix, ".")
	if len(src) != 2 {
		log.Fatal("template_prefix must have dataset.table_prefix")
	}
	srcTableParts := strings.Split(src[1], "_")
	if len(srcTableParts) != 2 {
		log.Fatal("pattern must have _", src[1])
	}

	dsExt, err := bqext.NewDataset(*fProject, src[0])
	if err != nil {
		log.Fatal(err)
	}

	dest := strings.Split(*fDestinationTable, ".")
	if len(dest) != 2 {
		log.Fatal("destination_table must have dataset.table_prefix")
	}

	// TODO fix delay param.

	if srcTableParts[0] != dest[1] {
		log.Fatal("Source and destination table bases should be same", srcTableParts, dest)
	}
	err = dedup.ProcessTablesMatching(&dsExt, src[1], dest[0],
		dedup.Options{MinSrcAge: *fDelay, IgnoreDestAge: *fIgnoreDestAge, DryRun: *fDryRun, CopyOnly: *fSkipDedup})
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}
