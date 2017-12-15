package main

import (
	"flag"
	"log"
	"time"

	"github.com/m-lab/etl/dedup"
	"gopkg.in/m-lab/go.v1/bqext"
)

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

func main() {

	dsExt, err := bqext.NewDataset(*fProject, "etl")

	err = dedup.ProcessTablesMatching(&dsExt, "T", "public", "ndt", 14*24*time.Hour)
	if err != nil {
		log.Println(err)
	}

}
