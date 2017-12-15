package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/m-lab/etl/dedup"
	"gopkg.in/m-lab/go.v1/bqext"
)

var (
	// TODO - replace this with a service account?
	fProject          = flag.String("project", "", "BigQuery project.")
	fTemplatePrefix   = flag.String("template_prefix", "", "table prefix")
	fDelay            = flag.Float64("delay", 48, "delay (hours) from last update")
	fDestinationTable = flag.String("destination_table", "etl.dest", "destination table")
	fDeleteAfterCopy  = flag.Bool("delete", false, "Should delete table after copy")
	fDryRun           = flag.Bool("dry_run", false, "Print actions instead of executing")
)

func main() {
	flag.Parse()
	log.Println(*fProject)
	dsExt, err := bqext.NewDataset(*fProject, "batch")

	if *fTemplatePrefix == "" {
		log.Println("template_prefix must be non-empty")
		os.Exit(1)
	}
	err = dedup.ProcessTablesMatching(&dsExt, *fTemplatePrefix, "public", "ndt", 14*24*time.Hour)
	if err != nil {
		log.Println(err)
	}

}
