package main

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/m-lab/etl/dedup"
	"gopkg.in/m-lab/go.v1/bqext"
)

var (
	// TODO - replace this with a service account?
	fProject          = flag.String("project", "", "BigQuery project.")
	fTemplatePrefix   = flag.String("template_prefix", "", "source dataset.table_prefix")
	fDelay            = flag.Float64("delay", 48, "delay (hours) from last update")
	fDestinationTable = flag.String("destination_table", "etl.dest", "destination dataset.table")
	fDeleteAfterCopy  = flag.Bool("delete", false, "Should delete table after copy")
	fDryRun           = flag.Bool("dry_run", false, "Print actions instead of executing")
)

func main() {
	flag.Parse()

	src := strings.Split(*fTemplatePrefix, ".")
	if len(src) != 2 {
		log.Println("template_prefix must have dataset.table_prefix")
		os.Exit(1)
	}
	dsExt, err := bqext.NewDataset(*fProject, src[0])
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	dest := strings.Split(*fDestinationTable, ".")
	if len(dest) != 2 {
		log.Println("template_prefix must have dataset.table_prefix")
		os.Exit(1)
	}
	// TODO fix delay param.
	err = dedup.ProcessTablesMatching(&dsExt, src[1], dest[0], dest[1], 14*24*time.Hour)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}
