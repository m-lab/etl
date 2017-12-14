package main

import "flag"

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

func main() {}
