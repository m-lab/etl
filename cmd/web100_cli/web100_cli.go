// web100_cli provides a simple CLI interface to web100 functions.
package main

// example:
// go build cmd/web100_cli/web100_cli.go
// ./web100_cli -filename parser/testdata/20170509T13\:45\:13.590210000Z_eb.measurementlab.net\:44160.s2c_snaplog
import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/etl/web100"
)

var (
	filename = flag.String("filename", "", "Trace filename.")
	tcpKis   = flag.String("tcp-kis", "tcp-kis.txt", "tcp-kis.txt filename.")
)

func prettyPrint(results map[string]bigquery.Value) {
	b, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Print(string(b))
}

func main() {
	flag.Parse()
	fmt.Println(*filename)

	content, err := ioutil.ReadFile(*filename)
	if err != nil {
		panic(err)
	}

	snaplog, err := web100.NewSnapLog(content)
	if err != nil {
		panic(err)
	}

	err = snaplog.ValidateSnapshots()
	if err != nil {
		panic(err)
	}

	last := snaplog.SnapCount() - 1
	snap, err := snaplog.Snapshot(last)
	if err != nil {
		panic(err)
	}

	snapValues := schema.EmptySnap()
	snap.SnapshotValues(snapValues)

	nestedConnSpec := make(schema.Web100ValueMap, 6)
	snaplog.ConnectionSpecValues(nestedConnSpec)

	results := schema.NewWeb100MinimalRecord(
		snaplog.Version, int64(snaplog.LogTime),
		(map[string]bigquery.Value)(nestedConnSpec),
		(map[string]bigquery.Value)(snapValues))

	prettyPrint(results)
}
