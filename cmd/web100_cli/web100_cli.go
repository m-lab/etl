// web100_cli provides a simple CLI interface to web100 functions.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

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

	// Parse tcp-kis.txt variable definitions.
	k, err := os.Open(*tcpKis)
	if err != nil {
		panic(err)
	}
	legacyNames, err := web100.ParseWeb100Definitions(k)
	if err != nil {
		panic(err)
	}

	// Open web100 snapshot log.
	w, err := web100.Open(*filename, legacyNames)
	if err != nil {
		panic(err)
	}
	defer w.Close()

	// Find all last web100 snapshot.
	for {
		err = w.Next()
		if err != nil {
			break
		}
	}
	if err != io.EOF {
		panic(err)
	}

	snapValues := schema.Web100ValueMap{}
	err = w.SnapshotValues(snapValues)
	if err != nil {
		panic(err)
	}
	connSpec := schema.Web100ValueMap{}
	w.ConnectionSpec(connSpec)
	results := schema.NewWeb100FullRecord(
		w.LogVersion(), w.LogTime(),
		(map[string]bigquery.Value)(connSpec),
		(map[string]bigquery.Value)(snapValues))

	prettyPrint(results)
}
