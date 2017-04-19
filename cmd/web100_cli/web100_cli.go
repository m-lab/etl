// web100_cli provides a simple CLI interface to web100lib functions.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/web100lib"
)

var (
	filename = flag.String("filename", "", "Trace filename.")
	tcpKis   = flag.String("tcp-kis", "tcp-kis.txt", "tcp-kis.txt filename.")
)

func PrettyPrint(results map[string]bigquery.Value) {
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
	legacyNames, err := web100lib.ParseWeb100Definitions(k)
	if err != nil {
		panic(err)
	}

	// Open web100 snapshot log.
	w, err := web100lib.Open(*filename, legacyNames)
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
		results, err := w.Values(legacyNames)
		if err != nil {
			panic(err)
		}
		PrettyPrint(results)
	}
	if err != io.EOF {
		panic(err)
	}
	results, err := w.Values()
	if err != nil {
		panic(err)
	}
	PrettyPrint(results)

	// Get results.
	// results, err = w.SnapValues(legacyNames)
	// if err != nil {
	// 	panic(err)
	// }
	// PrettyPrint(results)
	// fmt.Printf("%#v\n", w)
}
