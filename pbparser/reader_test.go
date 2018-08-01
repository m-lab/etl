package pbparser_test

import (
	"context"
	"log"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/pbparser"
	"github.com/m-lab/tcp-info/zstd"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// Notes on creating schema...
//  1. Used this code to print out json.
//  2. removed newlines
//  3. created empty table with bq mk
//  4. upload single row, with
//     bq load --autodetect --source_format=NEWLINE_DELIMITED_JSON mlab-testing:gfr.foobar foo.json
//  5. dumped schema with
//     bq show --format=prettyjson mlab-testing:gfr.foobar > schema.json
//  6. Update timestamp field to TIMESTAMP.
//  7. For new rows, change timestamp field to float, and divide by 1E9

func TestProtoParsing(t *testing.T) {
	// Cache info new 140  err 0 same 277 local 789 diff 3 total 1209
	// 1209 sockets 143 remotes 403 per iteration
	//source := "testdata/20180717Z144141.694U00148024L100.101.230.223:41506R192.30.253.116:443_00000.zst"
	source := "testdata/20180607Z153856.193U00000000L2620:0:1003:415:b33e:9d6a:81bf:87a1:36032R2607:f8b0:400d:c0d::81:5034_00000.zst"
	log.Println("Reading messages from", source)
	rdr := zstd.NewReader(source)

	protos, err := pbparser.ReadAll(rdr)

	if err != nil {
		t.Fatal(err)
	}

	if len(protos) != 17 {
		t.Error("Should be 17 messages", len(protos))
	}

	row, _, _ := pbparser.InfoWrapper{TCPDiagnosticsProto: protos[0]}.Save()
	log.Println(row)
}

func TestMakeTable(t *testing.T) {
	schema, err := pbparser.BuildSchema()
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "mlab-testing")
	dataset := client.Dataset("gfr")
	table := dataset.Table("tcpinfo")

	if err = table.Delete(ctx); err != nil {
		t.Error(err)
	}

	if err = table.Create(ctx, &bigquery.TableMetadata{Schema: schema, TimePartitioning: &bigquery.TimePartitioning{}}); err != nil {
		t.Error(err)
	}

}

func TestUploadData(t *testing.T) {
	//        "testdata/20180717Z144141.694U00148024L100.101.230.223:53464R35.226.80.19:443_00000.zst"
	source := "testdata/20180607Z153856.193U00000000L2620:0:1003:415:b33e:9d6a:81bf:87a1:36032R2607:f8b0:400d:c0d::81:5034_00000.zst"
	log.Println("Reading messages from", source)
	rdr := zstd.NewReader(source)
	startRead := time.Now()
	protos, err := pbparser.ReadAll(rdr)
	log.Println("Read", time.Now().Sub(startRead)/17)
	if err != nil {
		t.Fatal(err)
	}

	if len(protos) != 17 {
		t.Error("Should be 17 messages", len(protos))
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "mlab-testing")
	dataset := client.Dataset("gfr")
	table := dataset.Table("tcpinfo")

	for i := range protos {
		u := table.Uploader()
		start := time.Now()
		err = u.Put(ctx, pbparser.InfoWrapper{TCPDiagnosticsProto: protos[i], TaskFilename: source})
		log.Println(time.Now().Sub(start))
		if err != nil {
			e := err.(bigquery.PutMultiError)
			if e != nil {
				for i := range e {
					log.Println(e[i])
				}
			}
			t.Error(err)
		}
	}
}
