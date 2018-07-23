package pbparser_test

import (
	"log"
	"testing"

	"github.com/m-lab/etl/pbparser"
	"github.com/m-lab/tcp-info/zstd"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

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
	for i := range protos {
		log.Println(protos[i])
	}
}
