package pbparser_test

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/m-lab/tcp-info/zstd"

	tcpinfo "github.com/m-lab/tcp-info/nl-proto"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestReader(t *testing.T) {
	// Cache info new 140  err 0 same 277 local 789 diff 3 total 1209
	// 1209 sockets 143 remotes 403 per iteration
	//source := "testdata/20180717Z144141.694U00148024L100.101.230.223:41506R192.30.253.116:443_00000.zst"
	source := "testdata/20180607Z153856.193U00000000L2620:0:1003:415:b33e:9d6a:81bf:87a1:36032R2607:f8b0:400d:c0d::81:5034_00000.zst"
	log.Println("Reading messages from", source)
	rdr := zstd.NewReader(source)
	byteRdr := bufio.NewReader(rdr)

	size, err := binary.ReadUvarint(byteRdr)

	if err != nil {
		t.Fatal(err)
	}
	log.Println(size)
	buf := make([]byte, size)
	n, err := io.ReadFull(byteRdr, buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buf) {
		t.Fatalf("wrong size %d != %d", n, len(buf))
	}

	pb := tcpinfo.TCPDiagnosticsProto{}
	proto.Unmarshal(buf, &pb)
	log.Println(pb)
}
