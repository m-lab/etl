package pbparser

import (
	"log"
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/golang/protobuf/jsonpb"
	"github.com/m-lab/etl/fake"
	tcp "github.com/m-lab/tcp-info/nl-proto"
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

type TCPDiagnosticsProto struct {
	InetDiagMsg *tcp.InetDiagMsgProto `protobuf:"bytes,1,opt,name=inet_diag_msg,json=inetDiagMsg,proto3" json:"inet_diag_msg,omitempty"`
	// From INET_DIAG_PROTOCOL message.
	DiagProtocol tcp.Protocol `protobuf:"varint,2,opt,name=diag_protocol,json=diagProtocol,proto3,enum=Protocol" json:"diag_protocol,omitempty"`
	// From INET_DIAG_CONG message.
	CongestionAlgorithm string `protobuf:"bytes,3,opt,name=congestion_algorithm,json=congestionAlgorithm,proto3" json:"congestion_algorithm,omitempty"`
	// The following three are mutually exclusive, as they provide
	// data from different congestion control strategies.
	//
	// Types that are valid to be assigned to CcInfo:
	//	*TCPDiagnosticsProto_Vegas
	//	*TCPDiagnosticsProto_Dctcp
	//	*TCPDiagnosticsProto_BbrInfo
	//CcInfo isTCPDiagnosticsProto_CcInfo `protobuf_oneof:"cc_info"`
	// Data obtained from INET_DIAG_SKMEMINFO.
	SocketMem *tcp.SocketMemInfoProto `protobuf:"bytes,7,opt,name=socket_mem,json=socketMem,proto3" json:"socket_mem,omitempty"`
	// Data obtained from INET_DIAG_MEMINFO.
	MemInfo *tcp.MemInfoProto `protobuf:"bytes,8,opt,name=mem_info,json=memInfo,proto3" json:"mem_info,omitempty"`
	// Data obtained from struct tcp_info.
	TcpInfo *tcp.TCPInfoProto `protobuf:"bytes,9,opt,name=tcp_info,json=tcpInfo,proto3" json:"tcp_info,omitempty"`
	// If there is shutdown info, this is the mask value.
	// Check has_shutdown_mask to determine whether present.
	//
	// Types that are valid to be assigned to Shutdown:
	//	*TCPDiagnosticsProto_ShutdownMask
	// Shutdown isTCPDiagnosticsProto_Shutdown `protobuf_oneof:"shutdown"`
	ShutdownMask uint32
	// Timestamp of batch of messages containing this message.
	Timestamp int64 `protobuf:"varint,11,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
}

func removeXXX(schema bigquery.Schema) bigquery.Schema {
	result := make([]*bigquery.FieldSchema, 0, len(schema))
	for i := range schema {
		log.Println(i, schema[i].Name)
		if !strings.HasPrefix(schema[i].Name, "XXX_") {
			if schema[i].Type == bigquery.RecordFieldType {
				schema[i].Schema = removeXXX(schema[i].Schema)
			}
			result = append(result, schema[i])
		}
	}
	return result
}

func GetSchema(t *testing.T) bigquery.Schema {
	schema, err := bigquery.InferSchema(TCPDiagnosticsProto{})
	if err != nil {
		t.Fatal(err)
	}

	return removeXXX(schema)
}

func TestProtoParsing(t *testing.T) {
	// Cache info new 140  err 0 same 277 local 789 diff 3 total 1209
	// 1209 sockets 143 remotes 403 per iteration
	//source := "testdata/20180717Z144141.694U00148024L100.101.230.223:41506R192.30.253.116:443_00000.zst"
	source := "testdata/20180607Z153856.193U00000000L2620:0:1003:415:b33e:9d6a:81bf:87a1:36032R2607:f8b0:400d:c0d::81:5034_00000.zst"
	log.Println("Reading messages from", source)
	rdr := zstd.NewReader(source)

	protos, err := ReadAll(rdr)

	if err != nil {
		t.Fatal(err)
	}

	if len(protos) != 17 {
		t.Error("Should be 17 messages", len(protos))
	}

	// This is a bit of a hack to get a bigquery compatible schema.
	schema := GetSchema(t)
	// TODO just use a StructSaver directly?
	pMap, err := fake.StructToMap(reflect.ValueOf(protos[0]), schema)
	if err != nil {
		t.Fatal(err)
	}
	log.Println(pMap)

	pMap, err = fake.StructToMap(reflect.ValueOf(protos[1]), schema)
	if err != nil {
		t.Fatal(err)
	}
	log.Println(pMap)

	log.Fatal("foo")
	marshaler := jsonpb.Marshaler{EnumsAsInts: true, Indent: "  ", OrigName: true}
	for i := range protos {
		str, err := marshaler.MarshalToString(&protos[i])
		if err != nil {
			t.Fatal(err)
		}
		log.Println(string(str))
		log.Println(protos[i])
	}
}
