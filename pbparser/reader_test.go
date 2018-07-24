package pbparser_test

import (
	"log"
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/golang/protobuf/jsonpb"
	"github.com/m-lab/etl/fake"
	"github.com/m-lab/etl/pbparser"
	"github.com/m-lab/tcp-info/zstd"

	tcp "github.com/m-lab/tcp-info/nl-proto"
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
type EndPoint struct {
	Port uint32 `protobuf:"varint,1,opt,name=port,proto3" json:"port,omitempty"`
	Ip   []byte `protobuf:"bytes,2,opt,name=ip,proto3" json:"ip,omitempty"`
}

type InetSocketIDProto struct {
	Source      *tcp.EndPoint `protobuf:"bytes,1,opt,name=source,proto3" json:"source,omitempty"`
	Destination *tcp.EndPoint `protobuf:"bytes,2,opt,name=destination,proto3" json:"destination,omitempty"`
	Interface   uint32        `protobuf:"varint,5,opt,name=interface,proto3" json:"interface,omitempty"`
	// For some reason, reflection isn't working on uint64
	Cookie int64 `protobuf:"fixed64,6,opt,name=cookie,proto3" json:"cookie,omitempty"`
}

type InetDiagMsgProto struct {
	// These are 8 bit unsigned.
	//Family InetDiagMsgProto_AddressFamily `protobuf:"varint,1,opt,name=family,proto3,enum=InetDiagMsgProto_AddressFamily" json:"family,omitempty"`
	// Assuming for now this is the same as the TCPF_... states in struct tcp_info.
	// So use the same enumeration.
	// TODO - is this the same TCPF state?
	//	State   TCPState           `protobuf:"varint,2,opt,name=state,proto3,enum=TCPState" json:"state,omitempty"`
	Timer   uint32             `protobuf:"varint,3,opt,name=timer,proto3" json:"timer,omitempty"`
	Retrans uint32             `protobuf:"varint,4,opt,name=retrans,proto3" json:"retrans,omitempty"`
	SockId  *InetSocketIDProto `protobuf:"bytes,5,opt,name=sock_id,json=sockId,proto3" json:"sock_id,omitempty"`
	Expires uint32             `protobuf:"varint,6,opt,name=expires,proto3" json:"expires,omitempty"`
	Rqueue  uint32             `protobuf:"varint,7,opt,name=rqueue,proto3" json:"rqueue,omitempty"`
	Wqueue  uint32             `protobuf:"varint,8,opt,name=wqueue,proto3" json:"wqueue,omitempty"`
	Uid     uint32             `protobuf:"varint,9,opt,name=uid,proto3" json:"uid,omitempty"`
	Inode   uint32             `protobuf:"varint,10,opt,name=inode,proto3" json:"inode,omitempty"`
}
type TCPInfoProto struct {
	State tcp.TCPState `protobuf:"varint,1,opt,name=state,proto3,enum=TCPState" json:"state,omitempty"`
	// bitwise OR of CAState enums.
	CaState     uint32 `protobuf:"varint,2,opt,name=ca_state,json=caState,proto3" json:"ca_state,omitempty"`
	Retransmits uint32 `protobuf:"varint,3,opt,name=retransmits,proto3" json:"retransmits,omitempty"`
	Probes      uint32 `protobuf:"varint,4,opt,name=probes,proto3" json:"probes,omitempty"`
	Backoff     uint32 `protobuf:"varint,5,opt,name=backoff,proto3" json:"backoff,omitempty"`
	Options     uint32 `protobuf:"varint,6,opt,name=options,proto3" json:"options,omitempty"`
	// Here are the 6 OPTs broken out as bools.
	TsOpt   bool `protobuf:"varint,601,opt,name=ts_opt,json=tsOpt,proto3" json:"ts_opt,omitempty"`
	SackOpt bool `protobuf:"varint,602,opt,name=sack_opt,json=sackOpt,proto3" json:"sack_opt,omitempty"`
	// wscale_opt determines whether snd_wscale and rcv_wscale are populated.
	// So this is actually redundant with has_snd_wscale and has_rcv_wscale.
	WscaleOpt   bool `protobuf:"varint,603,opt,name=wscale_opt,json=wscaleOpt,proto3" json:"wscale_opt,omitempty"`
	EcnOpt      bool `protobuf:"varint,604,opt,name=ecn_opt,json=ecnOpt,proto3" json:"ecn_opt,omitempty"`
	EcnseenOpt  bool `protobuf:"varint,605,opt,name=ecnseen_opt,json=ecnseenOpt,proto3" json:"ecnseen_opt,omitempty"`
	FastopenOpt bool `protobuf:"varint,606,opt,name=fastopen_opt,json=fastopenOpt,proto3" json:"fastopen_opt,omitempty"`
	// These are 4 bit fields.
	SndWscale uint32 `protobuf:"varint,7,opt,name=snd_wscale,json=sndWscale,proto3" json:"snd_wscale,omitempty"`
	RcvWscale uint32 `protobuf:"varint,8,opt,name=rcv_wscale,json=rcvWscale,proto3" json:"rcv_wscale,omitempty"`
	// This field was recently added as an eighth u8 immediately following
	// tcpi_xxx_wscale bit fields, so inserting it here.
	DeliveryRateAppLimited bool   `protobuf:"varint,801,opt,name=delivery_rate_app_limited,json=deliveryRateAppLimited,proto3" json:"delivery_rate_app_limited,omitempty"`
	Rto                    uint32 `protobuf:"varint,9,opt,name=rto,proto3" json:"rto,omitempty"`
	Ato                    uint32 `protobuf:"varint,10,opt,name=ato,proto3" json:"ato,omitempty"`
	SndMss                 uint32 `protobuf:"varint,11,opt,name=snd_mss,json=sndMss,proto3" json:"snd_mss,omitempty"`
	RcvMss                 uint32 `protobuf:"varint,12,opt,name=rcv_mss,json=rcvMss,proto3" json:"rcv_mss,omitempty"`
	Unacked                uint32 `protobuf:"varint,13,opt,name=unacked,proto3" json:"unacked,omitempty"`
	Sacked                 uint32 `protobuf:"varint,14,opt,name=sacked,proto3" json:"sacked,omitempty"`
	Lost                   uint32 `protobuf:"varint,15,opt,name=lost,proto3" json:"lost,omitempty"`
	Retrans                uint32 `protobuf:"varint,16,opt,name=retrans,proto3" json:"retrans,omitempty"`
	Fackets                uint32 `protobuf:"varint,17,opt,name=fackets,proto3" json:"fackets,omitempty"`
	// Times.
	LastDataSent uint32 `protobuf:"varint,18,opt,name=last_data_sent,json=lastDataSent,proto3" json:"last_data_sent,omitempty"`
	LastAckSent  uint32 `protobuf:"varint,19,opt,name=last_ack_sent,json=lastAckSent,proto3" json:"last_ack_sent,omitempty"`
	LastDataRecv uint32 `protobuf:"varint,20,opt,name=last_data_recv,json=lastDataRecv,proto3" json:"last_data_recv,omitempty"`
	LastAckRecv  uint32 `protobuf:"varint,21,opt,name=last_ack_recv,json=lastAckRecv,proto3" json:"last_ack_recv,omitempty"`
	// Metrics.
	Pmtu         uint32 `protobuf:"varint,22,opt,name=pmtu,proto3" json:"pmtu,omitempty"`
	RcvSsthresh  uint32 `protobuf:"varint,23,opt,name=rcv_ssthresh,json=rcvSsthresh,proto3" json:"rcv_ssthresh,omitempty"`
	Rtt          uint32 `protobuf:"varint,24,opt,name=rtt,proto3" json:"rtt,omitempty"`
	Rttvar       uint32 `protobuf:"varint,25,opt,name=rttvar,proto3" json:"rttvar,omitempty"`
	SndSsthresh  uint32 `protobuf:"varint,26,opt,name=snd_ssthresh,json=sndSsthresh,proto3" json:"snd_ssthresh,omitempty"`
	SndCwnd      uint32 `protobuf:"varint,27,opt,name=snd_cwnd,json=sndCwnd,proto3" json:"snd_cwnd,omitempty"`
	Advmss       uint32 `protobuf:"varint,28,opt,name=advmss,proto3" json:"advmss,omitempty"`
	Reordering   uint32 `protobuf:"varint,29,opt,name=reordering,proto3" json:"reordering,omitempty"`
	RcvRtt       uint32 `protobuf:"varint,30,opt,name=rcv_rtt,json=rcvRtt,proto3" json:"rcv_rtt,omitempty"`
	RcvSpace     uint32 `protobuf:"varint,31,opt,name=rcv_space,json=rcvSpace,proto3" json:"rcv_space,omitempty"`
	TotalRetrans uint32 `protobuf:"varint,32,opt,name=total_retrans,json=totalRetrans,proto3" json:"total_retrans,omitempty"`
	// In tcp.h, these four are 64 bit unsigned.  However, the pacing rates
	// are often max-1.  Since protobufs use varints, we make these signed for
	// compact encoding.
	PacingRate    int64 `protobuf:"varint,33,opt,name=pacing_rate,json=pacingRate,proto3" json:"pacing_rate,omitempty"`
	MaxPacingRate int64 `protobuf:"varint,34,opt,name=max_pacing_rate,json=maxPacingRate,proto3" json:"max_pacing_rate,omitempty"`
	// uint64 doesn't work with bigquery InferSchema
	BytesAcked    int64  `protobuf:"varint,35,opt,name=bytes_acked,json=bytesAcked,proto3" json:"bytes_acked,omitempty"`
	BytesReceived int64  `protobuf:"varint,36,opt,name=bytes_received,json=bytesReceived,proto3" json:"bytes_received,omitempty"`
	SegsOut       uint32 `protobuf:"varint,37,opt,name=segs_out,json=segsOut,proto3" json:"segs_out,omitempty"`
	SegsIn        uint32 `protobuf:"varint,38,opt,name=segs_in,json=segsIn,proto3" json:"segs_in,omitempty"`
	NotsentBytes  uint32 `protobuf:"varint,39,opt,name=notsent_bytes,json=notsentBytes,proto3" json:"notsent_bytes,omitempty"`
	MinRtt        uint32 `protobuf:"varint,40,opt,name=min_rtt,json=minRtt,proto3" json:"min_rtt,omitempty"`
	DataSegsIn    uint32 `protobuf:"varint,41,opt,name=data_segs_in,json=dataSegsIn,proto3" json:"data_segs_in,omitempty"`
	DataSegsOut   uint32 `protobuf:"varint,42,opt,name=data_segs_out,json=dataSegsOut,proto3" json:"data_segs_out,omitempty"`
	// uint64 doesn't work with bigquery SchemaToMap
	DeliveryRate  int64 `protobuf:"varint,43,opt,name=delivery_rate,json=deliveryRate,proto3" json:"delivery_rate,omitempty"`
	BusyTime      int64 `protobuf:"varint,44,opt,name=busy_time,json=busyTime,proto3" json:"busy_time,omitempty"`
	RwndLimited   int64 `protobuf:"varint,45,opt,name=rwnd_limited,json=rwndLimited,proto3" json:"rwnd_limited,omitempty"`
	SndbufLimited int64 `protobuf:"varint,46,opt,name=sndbuf_limited,json=sndbufLimited,proto3" json:"sndbuf_limited,omitempty"`
}
type TCPDiagnosticsProto struct {
	InetDiagMsg *InetDiagMsgProto `protobuf:"bytes,1,opt,name=inet_diag_msg,json=inetDiagMsg,proto3" json:"inet_diag_msg,omitempty"`
	// From INET_DIAG_PROTOCOL message.
	//DiagProtocol tcp.Protocol `protobuf:"varint,2,opt,name=diag_protocol,json=diagProtocol,proto3,enum=Protocol" json:"diag_protocol,omitempty"`
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
	//SocketMem *tcp.SocketMemInfoProto `protobuf:"bytes,7,opt,name=socket_mem,json=socketMem,proto3" json:"socket_mem,omitempty"`
	// Data obtained from INET_DIAG_MEMINFO.
	//MemInfo *tcp.MemInfoProto `protobuf:"bytes,8,opt,name=mem_info,json=memInfo,proto3" json:"mem_info,omitempty"`
	// Data obtained from struct tcp_info.
	TcpInfo *TCPInfoProto `protobuf:"bytes,9,opt,name=tcp_info,json=tcpInfo,proto3" json:"tcp_info,omitempty"`
	// If there is shutdown info, this is the mask value.
	// Check has_shutdown_mask to determine whether present.
	//
	// Types that are valid to be assigned to Shutdown:
	//	*TCPDiagnosticsProto_ShutdownMask
	//Shutdown isTCPDiagnosticsProto_Shutdown `protobuf_oneof:"shutdown"`
	// Timestamp of batch of messages containing this message.
	Timestamp int64 `protobuf:"varint,11,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
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

	schema, err := bigquery.InferSchema(TCPDiagnosticsProto{})
	if err != nil {
		t.Fatal(err)
	}
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
