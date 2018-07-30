// Package pbparser comprises parsers that use protobufs in one way or another.
// This is separate from other parsers, because protobuffers incur additional build
// dependencies and overhead.
package pbparser

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/m-lab/etl/annotation"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
	tcp "github.com/m-lab/tcp-info/nl-proto"
)

// RowBuffer for tcpinfo.
// TODO - merge with similar code in ss.go
type RowBuffer struct {
	bufferSize int
	rows       []interface{}
}

// AddRow simply inserts a row into the buffer.  Returns error if buffer is full.
// Not thread-safe.  Should only be called by owning thread.
func (buf *RowBuffer) AddRow(row interface{}) error {
	for len(buf.rows) >= buf.bufferSize-1 {
		return etl.ErrBufferFull
	}
	buf.rows = append(buf.rows, row)
	return nil
}

// TakeRows returns all rows in the buffer, and clears the buffer.
// Not thread-safe.  Should only be called by owning thread.
func (buf *RowBuffer) TakeRows() []interface{} {
	res := buf.rows
	buf.rows = make([]interface{}, 0, buf.bufferSize)
	return res
}

// Annotate fetches annotations for all rows in the buffer.
// Not thread-safe.  Should only be called by owning thread.
func (buf *RowBuffer) Annotate(tableBase string) {
	metrics.WorkerState.WithLabelValues(tableBase, "annotate").Inc()
	defer metrics.WorkerState.WithLabelValues(tableBase, "annotate").Dec()
	if len(buf.rows) == 0 {
		return
	}

	ipSlice := make([]string, 2*len(buf.rows))
	geoSlice := make([]*annotation.GeolocationIP, 2*len(buf.rows))
	var logTime time.Time
	switch buf.rows[0].(type) {
	case *schema.SS:
		// Just use the logtime of the first row.
		logTime = time.Unix(buf.rows[0].(*schema.SS).Web100_log_entry.Log_time, 0)
		for i := range buf.rows {
			row := buf.rows[i].(*schema.SS)
			connSpec := &row.Web100_log_entry.Connection_spec
			ipSlice[i+i] = connSpec.Local_ip
			geoSlice[i+i] = &connSpec.Local_geolocation
			ipSlice[i+i+1] = connSpec.Remote_ip
			geoSlice[i+i+1] = &connSpec.Remote_geolocation
		}
	case *tcp.TCPDiagnosticsProto:
	default:
	}

	start := time.Now()
	// TODO - are there any errors we should process from Fetch?
	annotation.FetchGeoAnnotations(ipSlice, logTime, geoSlice)
	metrics.AnnotationTimeSummary.With(prometheus.Labels{"test_type": "SS"}).Observe(float64(time.Since(start).Nanoseconds()))
}

// ReadAll reads and marshals all protobufs from a Reader.
func ReadAll(rdr io.Reader) ([]tcp.TCPDiagnosticsProto, error) {
	var result []tcp.TCPDiagnosticsProto

	byteRdr := bufio.NewReader(rdr)
	bufSize := 100
	rowBuf := RowBuffer{bufSize, make([]interface{}, 0, bufSize)}

	for {
		size, err := binary.ReadUvarint(byteRdr)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		buf := make([]byte, size)
		n, err := io.ReadFull(byteRdr, buf)
		if err != nil {
			return nil, err
		}
		if n != len(buf) {
			return nil, errors.New("corrupted protobuf file")
		}

		pb := tcp.TCPDiagnosticsProto{}
		proto.Unmarshal(buf, &pb)
		result = append(result, pb)
		rowBuf.AddRow(&pb)
	}

	/*
		inserter, err := bq.NewInserter(etl.TCPINFO, time.Now())
		log.Println(inserter)
		if err != nil {
			log.Fatal("foobar")
		}
		inserter.PutAsync(rowBuf.TakeRows()) */

	return result, nil
}

type TCPDiagnosticsProto struct {
	TcpInfo *tcp.TCPInfoProto `protobuf:"bytes,9,opt,name=tcp_info,json=tcpInfo,proto3" json:"tcp_info,omitempty"`
	// If there is shutdown info, this is the mask value.
	// Check has_shutdown_mask to determine whether present.
	//
	// Types that are valid to be assigned to Shutdown:
	//	*TCPDiagnosticsProto_ShutdownMask
	// Shutdown isTCPDiagnosticsProto_Shutdown `protobuf_oneof:"shutdown"`
	ShutdownMask uint32
	// Timestamp of batch of messages containing this message.
	Timestamp time.Time `protobuf:"varint,11,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
}

// InfoWrapper that implements ValueSaver
type InfoWrapper struct {
	tcp.TCPDiagnosticsProto
}

func add(name string, row map[string]bigquery.Value, pstruct interface{}) error {
	schema, err := bigquery.InferSchema(pstruct)
	if err != nil {
		return err
	}
	schema = removeXXX(schema)
	ss := bigquery.StructSaver{Schema: schema, InsertID: "", Struct: pstruct}
	row[name], _, err = ss.Save()
	if err != nil {
		return err
	}
	return nil
}

// Save implements the ValueSaver.Save() method.
func (iw InfoWrapper) Save() (row map[string]bigquery.Value, insertID string, err error) {
	// Assemble the full map by examining each top level field.
	row = make(map[string]bigquery.Value, 10)

	err = add("InetDiagMsg", row, iw.InetDiagMsg)
	if err != nil {
		log.Println(err)
		return
	}

	if iw.DiagProtocol != tcp.Protocol_IPPROTO_UNUSED {
		row["DiagProtocol"] = iw.DiagProtocol
	}

	if iw.CongestionAlgorithm != "" {
		row["CongestionAlgorithm"] = iw.CongestionAlgorithm
	}

	switch iw.CcInfo.(type) {
	case *tcp.TCPDiagnosticsProto_BbrInfo:
		err = add("BBR", row, iw.CcInfo)
	case *tcp.TCPDiagnosticsProto_Dctcp:
		err = add("DCTCP", row, iw.CcInfo)
	case *tcp.TCPDiagnosticsProto_Vegas:
		err = add("VEGAS", row, iw.CcInfo)
	default:
	}
	if err != nil {
		log.Println(err)
		return
	}

	//	*TCPDiagnosticsProto_ShutdownMask
	err = add("SocketMem", row, iw.SocketMem)
	if err != nil {
		log.Println(err)
		return
	}

	err = add("MemInfo", row, iw.MemInfo)
	if err != nil {
		log.Println(err)
		return
	}

	err = add("MemInfo", row, iw.MemInfo)
	if err != nil {
		log.Println(err)
		return
	}

	err = add("TCPInfo", row, iw.TcpInfo)
	if err != nil {
		log.Println(err)
		return
	}

	row["timestamp"] = time.Now()
	return
}

func removeXXX(schema bigquery.Schema) bigquery.Schema {
	result := make([]*bigquery.FieldSchema, 0, len(schema))
	for i := range schema {
		if !strings.HasPrefix(schema[i].Name, "XXX_") {
			log.Println(i, schema[i].Name, schema[i].Type)
			if schema[i].Type == bigquery.RecordFieldType {
				schema[i].Schema = removeXXX(schema[i].Schema)
			}
			result = append(result, schema[i])
		}
	}
	return result
}

func GetSchema() (bigquery.Schema, error) {
	schema, err := bigquery.InferSchema(TCPDiagnosticsProto{})
	if err != nil {
		return bigquery.Schema{}, err
	}
	return removeXXX(schema), nil
}
