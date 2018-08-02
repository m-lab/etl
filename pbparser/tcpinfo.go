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
// Maybe about 120 usec per record, not counting storage latency (for 17 rows, local workstation)
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

	return result, nil
}

// InfoWrapper that implements ValueSaver
type InfoWrapper struct {
	tcp.TCPDiagnosticsProto
	TaskFilename string
}

// GetStructMap infers schema, removes XXX_ fields, and returns complete map.
func GetStructMap(pstruct interface{}) (bigquery.Value, error) {
	start := time.Now()
	schema, err := bigquery.InferSchema(pstruct)
	if err != nil {
		return bigquery.NullBool{}, err
	}
	schema = removeXXX(schema)
	log.Println(time.Now().Sub(start))
	ss := bigquery.StructSaver{Schema: schema, InsertID: "", Struct: pstruct}
	result, _, err := ss.Save()
	return result, err
}

func addSchema(name string, outer *bigquery.Schema, pstruct interface{}) error {
	schema, err := bigquery.InferSchema(pstruct)
	if err != nil {
		return err
	}
	schema = removeXXX(schema)
	*outer = append(*outer, &bigquery.FieldSchema{Name: name, Schema: schema, Type: bigquery.RecordFieldType})
	return nil
}

// BuildSchema creates the full TCPInfo bigquery schema
// Used only for creating table in TestMakeTable()
// TODO - create an appropriate struct, and just use InferSchema and removeXXX
func BuildSchema() (bigquery.Schema, error) {
	schema := bigquery.Schema{}
	schema = append(schema, &bigquery.FieldSchema{Name: "test_id", Type: bigquery.StringFieldType})
	schema = append(schema, &bigquery.FieldSchema{Name: "task_filename", Type: bigquery.StringFieldType})
	schema = append(schema, &bigquery.FieldSchema{Name: "parse_time", Type: bigquery.TimestampFieldType})
	schema = append(schema, &bigquery.FieldSchema{Name: "log_time", Type: bigquery.TimestampFieldType})

	err := addSchema("InetDiagMsg", &schema, tcp.InetDiagMsgProto{})
	if err != nil {
		log.Println(err)
		return schema, err
	}

	schema = append(schema, &bigquery.FieldSchema{Name: "DiagProtocol", Type: bigquery.IntegerFieldType})
	schema = append(schema, &bigquery.FieldSchema{Name: "CongestionAlgorithm", Type: bigquery.StringFieldType})

	err = addSchema("Bbr", &schema, tcp.TCPDiagnosticsProto_BbrInfo{}.BbrInfo)
	if err != nil {
		log.Println(err)
		return schema, err
	}
	err = addSchema("Dctcp", &schema, tcp.TCPDiagnosticsProto_Dctcp{}.Dctcp)
	if err != nil {
		log.Println(err)
		return schema, err
	}
	err = addSchema("Vegas", &schema, tcp.TCPDiagnosticsProto_Vegas{}.Vegas)
	if err != nil {
		log.Println(err)
		return schema, err
	}

	err = addSchema("SocketMem", &schema, tcp.SocketMemInfoProto{})
	if err != nil {
		log.Println(err)
		return schema, err
	}

	err = addSchema("MemInfo", &schema, tcp.MemInfoProto{})
	if err != nil {
		log.Println(err)
		return schema, err
	}

	err = addSchema("TCPInfo", &schema, tcp.TCPInfoProto{})
	if err != nil {
		log.Println(err)
		return schema, err
	}

	schema = append(schema, &bigquery.FieldSchema{Name: "Shutdown", Type: bigquery.IntegerFieldType})

	return schema, nil
}

// Save implements the ValueSaver.Save() method.
// Benchmark - about 1 msec.
func (iw InfoWrapper) Save() (row map[string]bigquery.Value, insertID string, err error) {
	// Assemble the full map by examining each top level field.
	start := time.Now()
	row = make(map[string]bigquery.Value, 10)

	row["task_filename"] = iw.TaskFilename
	row["log_time"] = time.Unix(0, iw.Timestamp)
	row["parse_time"] = time.Now().Unix()

	row["InetDiagMsg"], err = GetStructMap(iw.InetDiagMsg)
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
		// TODO - verify that these actually work.
		row["Bbr"], err = GetStructMap(iw.GetBbrInfo())
	case *tcp.TCPDiagnosticsProto_Dctcp:
		row["Dctcp"], err = GetStructMap(iw.GetDctcp())
	case *tcp.TCPDiagnosticsProto_Vegas:
		row["Vegas"], err = GetStructMap(iw.GetVegas())
	default:
		row["Bbr"], err = GetStructMap(tcp.BBRInfoProto{})
		row["Dctcp"], err = GetStructMap(tcp.DCTCPInfoProto{})
		row["Vegas"], err = GetStructMap(tcp.TCPVegasInfoProto{})
	}
	if err != nil {
		log.Println(err)
		return
	}

	//	*TCPDiagnosticsProto_ShutdownMask
	row["SocketMem"], err = GetStructMap(iw.SocketMem)
	if err != nil {
		log.Println(err)
		return
	}

	row["MemInfo"], err = GetStructMap(iw.MemInfo)
	if err != nil {
		log.Println(err)
		return
	}

	row["TCPInfo"], err = GetStructMap(iw.TcpInfo)
	if err != nil {
		log.Println(err)
		return
	}

	shutdown := iw.GetShutdownMask()
	if shutdown != 0 {
		row["Shutdown"] = shutdown
	}

	log.Println(time.Now().Sub(start))
	return
}

func removeXXX(schema bigquery.Schema) bigquery.Schema {
	result := make([]*bigquery.FieldSchema, 0, len(schema))
	for i := range schema {
		if !strings.HasPrefix(schema[i].Name, "XXX_") {
			if schema[i].Type == bigquery.RecordFieldType {
				schema[i].Schema = removeXXX(schema[i].Schema)
			}
			result = append(result, schema[i])
		}
	}
	return result
}
