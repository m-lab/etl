package parser

import (
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/traceroute-caller/parser"
)

//=====================================================================================
//                       scamper1 Parser
//=====================================================================================

const (
	scamper1 = "scamper1"
)

var legacyScamperEnd = civil.Date{
	Year:  2021,
	Month: time.September,
	Day:   9,
}

// Scamper1Parser handles parsing for the scamper1 datatype.
type Scamper1Parser struct {
	*row.Base
	table  string
	suffix string
}

// NewScamper1Parser returns a new parser for the scamper1 archives.
func NewScamper1Parser(sink row.Sink, table, suffix string, ann v2as.Annotator) etl.Parser {
	bufSize := etl.SCAMPER1.BQBufferSize()
	if ann == nil {
		ann = v2as.GetAnnotator(etl.BatchAnnotatorURL)
	}

	return &Scamper1Parser{
		Base:   row.NewBase(table, sink, bufSize, ann),
		table:  table,
		suffix: suffix,
	}
}

// parseTracelb parses the TracelbLine struct defined in traceroute-caller and populates the BQTracelbLine.
func parseTracelb(bqScamperOutput *schema.BQScamperOutput, tracelb parser.TracelbLine) {
	bqScamperOutput.Tracelb = schema.BQTracelbLine{
		Type:        tracelb.Type,
		Version:     tracelb.Version,
		Userid:      tracelb.Userid,
		Method:      tracelb.Method,
		Src:         tracelb.Src,
		Dst:         tracelb.Dst,
		Start:       tracelb.Start,
		ProbeSize:   tracelb.ProbeSize,
		Firsthop:    tracelb.Firsthop,
		Attempts:    tracelb.Attempts,
		Confidence:  tracelb.Confidence,
		Tos:         tracelb.Tos,
		Gaplimit:    tracelb.Gaplint,
		WaitTimeout: tracelb.WaitTimeout,
		WaitProbe:   tracelb.WaitProbe,
		Probec:      tracelb.Probec,
		ProbecMax:   tracelb.ProbecMax,
		Nodec:       tracelb.Nodec,
		Linkc:       tracelb.Linkc,
	}

	nodes := tracelb.Nodes
	bqScamperOutput.Tracelb.Nodes = make([]schema.BQScamperNode, 0, len(nodes))

	for _, node := range nodes {
		bqLinkArray := make([]schema.BQScamperLinkArray, 0, len(node.Links))
		for _, link := range node.Links {
			bqLinks := schema.BQScamperLinkArray{}
			bqLinks.Links = make([]parser.ScamperLink, len(link))
			copy(bqLinks.Links, link)
			bqLinkArray = append(bqLinkArray, bqLinks)
		}

		bqScamperNode := schema.BQScamperNode{
			HopID: GetHopID(bqScamperOutput.CycleStart.StartTime, bqScamperOutput.CycleStart.Hostname,
				node.Addr),
			Addr:  node.Addr,
			Name:  node.Name,
			QTTL:  node.QTTL,
			Linkc: node.Linkc,
			Links: bqLinkArray,
		}
		bqScamperOutput.Tracelb.Nodes = append(bqScamperOutput.Tracelb.Nodes, bqScamperNode)
	}
}

// IsParsable returns the canonical test type and whether to parse data.
func (p *Scamper1Parser) IsParsable(testName string, data []byte) (string, bool) {
	if strings.HasSuffix(testName, "jsonl") {
		return scamper1, true
	}
	return "", false
}

// ParseAndInsert decodes the scamper1 data and inserts it into BQ.
func (p *Scamper1Parser) ParseAndInsert(fileMetadata map[string]bigquery.Value, testName string, rawContent []byte) error {
	metrics.WorkerState.WithLabelValues(p.TableName(), scamper1).Inc()
	defer metrics.WorkerState.WithLabelValues(p.TableName(), scamper1).Dec()

	scamperOutput, err := parser.ParseTraceroute(rawContent)
	if err != nil {
		date := fileMetadata["date"].(civil.Date)
		if legacyScamperEnd.Before(date) {
			return fmt.Errorf("failed to parse scamper1 file: %s, error: %v", testName, err)
		}
		return nil
	}

	bqScamperOutput := schema.BQScamperOutput{
		Metadata:   scamperOutput.Metadata,
		CycleStart: scamperOutput.CycleStart,
		CycleStop:  scamperOutput.CycleStop,
	}
	parseTracelb(&bqScamperOutput, scamperOutput.Tracelb)

	parseInfo := schema.ParseInfo{
		Version:    Version(),
		Time:       time.Now(),
		ArchiveURL: fileMetadata["filename"].(string),
		Filename:   testName,
		GitCommit:  GitCommit(),
	}

	row := schema.Scamper1Row{
		ID:     bqScamperOutput.Metadata.UUID,
		Parser: parseInfo,
		Date:   fileMetadata["date"].(civil.Date),
		Raw:    bqScamperOutput,
	}

	// Insert the row.
	if err := p.Put(&row); err != nil {
		return err
	}

	// Count successful inserts.
	metrics.TestCount.WithLabelValues(p.TableName(), scamper1, "ok").Inc()

	return nil
}

// NB: These functions are also required to complete the etl.Parser interface
// For scamper1, we just forward the calls to the Inserter.

// Flush flushes any pending rows.
func (p *Scamper1Parser) Flush() error {
	return p.Base.Flush()
}

// TableName of the table that this Parser inserts into.
// Used for metrics and logging.
func (p *Scamper1Parser) TableName() string {
	return p.table
}

// FullTableName of the BQ table that the uploader pushes to,
// including $YYYYMMNN, or _YYYYMMNN.
func (p *Scamper1Parser) FullTableName() string {
	return p.table + p.suffix
}

// RowsInBuffer returns the count of rows currently in the buffer.
func (p *Scamper1Parser) RowsInBuffer() int {
	return p.GetStats().Pending
}

// Committed returns the count of rows successfully committed to BQ.
func (p *Scamper1Parser) Committed() int {
	return p.GetStats().Committed
}

// Accepted returns the count of all rows received through InsertRow(s).
func (p *Scamper1Parser) Accepted() int {
	return p.GetStats().Total()
}

// Failed returns the count of all rows that could not be committed.
func (p *Scamper1Parser) Failed() int {
	return p.GetStats().Failed
}
