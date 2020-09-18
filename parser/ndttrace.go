package parser

import (
	"fmt"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	"cloud.google.com/go/civil"
	"github.com/m-lab/annotation-service/api"
	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/uuid-annotator/annotator"
)

//=====================================================================================
//                       NDTTraceToAnnotation Parser
//=====================================================================================

// Legacy web100 ndt archives include metadata, web100, and ndttrace files.  The latter
// contain pcap data.  To date, the desired datatype (and parser) have been determined by
// the directory structure in the gs:// archive.  But we wish to handle the nddtrace files
// separately from the test files, so we have to depart from this convention, OR move
// the ndttrace files, OR invoke multiple parsers per archive.

// Also, there are only about 50 to 100 ndttrace files per archive, so there will be a LOT
// of archives - roughly 20K per day in 2018, and 50K per day in 2019.  So, for the synthetic
// uuid-annotations, we will be generating the same number of small JSONL files.
// Of course, since JSONL files contain one JSON record per newline separated record,
// we can, if we wish, consolidate these by concatenating them, though we would lose
// information about the original archives.

// NDTTraceToAnnotation parses the NDTTrace datatype and produces synthetic uuid-annotator records.
type NDTTraceToAnnotation struct {
	*row.Base
	table  string
	suffix string
}

// NewNDTTraceToAnnotation creates a new parser for annotation data.
func NewNDTTraceToAnnotation(sink row.Sink, label, suffix string, ann v2as.Annotator) etl.Parser {
	bufSize := etl.ANNOTATION.BQBufferSize()
	if ann == nil {
		ann = &nullAnnotator{}
	}

	return &NDTTraceToAnnotation{
		Base:   row.NewBase(label, sink, bufSize, ann),
		table:  label,
		suffix: suffix,
	}
}

// TaskError returns non-nil if the task had enough failures to justify
// recording the entire task as in error.  For now, this is any failure
// rate exceeding 10%.
func (ap *NDTTraceToAnnotation) TaskError() error {
	stats := ap.GetStats()
	if stats.Total() < 10*stats.Failed {
		log.Printf("Warning: high row commit errors (more than 10%%): %d failed of %d accepted\n",
			stats.Failed, stats.Total())
		return etl.ErrHighInsertionFailureRate
	}
	return nil
}

// IsParsable returns the canonical test type and whether to parse data.
func (ap *NDTTraceToAnnotation) IsParsable(testName string, data []byte) (string, bool) {
	// Files look like: "<UUID>.json"
	if strings.HasSuffix(testName, "_ndttrace") || strings.HasSuffix(testName, "_ndttrace.gz") {
		return "ndttrace", true
	}
	return "unknown", false
}

// ParseAndInsert decodes the NDTTrace records, and produces synthetic annotation records.
func (ap *NDTTraceToAnnotation) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
	metrics.WorkerState.WithLabelValues(ap.TableName(), "annotation").Inc()
	defer metrics.WorkerState.WithLabelValues(ap.TableName(), "annotation").Dec()

	synth := annotations{}

	// Construct UUID from filename
	// prefix/20180101T00:00:38.714313000Z_68.170.88.57.c2s_ndttrace.gz
	parts := etl.ParseTestName(testName)
	if len(parts) == 5 {
		// TODO Get the Client IP from the suffix
		client := "foobar"
		t, err := time.Parse("20060102T15:04:05", fmt.Sprintf("%sT%sZ", parts[2], parts[3]))
		if err != nil {

		}
		synth.Timestamp = t
		synth.UUID = fmt.Sprintf("%s-%s-%s-%sT%sZ", meta["site"], meta["host"], client, parts[2], parts[3][0:8])

		synth.clientIP = suffix // Fix this with a regex
	}

	// NOTE: annotations are joined with other tables using the UUID, so
	// finegrain timestamp is not necessary.
	//
	// NOTE: Civil is not TZ adjusted. It takes the year, month, and date from
	// the given timestamp, regardless of the timestamp's timezone. Since we
	// run our systems in UTC, all timestamps will be relative to UTC and as
	// will these dates.
	synth.Date = meta["date"].(civil.Date)
	synth.meta["site"] = meta["site"].(string)
	synth.meta["host"] = meta["host"].(string)

	// Estimate the row size based on the input JSON size.
	metrics.RowSizeHistogram.WithLabelValues(ap.TableName()).Observe(float64(len(test)))

	// Insert the row.
	if err := ap.Base.Put(&synth); err != nil {
		return err
	}

	// Count successful inserts.
	metrics.TestCount.WithLabelValues(ap.TableName(), "annotation", "ok").Inc()
	return nil
}

// NB: These functions are also required to complete the etl.Parser interface.
// For Annotation, we just forward the calls to the Inserter.

func (ap *NDTTraceToAnnotation) Flush() error {
	return ap.Base.Flush()
}

func (ap *NDTTraceToAnnotation) TableName() string {
	return ap.table
}

func (ap *NDTTraceToAnnotation) FullTableName() string {
	return ap.table + ap.suffix
}

// RowsInBuffer returns the count of rows currently in the buffer.
func (ap *NDTTraceToAnnotation) RowsInBuffer() int {
	return ap.GetStats().Pending
}

// Committed returns the count of rows successfully committed to BQ.
func (ap *NDTTraceToAnnotation) Committed() int {
	return ap.GetStats().Committed
}

// Accepted returns the count of all rows received through InsertRow(s)
func (ap *NDTTraceToAnnotation) Accepted() int {
	return ap.GetStats().Total()
}

// Failed returns the count of all rows that could not be committed.
func (ap *NDTTraceToAnnotation) Failed() int {
	return ap.GetStats().Failed
}

//=====================================================================================
// This wraps the Annotations struct, and implements Annotatable
// so we can fill in Client and Server from Annotation Service.
//=====================================================================================

type annotations struct {
	annotator.Annotations
	// https://siteinfo.mlab-oti.measurementlab.net/v1/sites/hostnames.json
	serverIP string // Used by Annotatable
	clientIP string // Used by Annotatable

	meta map[string]string
	Date civil.Date `json:"-"` // For when we add standard columns
}

func (row *annotations) GetClientIPs() []string {
	return []string{row.clientIP}
}

func (row *annotations) GetServerIP() string {
	// Use https://siteinfo.mlab-oti.measurementlab.net/v1/sites/hostnames.json
	return row.serverIP
}

func (row *annotations) AnnotateClients(remote map[string]*api.Annotations) error {
	row.Client.Geo = &annotator.Geolocation{}
	row.Client.Network = &annotator.Network{}
	ann, ok := remote[row.clientIP]
	if !ok {
		row.Client.Geo.Missing = true
		row.Client.Network.Missing = true
		return nil
	}
	row.Client.Geo.AccuracyRadiusKm = ann.Geo.AccuracyRadiusKm
	row.Client.Geo.AreaCode = ann.Geo.AreaCode
	row.Client.Geo.City = ann.Geo.City
	row.Client.Geo.ContinentCode = ann.Geo.ContinentCode
	row.Client.Geo.CountryCode = ann.Geo.CountryCode
	row.Client.Geo.CountryCode3 = ann.Geo.CountryCode3
	row.Client.Geo.CountryName = ann.Geo.CountryName
	row.Client.Geo.Latitude = ann.Geo.Latitude
	row.Client.Geo.Longitude = ann.Geo.Longitude
	row.Client.Geo.MetroCode = ann.Geo.MetroCode
	row.Client.Geo.PostalCode = ann.Geo.PostalCode
	row.Client.Geo.Region = ann.Geo.Region
	// row.Client.Geo.Subdivision1ISOCode =
	// row.Client.Geo.Subdivision1Name
	// row.Client.Geo.Subdivision2ISOCode
	// row.Client.Geo.Subdivision2Name
	return nil
}

func (row *annotations) AnnotateServer(local *api.GeoData) error {
	// Can we ignore the service info and look up by site/machine?
	row.Server.Machine = row.meta["host"]
	row.Server.Site = row.meta["site"]
	//  https://siteinfo.mlab-oti.measurementlab.net/v1/sites/annotations.json
	row.Server.Geo = &annotator.Geolocation{}
	// https://siteinfo.mlab-oti.measurementlab.net/v1/sites/hostnames.json
	// https://siteinfo.mlab-oti.measurementlab.net/v1/sites/annotations.json
	row.Server.Network = &annotator.Network{}
	row.Server.Geo.Missing = true
	row.Server.Network.Missing = true
	return nil
}

func (row *annotations) GetLogTime() time.Time {
	return row.Timestamp
}

func assertTestRowAnnotatable(r *annotations) {
	func(row.Annotatable) {}(r)
}
