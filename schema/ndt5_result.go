package schema

import (
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/go/bqx"
	"github.com/m-lab/ndt-server/data"
)

// NDT5ResultRow defines the BQ schema for the data.NDT5Result produced by the
// ndt-server for NDT client measurements.
type NDT5ResultRow struct {
	ParseInfo *ParseInfo
	TestID    string          `json:"test_id,string" bigquery:"test_id"`
	LogTime   int64           `json:"log_time,int64" bigquery:"log_time"`
	Result    data.NDT5Result `json:"result" bigquery:"result"`
}

// Schema returns the BigQuery schema for NDT5ResultRow.
func (row *NDT5ResultRow) Schema() (bigquery.Schema, error) {
	sch, err := bigquery.InferSchema(row)
	if err != nil {
		return bigquery.Schema{}, err
	}
	docs := FindSchemaDocsFor(row)
	for _, doc := range docs {
		bqx.UpdateSchemaDescription(sch, doc)
	}
	rr := bqx.RemoveRequired(sch)
	return rr, err
}

// Implement row.Annotatable
// This is a trivial implementation, as the schema does not yet include
// annotations, and probably will not until we integrate UUID Annotator.
func (row *NDT5ResultRow) GetLogTime() time.Time {
	return time.Now()
}
func (row *NDT5ResultRow) GetClientIPs() []string {
	return []string{}
}
func (row *NDT5ResultRow) GetServerIP() string {
	return ""
}
func (row *NDT5ResultRow) AnnotateClients(map[string]*api.Annotations) error {
	return nil
}
func (row *NDT5ResultRow) AnnotateServer(*api.Annotations) error {
	return nil
}
