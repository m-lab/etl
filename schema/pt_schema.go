// This files contains schema for Paris TraceRoute tests.
package schema

import (
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/go/cloud/bqx"
	"github.com/m-lab/uuid-annotator/annotator"
	ptschema "github.com/m-lab/traceroute-caller/schema"

	"github.com/m-lab/etl/metrics"
)

type PTSummary {
	UUID string `json:"uuid,string" bigquery:"uuid"`
}

type PTTest struct {
	A         PTSummary                   `json:"a"`
	Server    annotator.ServerAnnotations `json:"server"`
	Client    annotator.ClientAnnotations `json:"client"`
	ParseInfo ParseInfo                   `json:"parseinfo"`
	TestTime  time.Time                   `json:"testtime"`
	Raw       ptchema.PTTestRaw           `json:"raw"`
}

