package schema

import (
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/go/bqx"

	"github.com/m-lab/ndt-server/ndt5/c2s"
	"github.com/m-lab/ndt-server/ndt5/ndt"
	"github.com/m-lab/ndt-server/ndt5/s2c"
)

// TODO: Remove this in favor of using the ndt-server legacy.NDTResult directly
// once NDTResult.Meta support bigquery.Saver.
type NDTResult struct {
	// GitShortCommit is the Git commit (short form) of the running server code.
	GitShortCommit string

	// These data members should all be self-describing. In the event of confusion,
	// rename them to add clarity rather than adding a comment.
	ControlChannelUUID string
	Protocol           ndt.ConnectionType
	MessageProtocol    string
	ServerIP           string
	ClientIP           string

	StartTime time.Time
	EndTime   time.Time
	C2S       *c2s.ArchivalData `json:",omitempty"`
	S2C       *s2c.ArchivalData `json:",omitempty"`
	// NOTE: we omit NDTResult.Meta (map[]) until it supports the bigquery.Saver interface.
}

// NDTLegacySchema defines the BQ schema for the NDTLegacys produced by the
// ndt-server for the NDT3 and NDT4 clients.
type NDTLegacySchema struct {
	ParseInfo *ParseInfo
	TestID    string    `json:"test_id,string" bigquery:"test_id"`
	LogTime   int64     `json:"log_time,int64" bigquery:"log_time"`
	Result    NDTResult `json:"result" bigquery:"result"`
}

// Schema returns the Bigquery schema for NDTLegacySchema
func (row *NDTLegacySchema) Schema() (bigquery.Schema, error) {
	sch, err := bigquery.InferSchema(row)
	if err != nil {
		return bigquery.Schema{}, err
	}
	rr := bqx.RemoveRequired(sch)
	return rr, nil
}
