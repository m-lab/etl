package schema

import "time"

// Sample is an individual measurement taken by DISCO.
type Sample struct {
	Timestamp int64   `json:"timestamp,int64" bigquery:"timestamp"`
	Value     float32 `json:"value,float32" bigquery:"value"`
	Counter   uint64  `json:"counter,float64" bigquery:"counter"`
}

// SwitchStats represents a row of data taken from the raw DISCO export file.
type SwitchStats struct {
	TaskFilename  string    `json:"task_filename,string" bigquery:"task_filename"`
	TestID        string    `json:"test_id,string" bigquery:"test_id"`
	ParseTime     time.Time `json:"parse_time" bigquery:"parse_time"`
	ParserVersion string    `json:"parser_version,string" bigquery:"parser_version"`
	LogTime       int64     `json:"log_time,int64" bigquery:"log_time"`
	Sample        []Sample  `json:"sample" bigquery:"sample"`
	Metric        string    `json:"metric" bigquery:"metric"`
	Hostname      string    `json:"hostname" bigquery:"hostname"`
	Experiment    string    `json:"experiment" bigquery:"experiment"`
}

// Size estimates the number of bytes in the SwitchStats object.
func (s *SwitchStats) Size() int {
	return (len(s.TaskFilename) + len(s.TestID) + 8 +
		12*len(s.Sample) + len(s.Metric) + len(s.Hostname) + len(s.Experiment))
}
