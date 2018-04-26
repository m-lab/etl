package schema

// TODO: copy disco schema here.

// Meta contains the archive and parse metadata.
type Meta struct {
	FileName  string `json:"task_filename, string" bigquery:"task_filename"`
	TestName  string `json:"test_id, string" bigquery:"test_id"`
	ParseTime int64  `json:"parse_time, int64" bigquery:"parse_time"`
}

// Sample is an individual measurement taken by DISCO.
type Sample struct {
	Timestamp int64   `json:"timestamp, int64" bigquery:"timestamp"`
	Value     float32 `json:"value, float32" bigquery:"value"`
}

// SwitchStats represents a row of data taken from the raw DISCO export file.
type SwitchStats struct {
	Meta       Meta     `json:"meta" bigquery:"meta"`
	Sample     []Sample `json:"sample" bigquery:"sample"`
	Metric     string   `json:"metric" bigquery:"metric"`
	Hostname   string   `json:"hostname" bigquery:"hostname"`
	Experiment string   `json:"experiment" bigquery:"experiment"`
}

// Size estimates the number of bytes in the SwitchStats object.
func (s *SwitchStats) Size() int {
	return (len(s.Meta.FileName) + len(s.Meta.TestName) + 8 +
		12*len(s.Sample) + len(s.Metric) + len(s.Hostname) + len(s.Experiment))
}
