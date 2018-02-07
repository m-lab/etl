package schema

// TODO: copy disco schema here.

// Meta contains the archive and parse metadata.
type Meta struct {
	FileName  string `json:"filename, string" bigquery:"filename"`
	TestName  string `json:"testname, string" bigquery:"testname"`
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
