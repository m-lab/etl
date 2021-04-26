package schema

import (
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/go/cloud/bqx"
)

// NDTWeb100 is a mirror struct of the BQ schema. This type is NOT USED by the parser.
//
// WARNING // WARNING // WARNING
//
// TODO: migrate parser/ndt.go to use native struct, then migrate to standard columns.
type NDTWeb100 struct {
	TestID         string                  `bigquery:"test_id"`
	TaskFilename   string                  `bigquery:"task_filename"`
	ParseTime      time.Time               `bigquery:"parse_time"`
	ParserVersion  string                  `bigquery:"parser_version"`
	LogTime        time.Time               `bigquery:"log_time"`
	BlacklistFlags int64                   `bigquery:"blacklist_flags"`
	Anomalies      ndtweb100Anomalies      `bigquery:"anomalies"`
	ConnectionSpec ndtweb100ConnectionSpec `bigquery:"connection_spec"`
	Web100LogEntry web100LogEntry          `bigquery:"web100_log_entry"`
}

type ndtweb100Anomalies struct {
	NoMeta         bool  `bigquery:"no_meta"`
	SnaplogError   bool  `bigquery:"snaplog_error"`
	NumSnaps       int64 `bigquery:"num_snaps"`
	BlacklistFlags int64 `bigquery:"blacklist_flags"`
}

type ndtweb100ConnectionSpec struct {
	ClientAF            int64                  `bigquery:"client_af"`
	ClientApplication   string                 `bigquery:"client_application"`
	ClientBrowser       string                 `bigquery:"client_browser"`
	ClientHostname      string                 `bigquery:"client_hostname"`
	ClientIP            string                 `bigquery:"client_ip"`
	ClientKernelVersion string                 `bigquery:"client_kernel_version"`
	ClientOS            string                 `bigquery:"client_os"`
	ClientVersion       string                 `bigquery:"client_version"`
	DataDirection       int64                  `bigquery:"data_direction"`
	ServerAF            int64                  `bigquery:"server_af"`
	ServerHostname      string                 `bigquery:"server_hostname"`
	ServerIP            string                 `bigquery:"server_ip"`
	ServerKernelVersion string                 `bigquery:"server_kernel_version"`
	TLS                 bool                   `bigquery:"tls"`
	Websockets          bool                   `bigquery:"websockets"`
	ClientGeolocation   api.GeolocationIP      `bigquery:"client_geolocation"`
	ServerGeolocation   api.GeolocationIP      `bigquery:"server_geolocation"`
	Client              ndtweb100ClientNetwork `bigquery:"client"`
	Server              ndtweb100ServerNetwork `bigquery:"server"`
}

type network struct {
	ASN string `bigquery:"asn"`
}

type ndtweb100ClientNetwork struct {
	Network network `bigquery:"network"`
	// api.ASData         // Include extended asn data from  annotation-service
}
type ndtweb100ServerNetwork struct {
	IataCode string  `bigquery:"iata_code"`
	Network  network `bigquery:"network"`
	// api.ASData         // Include extended asn data from  annotation-service
}

type web100ConnectionSpec struct {
	LocalAF    int64  `bigquery:"local_af"`
	LocalIP    string `bigquery:"local_ip"`
	LocalPort  int64  `bigquery:"local_port"`
	RemoteIP   string `bigquery:"remote_ip"`
	RemotePort int64  `bigquery:"remote_port"`
}

type web100LogEntry struct {
	LogTime        int64                `bigquery:"log_time"`
	Version        string               `bigquery:"version"`
	ConnectionSpec web100ConnectionSpec `bigquery:"connection_spec"`
	Snap           web100Snap           `bigquery:"snap"`
	Deltas         []web100Deltas       `bigquery:"deltas"`
}

type web100Snap struct {
	LocalAddress     string
	LocalAddressType int64
	LocalPort        int64
	RemAddress       string
	RemPort          int64
	web100SnapDelta
}

type web100SnapDelta struct {
	AbruptTimeouts       int64
	ActiveOpen           int64
	CERcvd               int64
	CongAvoid            int64
	CongOverCount        int64
	CongSignals          int64
	CountRTT             int64
	CurAppRQueue         int64
	CurAppWQueue         int64
	CurCwnd              int64
	CurMSS               int64
	CurRTO               int64
	CurReasmQueue        int64
	CurRetxQueue         int64
	CurRwinRcvd          int64
	CurRwinSent          int64
	CurSsthresh          int64
	CurTimeoutCount      int64
	DSACKDups            int64
	DataSegsIn           int64
	DataSegsOut          int64
	DupAcksIn            int64
	DupAcksOut           int64
	Duration             int64
	ECN                  int64
	FastRetran           int64
	HCDataOctetsIn       int64
	HCDataOctetsOut      int64
	HCThruOctetsAcked    int64
	HCThruOctetsReceived int64
	LimCwnd              int64
	LimRwin              int64
	MSSRcvd              int64
	MaxAppRQueue         int64
	MaxAppWQueue         int64
	MaxMSS               int64
	MaxRTO               int64
	MaxRTT               int64
	MaxReasmQueue        int64
	MaxRetxQueue         int64
	MaxRwinRcvd          int64
	MaxRwinSent          int64
	MaxSsCwnd            int64
	MaxSsthresh          int64
	MinMSS               int64
	MinRTO               int64
	MinRTT               int64
	MinRwinRcvd          int64
	MinRwinSent          int64
	MinSsthresh          int64
	Nagle                int64
	NonRecovDA           int64
	OctetsRetrans        int64
	OtherReductions      int64
	PostCongCountRTT     int64
	PostCongSumRTT       int64
	PreCongSumCwnd       int64
	PreCongSumRTT        int64
	QuenchRcvd           int64
	RTTVar               int64
	RcvNxt               int64
	RcvRTT               int64
	RcvWindScale         int64
	RecInitial           int64
	RetranThresh         int64
	SACK                 int64
	SACKBlocksRcvd       int64
	SACKsRcvd            int64
	SampleRTT            int64
	SegsIn               int64
	SegsOut              int64
	SegsRetrans          int64
	SendStall            int64
	SlowStart            int64
	SmoothedRTT          int64
	SndInitial           int64
	SndLimBytesCwnd      int64
	SndLimBytesRwin      int64
	SndLimBytesSender    int64
	SndLimTimeCwnd       int64
	SndLimTimeRwin       int64
	SndLimTimeSnd        int64
	SndLimTransCwnd      int64
	SndLimTransRwin      int64
	SndLimTransSnd       int64
	SndMax               int64
	SndNxt               int64
	SndUna               int64
	SndWindScale         int64
	SpuriousFrDetected   int64
	StartTimeStamp       int64
	StartTimeUsec        int64
	State                int64
	SubsequentTimeouts   int64
	SumRTT               int64
	TimeStamps           int64
	Timeouts             int64
	WinScaleRcvd         int64
	WinScaleSent         int64
	X_OtherReductionsCM  int64
	X_OtherReductionsCV  int64
	X_Rcvbuf             int64
	X_Sndbuf             int64
	X_dbg1               int64
	X_dbg2               int64
	X_dbg3               int64
	X_dbg4               int64
	X_rcv_ssthresh       int64
	X_wnd_clamp          int64
}

type web100Deltas struct {
	IsLast          bool  `bigquery:"is_last"`
	SnapshotNum     int64 `bigquery:"snapshot_num"`
	DeltaIndex      int64 `bigquery:"delta_index"`
	web100SnapDelta       // embed inline struct.
}

func (n *NDTWeb100) Schema() (bigquery.Schema, error) {
	sch, err := bigquery.InferSchema(n)
	if err != nil {
		return bigquery.Schema{}, err
	}
	rr := bqx.RemoveRequired(sch)
	return rr, nil
}
